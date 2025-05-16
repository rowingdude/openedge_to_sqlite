#include <set>
#include "TableSyncer.h"
#include "HashCalculator.h"
#include <algorithm>
#include <stdexcept>

TableSyncer::TableSyncer(SqliteHelper& sqliteHelper, 
                         OdbcHelper& odbcHelper,
                         std::shared_ptr<SyncState> syncState,
                         std::shared_ptr<HashStorage> hashDb,
                         std::shared_ptr<Logger> logger,
                         int batchSize)
    : sqliteHelper(sqliteHelper), 
      odbcHelper(odbcHelper),
      syncState(syncState),
      hashDb(hashDb),
      logger(logger),
      batchSize(batchSize),
      hashEnabled(hashDb != nullptr) {
}

int TableSyncer::SyncTable(const TableInfo& tableInfo, bool fullSync) {
    const std::string& tableName = tableInfo.tableName;
    
    if (!EnsureTargetTable(tableInfo)) {
        logger->Error("Failed to ensure target table " + tableName);
        return 0;
    }
    
    std::string strategy = GetSyncStrategy(tableInfo, fullSync);
    logger->Info("Using " + strategy + " sync strategy for " + tableName);
    
    int rowsSynced = 0;
    if (strategy == "full") {
        rowsSynced = SyncFullTable(tableInfo);
    } else if (strategy == "key_based") {
        rowsSynced = SyncKeyBased(tableInfo);
    } else if (strategy == "hash_based") {
        rowsSynced = SyncHashBased(tableInfo);
    } else if (strategy == "timestamp") {
        rowsSynced = SyncTimestampBased(tableInfo);
    }
    
    return rowsSynced;
}

std::string TableSyncer::GetSyncStrategy(const TableInfo& tableInfo, bool fullSync) {
    const std::string& tableName = tableInfo.tableName;
    const std::string& pkColumn = tableInfo.pkColumn;
    
    if (fullSync) {
        return "full";
    }
    
    auto lastSync = syncState->GetLastSync(tableName);
    
    if (lastSync.lastSyncTime.empty()) {
        return "full";
    }
    
    if (hashEnabled && !pkColumn.empty()) {
        return "hash_based";
    }
    
    if (!pkColumn.empty()) {
        return "key_based";
    }
    
    return "timestamp";
}

int TableSyncer::SyncFullTable(const TableInfo& tableInfo) {
    const std::string& tableName = tableInfo.tableName;
    const std::vector<std::string>& columns = tableInfo.columns;
    const std::string& pkColumn = tableInfo.pkColumn;
    
    int totalRows = GetSourceRowCount(tableName);
    
    try {
        // Delete all existing data
        if (!sqliteHelper.ExecuteNonQuery("DELETE FROM " + tableName)) {
            return 0;
        }
        
        // Prepare the query for source data
        std::string selectSql = "SELECT ";
        for (size_t i = 0; i < columns.size(); ++i) {
            selectSql += "\"" + columns[i] + "\"";
            if (i < columns.size() - 1) {
                selectSql += ", ";
            }
        }
        selectSql += " FROM PUB." + tableName;
        
        SQLHSTMT stmt = odbcHelper.ExecuteQuery(selectSql);
        if (stmt == SQL_NULL_HSTMT) {
            return 0;
        }
        
        // Begin transaction
        sqliteHelper.BeginTransaction();
        
        int rowsSynced = 0;
        std::string lastValue;
        int pkIndex = -1;
        
        if (!pkColumn.empty()) {
            for (size_t i = 0; i < columns.size(); ++i) {
                if (columns[i] == pkColumn) {
                    pkIndex = static_cast<int>(i);
                    break;
                }
            }
        }
        
        // Prepare insert statement
        std::string insertSql = "INSERT INTO " + tableName + " (";
        for (size_t i = 0; i < columns.size(); ++i) {
            insertSql += "\"" + columns[i] + "\"";
            if (i < columns.size() - 1) {
                insertSql += ", ";
            }
        }
        insertSql += ") VALUES (";
        for (size_t i = 0; i < columns.size(); ++i) {
            insertSql += "?";
            if (i < columns.size() - 1) {
                insertSql += ", ";
            }
        }
        insertSql += ")";
        
        sqlite3_stmt* insertStmt = sqliteHelper.PrepareStatement(insertSql);
        if (!insertStmt) {
            odbcHelper.FreeStatement(stmt);
            sqliteHelper.RollbackTransaction();
            return 0;
        }
        
        // Process rows
        while (odbcHelper.FetchRow(stmt)) {
            std::vector<std::string> rowData;
            
            for (size_t i = 0; i < columns.size(); ++i) {
                std::string value = odbcHelper.GetColumnData(stmt, i + 1);
                rowData.push_back(value);
                
                if (static_cast<int>(i) == pkIndex) {
                    lastValue = value;
                }
            }
            
            // Reset statement and bind parameters
            sqlite3_reset(insertStmt);
            
            for (size_t i = 0; i < rowData.size(); ++i) {
                sqliteHelper.BindParameter(insertStmt, i + 1, rowData[i]);
            }
            
            // Execute insert
            int rc = sqlite3_step(insertStmt);
            if (rc != SQLITE_DONE) {
                logger->Error("Error inserting row: " + std::string(sqlite3_errmsg(sqlite3_db_handle(insertStmt))));
            } else {
                rowsSynced++;
                
                // If hash-based sync is enabled, store the hash
                if (hashEnabled && pkIndex >= 0 && !rowData[pkIndex].empty()) {
                    std::string rowHash = HashCalculator::CalculateRowHash(rowData);
                    hashDb->StoreHash(tableName, rowData[pkIndex], rowHash);
                }
            }
            
            if (rowsSynced % batchSize == 0) {
                sqliteHelper.CommitTransaction();
                sqliteHelper.BeginTransaction();
                
                float progressPct = (totalRows > 0) ? static_cast<float>(rowsSynced) / totalRows * 100 : 0;
                logger->Info("Inserted " + std::to_string(batchSize) + " rows for " + tableName + 
                            " (total: " + std::to_string(rowsSynced) + " of " + std::to_string(totalRows) + 
                            " (" + std::to_string(progressPct) + "%)");
            }
        }
        
        // Finalize statements and commit transaction
        sqlite3_finalize(insertStmt);
        sqliteHelper.CommitTransaction();
        odbcHelper.FreeStatement(stmt);
        
        // Update sync state
        if (!pkColumn.empty() && !lastValue.empty()) {
            syncState->UpdateSyncState(tableName, lastValue, "key_based", rowsSynced);
        } else {
            syncState->UpdateSyncState(tableName, "", "timestamp", rowsSynced);
        }
        
        logger->Info("Completed full sync of " + tableName + ": " + std::to_string(rowsSynced) + " rows");
        return rowsSynced;
    } catch (const std::exception& e) {
        logger->Error("Error performing full sync of " + tableName + ": " + e.what());
        sqliteHelper.RollbackTransaction();
        return 0;
    }
}

int TableSyncer::SyncKeyBased(const TableInfo& tableInfo) {
    const std::string& tableName = tableInfo.tableName;
    const std::vector<std::string>& columns = tableInfo.columns;
    const std::string& pkColumn = tableInfo.pkColumn;
    
    if (pkColumn.empty()) {
        logger->Warning("Table " + tableName + " has no primary key, falling back to full sync");
        return SyncFullTable(tableInfo);
    }
    
    auto lastSync = syncState->GetLastSync(tableName);
    std::string lastKeyValue = lastSync.lastKeyValue;
    
    if (lastKeyValue.empty()) {
        logger->Warning("No last key value for " + tableName + ", falling back to full sync");
        return SyncFullTable(tableInfo);
    }
    
    try {
        // Count new/changed rows
        std::string countSql = "SELECT COUNT(*) FROM PUB." + tableName + " WHERE \"" + pkColumn + "\" > ?";
        SQLHSTMT countStmt = odbcHelper.PrepareStatement(countSql);
        
        if (countStmt == SQL_NULL_HSTMT) {
            return 0;
        }
        
        if (!odbcHelper.BindParameter(countStmt, 1, lastKeyValue)) {
            odbcHelper.FreeStatement(countStmt);
            return 0;
        }
        
        if (!odbcHelper.ExecutePreparedStatement(countStmt)) {
            odbcHelper.FreeStatement(countStmt);
            return 0;
        }
        
        int totalNewRows = 0;
        if (odbcHelper.FetchRow(countStmt)) {
            totalNewRows = std::stoi(odbcHelper.GetColumnData(countStmt, 1));
        }
        
        odbcHelper.FreeStatement(countStmt);
        
        logger->Info("Found " + std::to_string(totalNewRows) + " new/changed rows to sync for " + tableName);
        
        if (totalNewRows == 0) {
            return 0;
        }
        
        // Query for new/changed rows
        std::string selectSql = "SELECT ";
        for (size_t i = 0; i < columns.size(); ++i) {
            selectSql += "\"" + columns[i] + "\"";
            if (i < columns.size() - 1) {
                selectSql += ", ";
            }
        }
        selectSql += " FROM PUB." + tableName + " WHERE \"" + pkColumn + "\" > ? ORDER BY \"" + pkColumn + "\"";
        
        SQLHSTMT selectStmt = odbcHelper.PrepareStatement(selectSql);
        if (selectStmt == SQL_NULL_HSTMT) {
            return 0;
        }
        
        if (!odbcHelper.BindParameter(selectStmt, 1, lastKeyValue)) {
            odbcHelper.FreeStatement(selectStmt);
            return 0;
        }
        
        if (!odbcHelper.ExecutePreparedStatement(selectStmt)) {
            odbcHelper.FreeStatement(selectStmt);
            return 0;
        }
        
        // Find the primary key column index
        int pkIndex = -1;
        for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i] == pkColumn) {
                pkIndex = static_cast<int>(i);
                break;
            }
        }
        
        if (pkIndex == -1) {
            logger->Error("Could not find primary key column in result set");
            odbcHelper.FreeStatement(selectStmt);
            return 0;
        }
        
        // Begin transaction and process in batches
        sqliteHelper.BeginTransaction();
        
        int rowsSynced = 0;
        std::string lastValue = lastKeyValue;
        std::vector<std::string> pkValues;
        std::vector<std::vector<std::string>> batchData;
        
        while (odbcHelper.FetchRow(selectStmt)) {
            std::vector<std::string> rowData;
            std::string currentPk;
            
            for (size_t i = 0; i < columns.size(); ++i) {
                std::string value = odbcHelper.GetColumnData(selectStmt, i + 1);
                rowData.push_back(value);
                
                if (static_cast<int>(i) == pkIndex) {
                    currentPk = value;
                    lastValue = value;
                }
            }
            
            batchData.push_back(rowData);
            pkValues.push_back(currentPk);
            
            if (batchData.size() >= static_cast<size_t>(batchSize)) {
                ProcessKeyBasedBatch(tableName, columns, pkColumn, pkValues, batchData);
                
                rowsSynced += batchData.size();
                float progressPct = (totalNewRows > 0) ? static_cast<float>(rowsSynced) / totalNewRows * 100 : 0;
                logger->Info("Synced " + std::to_string(batchData.size()) + " rows for " + tableName + 
                            " (total: " + std::to_string(rowsSynced) + " of " + std::to_string(totalNewRows) + 
                            " (" + std::to_string(progressPct) + "%)");
                
                pkValues.clear();
                batchData.clear();
                
                sqliteHelper.CommitTransaction();
                sqliteHelper.BeginTransaction();
            }
        }
        
        if (!batchData.empty()) {
            ProcessKeyBasedBatch(tableName, columns, pkColumn, pkValues, batchData);
            rowsSynced += batchData.size();
        }
        
        sqliteHelper.CommitTransaction();
        odbcHelper.FreeStatement(selectStmt);
        
        int totalRows = lastSync.rowCount + rowsSynced;
        syncState->UpdateSyncState(tableName, lastValue, "key_based", totalRows);
        
        logger->Info("Completed key-based sync of " + tableName + ": " + std::to_string(rowsSynced) + " new/changed rows");
        return rowsSynced;
    } catch (const std::exception& e) {
        logger->Error("Error performing key-based sync of " + tableName + ": " + e.what());
        sqliteHelper.RollbackTransaction();
        return 0;
    }
}

void TableSyncer::ProcessKeyBasedBatch(
    const std::string& tableName, 
    const std::vector<std::string>& columns, 
    const std::string& pkColumn,
    const std::vector<std::string>& pkValues, 
    const std::vector<std::vector<std::string>>& batchData) {
    
    if (pkValues.empty() || batchData.empty()) {
        return;
    }
    
    try {
        // Delete existing rows
        if (!sqliteHelper.DeleteRows(tableName, pkColumn, pkValues)) {
            logger->Error("Error deleting existing rows for key-based sync");
            return;
        }
        
        // Insert updated rows
        std::string insertSql = "INSERT INTO " + tableName + " (";
        for (size_t i = 0; i < columns.size(); ++i) {
            insertSql += "\"" + columns[i] + "\"";
            if (i < columns.size() - 1) {
                insertSql += ", ";
            }
        }
        insertSql += ") VALUES (";
        for (size_t i = 0; i < columns.size(); ++i) {
            insertSql += "?";
            if (i < columns.size() - 1) {
                insertSql += ", ";
            }
        }
        insertSql += ")";
        
        sqlite3_stmt* insertStmt = sqliteHelper.PrepareStatement(insertSql);
        if (!insertStmt) {
            logger->Error("Error preparing insert statement for key-based sync");
            return;
        }
        
        for (size_t rowIdx = 0; rowIdx < batchData.size(); ++rowIdx) {
            const auto& row = batchData[rowIdx];
            const std::string& pkValue = pkValues[rowIdx];
            
            sqlite3_reset(insertStmt);
            
            for (size_t i = 0; i < row.size() && i < columns.size(); ++i) {
                sqliteHelper.BindParameter(insertStmt, i + 1, row[i]);
            }
            
            int rc = sqlite3_step(insertStmt);
            if (rc != SQLITE_DONE) {
                logger->Error("Error inserting row: " + std::string(sqlite3_errmsg(sqlite3_db_handle(insertStmt))));
            } else if (hashEnabled && !pkValue.empty()) {
                // Update hash in the hash database when key-based sync is used
                std::string rowHash = HashCalculator::CalculateRowHash(row);
                hashDb->StoreHash(tableName, pkValue, rowHash);
            }
        }
        
        sqlite3_finalize(insertStmt);
    } catch (const std::exception& e) {
        logger->Error("Error processing batch: " + std::string(e.what()));
    }
}

int TableSyncer::SyncTimestampBased(const TableInfo& tableInfo) {
    const std::string& tableName = tableInfo.tableName;
    const std::vector<std::string>& columns = tableInfo.columns;
    
    auto lastSync = syncState->GetLastSync(tableName);
    
    if (lastSync.lastSyncTime.empty()) {
        logger->Warning("No last sync time for " + tableName + ", falling back to full sync");
        return SyncFullTable(tableInfo);
    }
    
    try {
        // Find a timestamp or date/time column
        std::string timestampColumn = FindTimestampColumn(columns);
        
        if (timestampColumn.empty()) {
            logger->Warning("No timestamp column found for " + tableName + ", falling back to full sync");
            return SyncFullTable(tableInfo);
        }
        
        logger->Info("Using timestamp column: " + timestampColumn + " for table " + tableName);
        
        // Query for changes since last sync
        std::string selectSql = "SELECT ";
        for (size_t i = 0; i < columns.size(); ++i) {
            selectSql += "\"" + columns[i] + "\"";
            if (i < columns.size() - 1) {
                selectSql += ", ";
            }
        }
        selectSql += " FROM PUB." + tableName + " WHERE \"" + timestampColumn + "\" > ?";
        
        if (!tableInfo.pkColumn.empty()) {
            selectSql += " ORDER BY \"" + tableInfo.pkColumn + "\"";
        }
        
        SQLHSTMT stmt = odbcHelper.PrepareStatement(selectSql);
        if (stmt == SQL_NULL_HSTMT) {
            return 0;
        }
        
        // Format the timestamp as expected by the database
        std::string formattedTimestamp = lastSync.lastSyncTime;
        
        if (!odbcHelper.BindParameter(stmt, 1, formattedTimestamp)) {
            odbcHelper.FreeStatement(stmt);
            return 0;
        }
        
        if (!odbcHelper.ExecutePreparedStatement(stmt)) {
            odbcHelper.FreeStatement(stmt);
            return 0;
        }
        
        // Process rows in batches
        sqliteHelper.BeginTransaction();
        
        int rowsSynced = 0;
        std::vector<std::string> pkValues;
        std::vector<std::vector<std::string>> batchData;
        
        int pkIndex = -1;
        if (!tableInfo.pkColumn.empty()) {
            for (size_t i = 0; i < columns.size(); ++i) {
                if (columns[i] == tableInfo.pkColumn) {
                    pkIndex = static_cast<int>(i);
                    break;
                }
            }
        }
        
        while (odbcHelper.FetchRow(stmt)) {
            std::vector<std::string> rowData;
            std::string pkValue;
            
            for (size_t i = 0; i < columns.size(); ++i) {
                std::string value = odbcHelper.GetColumnData(stmt, i + 1);
                rowData.push_back(value);
                
                if (pkIndex >= 0 && static_cast<int>(i) == pkIndex) {
                    pkValue = value;
                }
            }
            
            if (pkIndex >= 0 && !pkValue.empty()) {
                pkValues.push_back(pkValue);
            } else if (pkIndex >= 0) {
                pkValues.push_back("");
            }
            
            batchData.push_back(rowData);
            
            if (batchData.size() >= static_cast<size_t>(batchSize)) {
                if (pkIndex >= 0) {
                    ProcessKeyBasedBatch(tableName, columns, tableInfo.pkColumn, 
                                       pkValues, batchData);
                } else {
                    // For tables without PKs, insert rows directly
                    std::string insertSql = "INSERT INTO " + tableName + " (";
                    for (size_t i = 0; i < columns.size(); ++i) {
                        insertSql += "\"" + columns[i] + "\"";
                        if (i < columns.size() - 1) {
                            insertSql += ", ";
                        }
                    }
                    insertSql += ") VALUES (";
                    for (size_t i = 0; i < columns.size(); ++i) {
                        insertSql += "?";
                        if (i < columns.size() - 1) {
                            insertSql += ", ";
                        }
                    }
                    insertSql += ")";
                    
                    sqlite3_stmt* insertStmt = sqliteHelper.PrepareStatement(insertSql);
                    if (insertStmt) {
                        for (const auto& row : batchData) {
                            sqlite3_reset(insertStmt);
                            
                            for (size_t i = 0; i < row.size() && i < columns.size(); ++i) {
                                sqliteHelper.BindParameter(insertStmt, i + 1, row[i]);
                            }
                            
                            sqlite3_step(insertStmt);
                        }
                        
                        sqlite3_finalize(insertStmt);
                    }
                }
                
                rowsSynced += batchData.size();
                logger->Info("Processed " + std::to_string(batchData.size()) + 
                           " rows for table " + tableName);
                
                pkValues.clear();
                batchData.clear();
                
                sqliteHelper.CommitTransaction();
                sqliteHelper.BeginTransaction();
            }
        }
        
        if (!batchData.empty()) {
            if (pkIndex >= 0) {
                ProcessKeyBasedBatch(tableName, columns, tableInfo.pkColumn, 
                                   pkValues, batchData);
            } else {
                // Same approach for remaining rows without PKs
                std::string insertSql = "INSERT INTO " + tableName + " (";
                for (size_t i = 0; i < columns.size(); ++i) {
                    insertSql += "\"" + columns[i] + "\"";
                    if (i < columns.size() - 1) {
                        insertSql += ", ";
                    }
                }
                insertSql += ") VALUES (";
                for (size_t i = 0; i < columns.size(); ++i) {
                    insertSql += "?";
                    if (i < columns.size() - 1) {
                        insertSql += ", ";
                    }
                }
                insertSql += ")";
                
                sqlite3_stmt* insertStmt = sqliteHelper.PrepareStatement(insertSql);
                if (insertStmt) {
                    for (const auto& row : batchData) {
                        sqlite3_reset(insertStmt);
                        
                        for (size_t i = 0; i < row.size() && i < columns.size(); ++i) {
                            sqliteHelper.BindParameter(insertStmt, i + 1, row[i]);
                        }
                        
                        sqlite3_step(insertStmt);
                    }
                    
                    sqlite3_finalize(insertStmt);
                }
            }
            rowsSynced += batchData.size();
        }
        
        sqliteHelper.CommitTransaction();
        odbcHelper.FreeStatement(stmt);
        
        // Update the last sync time
        std::string lastKeyValue = "";
        if (pkIndex >= 0 && !pkValues.empty()) {
            lastKeyValue = pkValues.back();
        }
        
        int totalRows = lastSync.rowCount + rowsSynced;
        syncState->UpdateSyncState(tableName, lastKeyValue, "timestamp", totalRows);
        
        logger->Info("Completed timestamp-based sync of " + tableName + ": " + 
                   std::to_string(rowsSynced) + " changed rows");
        
        return rowsSynced;
    } catch (const std::exception& e) {
        logger->Error("Error performing timestamp-based sync of " + tableName + ": " + e.what());
        sqliteHelper.RollbackTransaction();
        return 0;
    }
}

int TableSyncer::SyncHashBased(const TableInfo& tableInfo) {
    const std::string& tableName = tableInfo.tableName;
    const std::vector<std::string>& columns = tableInfo.columns;
    const std::string& pkColumn = tableInfo.pkColumn;
    
    if (pkColumn.empty() || !hashEnabled || !hashDb) {
        logger->Warning("Unable to use hash-based sync for " + tableName + ", falling back to key-based");
        return SyncKeyBased(tableInfo);
    }
    
    try {
        // Query all rows
        std::string selectSql = "SELECT ";
        for (size_t i = 0; i < columns.size(); ++i) {
            selectSql += "\"" + columns[i] + "\"";
            if (i < columns.size() - 1) {
                selectSql += ", ";
            }
        }
        selectSql += " FROM PUB." + tableName + " ORDER BY \"" + pkColumn + "\"";
        
        SQLHSTMT stmt = odbcHelper.ExecuteQuery(selectSql);
        if (stmt == SQL_NULL_HSTMT) {
            return 0;
        }
        
        // Find the primary key column index
        int pkIndex = -1;
        for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i] == pkColumn) {
                pkIndex = static_cast<int>(i);
                break;
            }
        }
        
        if (pkIndex == -1) {
            logger->Error("Could not find primary key column in result set");
            odbcHelper.FreeStatement(stmt);
            return 0;
        }
        
        // Process rows in batches
        sqliteHelper.BeginTransaction();
        
        int rowsSynced = 0;
        std::vector<std::string> pkValues;
        std::vector<std::string> rowHashes;
        std::vector<std::vector<std::string>> batchData;
        
        while (odbcHelper.FetchRow(stmt)) {
            std::vector<std::string> rowData;
            std::string pkValue;
            
            for (size_t i = 0; i < columns.size(); ++i) {
                std::string value = odbcHelper.GetColumnData(stmt, i + 1);
                rowData.push_back(value);
                
                if (static_cast<int>(i) == pkIndex) {
                    pkValue = value;
                }
            }
            
            if (!pkValue.empty()) {
                std::string rowHash = HashCalculator::CalculateRowHash(rowData);
                pkValues.push_back(pkValue);
                rowHashes.push_back(rowHash);
                batchData.push_back(rowData);
            }
            
            if (batchData.size() >= static_cast<size_t>(batchSize)) {
                auto changedRows = hashDb->GetChangedRows(tableName, pkValues, rowHashes);
                
                if (!changedRows.empty()) {
                    std::vector<std::string> changedPks;
                    std::vector<std::vector<std::string>> changedData;
                    
                    for (size_t i = 0; i < pkValues.size(); ++i) {
                        auto it = std::find(changedRows.begin(), changedRows.end(), pkValues[i]);
                        if (it != changedRows.end()) {
                            changedPks.push_back(pkValues[i]);
                            changedData.push_back(batchData[i]);
                        }
                    }
                    
                    if (!changedPks.empty()) {
                        ProcessHashBasedBatch(tableName, columns, pkColumn, changedPks, changedData);
                        rowsSynced += changedPks.size();
                    }
                }
                
                logger->Info("Processed " + std::to_string(batchData.size()) + " rows for " + tableName + 
                           ", found " + std::to_string(changedRows.size()) + " changes");
                
                pkValues.clear();
                rowHashes.clear();
                batchData.clear();
                
                sqliteHelper.CommitTransaction();
                sqliteHelper.BeginTransaction();
            }
        }
        
        if (!batchData.empty()) {
            auto changedRows = hashDb->GetChangedRows(tableName, pkValues, rowHashes);
            
            if (!changedRows.empty()) {
                std::vector<std::string> changedPks;
                std::vector<std::vector<std::string>> changedData;
                
                for (size_t i = 0; i < pkValues.size(); ++i) {
                    auto it = std::find(changedRows.begin(), changedRows.end(), pkValues[i]);
                    if (it != changedRows.end()) {
                        changedPks.push_back(pkValues[i]);
                        changedData.push_back(batchData[i]);
                    }
                }
                
                if (!changedPks.empty()) {
                    ProcessHashBasedBatch(tableName, columns, pkColumn, changedPks, changedData);
                    rowsSynced += changedPks.size();
                }
            }
            
            logger->Info("Processed final " + std::to_string(batchData.size()) + " rows for " + tableName + 
                       ", found " + std::to_string(changedRows.size()) + " changes");
        }
        
        sqliteHelper.CommitTransaction();
        odbcHelper.FreeStatement(stmt);
        
        logger->Info("Completed hash-based sync of " + tableName + ": " + 
                    std::to_string(rowsSynced) + " changed rows");
        
        auto lastSync = syncState->GetLastSync(tableName);
        int totalRows = lastSync.rowCount;
        syncState->UpdateSyncState(tableName, "", "hash_based", totalRows);
        
        return rowsSynced;
    } catch (const std::exception& e) {
        logger->Error("Error performing hash-based sync of " + tableName + ": " + e.what());
        sqliteHelper.RollbackTransaction();
        return 0;
    }
}

void TableSyncer::ProcessHashBasedBatch(
    const std::string& tableName,
    const std::vector<std::string>& columns,
    const std::string& pkColumn,
    const std::vector<std::string>& pkValues,
    const std::vector<std::vector<std::string>>& batchData) {
    
    if (pkValues.empty() || batchData.empty()) {
        return;
    }
    
    try {
        // Delete existing rows
        if (!sqliteHelper.DeleteRows(tableName, pkColumn, pkValues)) {
            logger->Error("Error deleting existing rows for hash-based sync");
            return;
        }
        
        // Insert updated rows
        std::string insertSql = "INSERT INTO " + tableName + " (";
        for (size_t i = 0; i < columns.size(); ++i) {
            insertSql += "\"" + columns[i] + "\"";
            if (i < columns.size() - 1) {
                insertSql += ", ";
            }
        }
        insertSql += ") VALUES (";
        for (size_t i = 0; i < columns.size(); ++i) {
            insertSql += "?";
            if (i < columns.size() - 1) {
                insertSql += ", ";
            }
        }
        insertSql += ")";
        
        sqlite3_stmt* insertStmt = sqliteHelper.PrepareStatement(insertSql);
        if (!insertStmt) {
            logger->Error("Error preparing insert statement for hash-based sync");
            return;
        }
        
        for (size_t rowIdx = 0; rowIdx < batchData.size(); ++rowIdx) {
            const auto& row = batchData[rowIdx];
            const std::string& pkValue = pkValues[rowIdx];
            
            sqlite3_reset(insertStmt);
            
            for (size_t i = 0; i < row.size() && i < columns.size(); ++i) {
                sqliteHelper.BindParameter(insertStmt, i + 1, row[i]);
            }
            
            int rc = sqlite3_step(insertStmt);
            if (rc != SQLITE_DONE) {
                logger->Error("Error inserting row: " + std::string(sqlite3_errmsg(sqlite3_db_handle(insertStmt))));
            } else {
                // Update hash in the hash database
                std::string rowHash = HashCalculator::CalculateRowHash(row);
hashDb->StoreHash(tableName, pkValue, rowHash);
            }
        }
        
        sqlite3_finalize(insertStmt);
        logger->Info("Updated " + std::to_string(batchData.size()) + " rows in hash-based sync");
    } catch (const std::exception& e) {
        logger->Error("Error processing hash-based batch: " + std::string(e.what()));
    }
}

bool TableSyncer::EnsureTargetTable(const TableInfo& tableInfo) {
    if (!sqliteHelper.ExecuteNonQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='" + 
                                      tableInfo.tableName + "'")) {
        return false;
    }
    
    const std::string& tableName = tableInfo.tableName;
    const std::vector<std::string>& columns = tableInfo.columns;
    
    try {
        // Check if table exists
        std::string checkSql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
        sqlite3_stmt* stmt = sqliteHelper.PrepareStatement(checkSql);
        
        if (!stmt) {
            return false;
        }
        
        sqliteHelper.BindParameter(stmt, 1, tableName);
        
        bool tableExists = false;
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            tableExists = true;
        }
        
        sqlite3_finalize(stmt);
        
        if (!tableExists) {
            // Create table
            std::string createSql = "CREATE TABLE " + tableName + " (";
            for (size_t i = 0; i < columns.size(); ++i) {
                createSql += "\"" + columns[i] + "\" TEXT";
                if (i < columns.size() - 1) {
                    createSql += ", ";
                }
            }
            createSql += ")";
            
            if (!sqliteHelper.ExecuteNonQuery(createSql)) {
                return false;
            }
            
            logger->Info("Created table " + tableName);
        } else {
            // Check existing columns
            std::string pragmaSql = "PRAGMA table_info(" + tableName + ")";
            stmt = sqliteHelper.PrepareStatement(pragmaSql);
            
            if (!stmt) {
                return false;
            }
            
            std::set<std::string> existingColumns;
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                const char* colName = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
                std::string colNameStr = colName ? colName : "";
                std::transform(colNameStr.begin(), colNameStr.end(), colNameStr.begin(),
                              [](unsigned char c) { return std::tolower(c); });
                
                existingColumns.insert(colNameStr);
            }
            
            sqlite3_finalize(stmt);
            
            // Add missing columns
            for (const auto& col : columns) {
                std::string colLower = col;
                std::transform(colLower.begin(), colLower.end(), colLower.begin(),
                              [](unsigned char c) { return std::tolower(c); });
                
                if (existingColumns.find(colLower) == existingColumns.end()) {
                    std::string alterSql = "ALTER TABLE " + tableName + " ADD COLUMN \"" + col + "\" TEXT";
                    
                    if (!sqliteHelper.ExecuteNonQuery(alterSql)) {
                        logger->Warning("Failed to add column " + col + " to table " + tableName);
                    } else {
                        logger->Info("Added column " + col + " to table " + tableName);
                    }
                }
            }
        }
        
        return true;
    } catch (const std::exception& e) {
        logger->Error("Error ensuring target table: " + std::string(e.what()));
        return false;
    }
}

int TableSyncer::GetSourceRowCount(const std::string& tableName) {
    std::string countSql = "SELECT COUNT(*) FROM PUB." + tableName;
    SQLHSTMT stmt = odbcHelper.ExecuteQuery(countSql);
    
    if (stmt == SQL_NULL_HSTMT) {
        return 0;
    }
    
    int count = 0;
    if (odbcHelper.FetchRow(stmt)) {
        std::string countStr = odbcHelper.GetColumnData(stmt, 1);
        if (!countStr.empty()) {
            count = std::stoi(countStr);
        }
    }
    
    odbcHelper.FreeStatement(stmt);
    logger->Info("Source table " + tableName + " has " + std::to_string(count) + " rows");
    
    return count;
}

std::string TableSyncer::FindTimestampColumn(const std::vector<std::string>& columns) {
    for (const auto& col : columns) {
        std::string lowerCol = col;
        std::transform(lowerCol.begin(), lowerCol.end(), lowerCol.begin(),
                      [](unsigned char c) { return std::tolower(c); });
        
        // Look for common timestamp column patterns
        if (lowerCol.find("timestamp") != std::string::npos || 
            lowerCol.find("modified") != std::string::npos ||
            lowerCol.find("updated") != std::string::npos ||
            lowerCol.find("changed") != std::string::npos ||
            lowerCol.find("datetime") != std::string::npos) {
            return col;
        }
    }
    
    return "";
}