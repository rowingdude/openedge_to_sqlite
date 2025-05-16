#include "DataSyncManager.h"
#include <iostream>
#include <fstream>
#include <algorithm>

DataSyncManager::DataSyncManager(const std::string& configFile, bool fullSync, const std::vector<std::string>& ignoreTables)
    : configFile(configFile), fullSync(fullSync) {
    
    config = Config(configFile);
    
    logger = std::make_shared<Logger>(config.mirrorSettings.logFile);
    
    metrics.tablesProcessed = 0;
    metrics.rowsSynced = 0;
    metrics.startTime = time(nullptr);
    
    LoadIgnoreList();
    
    if (!ignoreTables.empty()) {
        AddToIgnoreList(ignoreTables);
    }
}

DataSyncManager::~DataSyncManager() {
    // Proper destruction order is handled by smart pointers
}

void DataSyncManager::RunSync() {
    logger->Info("Starting data sync (fullSync=" + std::string(fullSync ? "true" : "false") + ")");
    
    metrics.startTime = time(nullptr);
    
    try {
        // Initialize database connector
        dbConnector = std::make_unique<DatabaseConnector>(config, logger);
        
        if (!dbConnector->Connect()) {
            logger->Error("Failed to connect to databases");
            return;
        }
        
        // Initialize helpers
        sqliteHelper = std::make_unique<SqliteHelper>(dbConnector->GetSqliteConnection(), logger);
        odbcHelper = std::make_unique<OdbcHelper>(dbConnector->GetOdbcConnection(), dbConnector->GetOdbcEnvironment(), logger);
        
        // Initialize state tracking
        syncState = std::make_shared<SyncState>(dbConnector->GetSqliteConnection(), logger);
        
        // Initialize hash database if enabled
        if (config.hashDb.enableHashing) {
            hashDb = std::make_shared<HashStorage>(config.hashDb.dbPath, logger);
            if (!hashDb->Initialize()) {
                logger->Error("Failed to initialize hash database");
                hashDb.reset();
            } else {
                logger->Info("Hash database initialized successfully");
            }
        }
        
        // Create table syncer
        tableSyncer = std::make_unique<TableSyncer>(
            *sqliteHelper,
            *odbcHelper,
            syncState,
            hashDb,
            logger,
            config.mirrorSettings.batchSize
        );
        
        // Get tables to sync
        auto tables = GetSourceTables();
        if (tables.empty()) {
            logger->Error("No tables found to sync");
            return;
        }
        
        logger->Info("Found " + std::to_string(tables.size()) + " tables to sync");
        
        // Process each table
        int tableIndex = 1;
        for (const auto& tableInfo : tables) {
            logger->Info("Processing table " + std::to_string(tableIndex) + "/" + 
                        std::to_string(tables.size()) + ": " + tableInfo.tableName);
            
            try {
                int rows = tableSyncer->SyncTable(tableInfo, fullSync);
                
                metrics.tablesProcessed++;
                metrics.rowsSynced += rows;
            } catch (const std::exception& e) {
                logger->Error("Error syncing table " + tableInfo.tableName + ": " + e.what());
            }
            
            tableIndex++;
        }
        
        double duration = difftime(time(nullptr), metrics.startTime);
        logger->Info("Sync completed in " + std::to_string(duration) + " seconds");
        logger->Info("Processed " + std::to_string(metrics.tablesProcessed) + " tables");
        logger->Info("Synced " + std::to_string(metrics.rowsSynced) + " rows");
        
    } catch (const std::exception& e) {
        logger->Error("Sync process failed: " + std::string(e.what()));
    }
}

void DataSyncManager::LoadIgnoreList() {
    std::string ignoreFile = config.mirrorSettings.ignoreFile;
    
    try {
        std::ifstream file(ignoreFile);
        if (file.is_open()) {
            std::string line;
            while (std::getline(file, line)) {
                if (!line.empty()) {
                    // Convert to lowercase
                    std::transform(line.begin(), line.end(), line.begin(), 
                                  [](unsigned char c) { return std::tolower(c); });
                    ignoredTables.insert(line);
                }
            }
            logger->Info("Loaded ignore list with " + std::to_string(ignoredTables.size()) + " tables");
        }
    } catch (const std::exception& e) {
        logger->Error("Error loading ignore file " + ignoreFile + ": " + e.what());
    }
}

void DataSyncManager::AddToIgnoreList(const std::vector<std::string>& tables) {
    std::string ignoreFile = config.mirrorSettings.ignoreFile;
    
    try {
        std::ofstream file(ignoreFile, std::ios::app);
        if (file.is_open()) {
            for (const auto& table : tables) {
                std::string lowerTable = table;
                std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(), 
                              [](unsigned char c) { return std::tolower(c); });
                
                if (ignoredTables.find(lowerTable) == ignoredTables.end()) {
                    file << lowerTable << "\n";
                    ignoredTables.insert(lowerTable);
                }
            }
            logger->Info("Added tables to ignore list");
        } else {
            logger->Error("Error opening ignore file for writing: " + ignoreFile);
        }
    } catch (const std::exception& e) {
        logger->Error(std::string("Error adding tables to ignore file: ") + e.what());
    }
}

std::vector<TableInfo> DataSyncManager::GetSourceTables() {
    std::vector<TableInfo> tables;
    
    try {
        // Get list of tables in the schema
        auto tableNames = odbcHelper->GetTableList("PUB");
        
        for (const auto& tableName : tableNames) {
            std::string lowerTableName = tableName;
            std::transform(lowerTableName.begin(), lowerTableName.end(), lowerTableName.begin(),
                          [](unsigned char c) { return std::tolower(c); });
            
            // Skip system tables and ignored tables
            if (lowerTableName[0] == '_' || ignoredTables.find(lowerTableName) != ignoredTables.end()) {
                continue;
            }
            
            TableInfo tableInfo;
            tableInfo.tableName = lowerTableName;
            
            // Get primary key
            tableInfo.pkColumn = odbcHelper->GetPrimaryKeyColumn("PUB", tableName);
            
            // Transform to lowercase
            std::transform(tableInfo.pkColumn.begin(), tableInfo.pkColumn.end(), tableInfo.pkColumn.begin(),
                          [](unsigned char c) { return std::tolower(c); });
            
            // Get column information
            std::string sql = "SELECT * FROM PUB." + lowerTableName + " WHERE 1=0";
            SQLHSTMT stmt = odbcHelper->ExecuteQuery(sql);
            
            if (stmt != SQL_NULL_HSTMT) {
                auto columns = odbcHelper->GetColumns(stmt);
                
                for (const auto& column : columns) {
                    std::string colName = column.name;
                    std::transform(colName.begin(), colName.end(), colName.begin(),
                                  [](unsigned char c) { return std::tolower(c); });
                    tableInfo.columns.push_back(colName);
                }
                
                odbcHelper->FreeStatement(stmt);
            }
            
            if (!tableInfo.columns.empty()) {
                tables.push_back(tableInfo);
                logger->Info("Found table " + lowerTableName + " with " + 
                            std::to_string(tableInfo.columns.size()) + " columns and PK: " + 
                            (tableInfo.pkColumn.empty() ? "none" : tableInfo.pkColumn));
            }
        }
    } catch (const std::exception& e) {
        logger->Error("Error getting source tables: " + std::string(e.what()));
    }
    
    return tables;
}