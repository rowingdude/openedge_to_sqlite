#include "SyncState.h"
#include <stdexcept>

SyncState::SyncState(sqlite3* sqliteConn, std::shared_ptr<Logger> logger) 
    : sqliteConn(sqliteConn), logger(logger) {
    EnsureStateTable();
}

void SyncState::EnsureStateTable() {
    const char* createTableSql = 
        "CREATE TABLE IF NOT EXISTS sync_state ("
        "table_name TEXT PRIMARY KEY,"
        "last_sync_time TEXT,"
        "last_key_value TEXT,"
        "sync_method TEXT DEFAULT 'timestamp',"
        "row_count INTEGER DEFAULT 0"
        ")";
    
    char* errMsg = nullptr;
    int rc = sqlite3_exec(sqliteConn, createTableSql, nullptr, nullptr, &errMsg);
    
    if (rc != SQLITE_OK) {
        std::string error = "Error creating sync state table: ";
        if (errMsg) {
            error += errMsg;
            sqlite3_free(errMsg);
        }
        logger->Error(error);
        throw std::runtime_error(error);
    }
    
    logger->Info("Ensured sync state table exists");
}

SyncState::SyncData SyncState::GetLastSync(const std::string& tableName) {
    SyncData result;
    
    const char* selectSql = 
        "SELECT last_sync_time, last_key_value, sync_method, row_count "
        "FROM sync_state "
        "WHERE table_name = ?";
    
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(sqliteConn, selectSql, -1, &stmt, nullptr);
    
    if (rc != SQLITE_OK) {
        logger->Error("Error preparing sync state query: " + std::string(sqlite3_errmsg(sqliteConn)));
        return result;
    }
    
    sqlite3_bind_text(stmt, 1, tableName.c_str(), -1, SQLITE_STATIC);
    
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char* syncTime = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        const char* keyValue = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        const char* method = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
        
        result.lastSyncTime = syncTime ? syncTime : "";
        result.lastKeyValue = keyValue ? keyValue : "";
        result.syncMethod = method ? method : "timestamp";
        result.rowCount = sqlite3_column_int(stmt, 3);
    }
    
    sqlite3_finalize(stmt);
    return result;
}

void SyncState::UpdateSyncState(const std::string& tableName, 
                              const std::string& lastKeyValue, 
                              const std::string& syncMethod, 
                              int rowCount) {
    const char* updateSql = 
        "INSERT OR REPLACE INTO sync_state "
        "(table_name, last_sync_time, last_key_value, sync_method, row_count) "
        "VALUES (?, datetime('now'), ?, ?, ?)";
    
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(sqliteConn, updateSql, -1, &stmt, nullptr);
    
    if (rc != SQLITE_OK) {
        logger->Error("Error preparing sync state update: " + std::string(sqlite3_errmsg(sqliteConn)));
        return;
    }
    
    sqlite3_bind_text(stmt, 1, tableName.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, lastKeyValue.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, syncMethod.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 4, rowCount);
    
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        logger->Error("Error updating sync state: " + std::string(sqlite3_errmsg(sqliteConn)));
    } else {
        logger->Info("Updated sync state for " + tableName + ", key: " + lastKeyValue + 
                    ", rows: " + std::to_string(rowCount));
    }
    
    sqlite3_finalize(stmt);
}