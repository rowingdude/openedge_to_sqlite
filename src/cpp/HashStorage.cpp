#include "HashStorage.h"
#include <stdexcept>

HashStorage::HashStorage(const std::string& dbPath, std::shared_ptr<Logger> logger)
    : dbPath(dbPath), dbConn(nullptr), logger(logger) {
}

HashStorage::~HashStorage() {
    if (dbConn) {
        sqlite3_close(dbConn);
        dbConn = nullptr;
    }
}

bool HashStorage::Initialize() {
    int rc = sqlite3_open(dbPath.c_str(), &dbConn);
    if (rc != SQLITE_OK) {
        logger->Error("Failed to open hash database: " + dbPath + " - " + sqlite3_errmsg(dbConn));
        return false;
    }
    
    logger->Info("Connected to hash database: " + dbPath);
    return EnsureHashTable();
}

bool HashStorage::EnsureHashTable() {
    const char* createTableSql = 
        "CREATE TABLE IF NOT EXISTS row_hashes ("
        "table_name TEXT NOT NULL,"
        "pk_value TEXT NOT NULL,"
        "row_hash TEXT NOT NULL,"
        "last_updated TEXT NOT NULL,"
        "PRIMARY KEY (table_name, pk_value)"
        ")";
    
    char* errMsg = nullptr;
    int rc = sqlite3_exec(dbConn, createTableSql, nullptr, nullptr, &errMsg);
    
    if (rc != SQLITE_OK) {
        std::string error = "Error creating hash table: ";
        if (errMsg) {
            error += errMsg;
            sqlite3_free(errMsg);
        }
        logger->Error(error);
        return false;
    }
    
    logger->Info("Ensured hash table exists");
    return true;
}

bool HashStorage::StoreHash(const std::string& tableName, const std::string& pkValue, const std::string& rowHash) {
    const char* insertSql = 
        "INSERT OR REPLACE INTO row_hashes (table_name, pk_value, row_hash, last_updated) "
        "VALUES (?, ?, ?, datetime('now'))";
    
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(dbConn, insertSql, -1, &stmt, nullptr);
    
    if (rc != SQLITE_OK) {
        logger->Error("Error preparing hash insert statement: " + std::string(sqlite3_errmsg(dbConn)));
        return false;
    }
    
    sqlite3_bind_text(stmt, 1, tableName.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, pkValue.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, rowHash.c_str(), -1, SQLITE_STATIC);
    
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    
    if (rc != SQLITE_DONE) {
        logger->Error("Error storing hash: " + std::string(sqlite3_errmsg(dbConn)));
        return false;
    }
    
    return true;
}

std::string HashStorage::GetHash(const std::string& tableName, const std::string& pkValue) {
    const char* selectSql = 
        "SELECT row_hash FROM row_hashes WHERE table_name = ? AND pk_value = ?";
    
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(dbConn, selectSql, -1, &stmt, nullptr);
    
    if (rc != SQLITE_OK) {
        logger->Error("Error preparing hash select statement: " + std::string(sqlite3_errmsg(dbConn)));
        return "";
    }
    
    sqlite3_bind_text(stmt, 1, tableName.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, pkValue.c_str(), -1, SQLITE_STATIC);
    
    std::string hash;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char* hashText = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        if (hashText) {
            hash = hashText;
        }
    }
    
    sqlite3_finalize(stmt);
    return hash;
}

bool HashStorage::DeleteHash(const std::string& tableName, const std::string& pkValue) {
    const char* deleteSql = 
        "DELETE FROM row_hashes WHERE table_name = ? AND pk_value = ?";
    
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(dbConn, deleteSql, -1, &stmt, nullptr);
    
    if (rc != SQLITE_OK) {
        logger->Error("Error preparing hash delete statement: " + std::string(sqlite3_errmsg(dbConn)));
        return false;
    }
    
    sqlite3_bind_text(stmt, 1, tableName.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, pkValue.c_str(), -1, SQLITE_STATIC);
    
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    
    if (rc != SQLITE_DONE) {
        logger->Error("Error deleting hash: " + std::string(sqlite3_errmsg(dbConn)));
        return false;
    }
    
    return true;
}

bool HashStorage::DeleteTableHashes(const std::string& tableName) {
    const char* deleteSql = "DELETE FROM row_hashes WHERE table_name = ?";
    
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(dbConn, deleteSql, -1, &stmt, nullptr);
    
    if (rc != SQLITE_OK) {
        logger->Error("Error preparing table hash delete statement: " + std::string(sqlite3_errmsg(dbConn)));
        return false;
    }
    
    sqlite3_bind_text(stmt, 1, tableName.c_str(), -1, SQLITE_STATIC);
    
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    
    if (rc != SQLITE_DONE) {
        logger->Error("Error deleting table hashes: " + std::string(sqlite3_errmsg(dbConn)));
        return false;
    }
    
    return true;
}

std::vector<std::string> HashStorage::GetChangedRows(
    const std::string& tableName, 
    const std::vector<std::string>& pkValues,
    const std::vector<std::string>& rowHashes) {
    
    std::vector<std::string> changedRows;
    
    if (pkValues.size() != rowHashes.size()) {
        logger->Error("Mismatch between primary key and hash array sizes");
        return changedRows;
    }
    
    for (size_t i = 0; i < pkValues.size(); ++i) {
        std::string storedHash = GetHash(tableName, pkValues[i]);
        
        if (storedHash.empty() || storedHash != rowHashes[i]) {
            changedRows.push_back(pkValues[i]);
        }
    }
    
    return changedRows;
}