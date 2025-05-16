#include "hash_rows.h"
#include <stdexcept>

hash_rows::hash_rows(const std::string& db_path, std::shared_ptr<Logger> logger)
    : db_path(db_path), db_conn(nullptr), logger(logger) {
}

hash_rows::~hash_rows() {
    if (db_conn) {
        sqlite3_close(db_conn);
        db_conn = nullptr;
    }
}

bool hash_rows::initialize() {
    int rc = sqlite3_open(db_path.c_str(), &db_conn);
    if (rc != SQLITE_OK) {
        logger->error("Failed to open hash database: " + db_path + " - " + sqlite3_errmsg(db_conn));
        return false;
    }
    
    logger->info("Connected to hash database: " + db_path);
    return ensure_hash_table();
}

bool hash_rows::ensure_hash_table() {
    const char* create_table_sql = 
        "CREATE TABLE IF NOT EXISTS row_hashes ("
        "table_name TEXT NOT NULL,"
        "pk_value TEXT NOT NULL,"
        "row_hash TEXT NOT NULL,"
        "last_updated TEXT NOT NULL,"
        "PRIMARY KEY (table_name, pk_value)"
        ")";
    
    char* err_msg = nullptr;
    int rc = sqlite3_exec(db_conn, create_table_sql, nullptr, nullptr, &err_msg);
    
    if (rc != SQLITE_OK) {
        std::string error = "Error creating hash table: ";
        if (err_msg) {
            error += err_msg;
            sqlite3_free(err_msg);
        }
        logger->error(error);
        return false;
    }
    
    logger->info("Ensured hash table exists");
    return true;
}

bool hash_rows::store_hash(const std::string& table_name, const std::string& pk_value, const std::string& row_hash) {
    const char* insert_sql = 
        "INSERT OR REPLACE INTO row_hashes (table_name, pk_value, row_hash, last_updated) "
        "VALUES (?, ?, ?, datetime('now'))";
    
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_conn, insert_sql, -1, &stmt, nullptr);
    
    if (rc != SQLITE_OK) {
        logger->error("Error preparing hash insert statement: " + std::string(sqlite3_errmsg(db_conn)));
        return false;
    }
    
    sqlite3_bind_text(stmt, 1, table_name.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, pk_value.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, row_hash.c_str(), -1, SQLITE_STATIC);
    
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    
    if (rc != SQLITE_DONE) {
        logger->error("Error storing hash: " + std::string(sqlite3_errmsg(db_conn)));
        return false;
    }
    
    return true;
}

std::string hash_rows::get_hash(const std::string& table_name, const std::string& pk_value) {
    const char* select_sql = 
        "SELECT row_hash FROM row_hashes WHERE table_name = ? AND pk_value = ?";
    
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_conn, select_sql, -1, &stmt, nullptr);
    
    if (rc != SQLITE_OK) {
        logger->error("Error preparing hash select statement: " + std::string(sqlite3_errmsg(db_conn)));
        return "";
    }
    
    sqlite3_bind_text(stmt, 1, table_name.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, pk_value.c_str(), -1, SQLITE_STATIC);
    
    std::string hash;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char* hash_text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        if (hash_text) {
            hash = hash_text;
        }
    }
    
    sqlite3_finalize(stmt);
    return hash;
}

bool hash_rows::delete_hash(const std::string& table_name, const std::string& pk_value) {
    const char* delete_sql = 
        "DELETE FROM row_hashes WHERE table_name = ? AND pk_value = ?";
    
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_conn, delete_sql, -1, &stmt, nullptr);
    
    if (rc != SQLITE_OK) {
        logger->error("Error preparing hash delete statement: " + std::string(sqlite3_errmsg(db_conn)));
        return false;
    }
    
    sqlite3_bind_text(stmt, 1, table_name.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, pk_value.c_str(), -1, SQLITE_STATIC);
    
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    
    if (rc != SQLITE_DONE) {
        logger->error("Error deleting hash: " + std::string(sqlite3_errmsg(db_conn)));
        return false;
    }
    
    return true;
}

bool hash_rows::delete_table_hashes(const std::string& table_name) {
    const char* delete_sql = "DELETE FROM row_hashes WHERE table_name = ?";
    
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_conn, delete_sql, -1, &stmt, nullptr);
    
    if (rc != SQLITE_OK) {
        logger->error("Error preparing table hash delete statement: " + std::string(sqlite3_errmsg(db_conn)));
        return false;
    }
    
    sqlite3_bind_text(stmt, 1, table_name.c_str(), -1, SQLITE_STATIC);
    
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    
    if (rc != SQLITE_DONE) {
        logger->error("Error deleting table hashes: " + std::string(sqlite3_errmsg(db_conn)));
        return false;
    }
    
    return true;
}

std::vector<std::string> hash_rows::get_changed_rows(
    const std::string& table_name, 
    const std::vector<std::string>& pk_values,
    const std::vector<std::string>& row_hashes) {
    
    std::vector<std::string> changed_rows;
    
    if (pk_values.size() != row_hashes.size()) {
        logger->error("Mismatch between primary key and hash array sizes");
        return changed_rows;
    }
    
    for (size_t i = 0; i < pk_values.size(); ++i) {
        std::string stored_hash = get_hash(table_name, pk_values[i]);
        
        if (stored_hash.empty() || stored_hash != row_hashes[i]) {
            changed_rows.push_back(pk_values[i]);
        }
    }
    
    return changed_rows;
}