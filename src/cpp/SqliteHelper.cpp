#include "SqliteHelper.h"
#include <sstream>

SqliteHelper::SqliteHelper(sqlite3* connection, std::shared_ptr<Logger> logger)
    : connection(connection), logger(logger) {
}

void SqliteHelper::BeginTransaction() {
    ExecuteNonQuery("BEGIN TRANSACTION");
}

void SqliteHelper::CommitTransaction() {
    ExecuteNonQuery("COMMIT");
}

void SqliteHelper::RollbackTransaction() {
    ExecuteNonQuery("ROLLBACK");
}

bool SqliteHelper::ExecuteNonQuery(const std::string& sql) {
    char* errMsg = nullptr;
    int rc = sqlite3_exec(connection, sql.c_str(), nullptr, nullptr, &errMsg);
    
    if (rc != SQLITE_OK) {
        std::string error = "SQL error: ";
        if (errMsg) {
            error += errMsg;
            sqlite3_free(errMsg);
        }
        logger->Error(error);
        return false;
    }
    
    return true;
}

bool SqliteHelper::ExecuteNonQuery(const std::string& sql, const std::vector<std::string>& parameters) {
    sqlite3_stmt* stmt = PrepareStatement(sql);
    if (!stmt) {
        return false;
    }
    
    if (!BindParameters(stmt, parameters)) {
        sqlite3_finalize(stmt);
        return false;
    }
    
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    
    if (rc != SQLITE_DONE) {
        logger->Error("SQL execution error: " + std::string(sqlite3_errmsg(connection)));
        return false;
    }
    
    return true;
}

sqlite3_stmt* SqliteHelper::PrepareStatement(const std::string& sql) {
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(connection, sql.c_str(), -1, &stmt, nullptr);
    
    if (rc != SQLITE_OK) {
        logger->Error("Error preparing statement: " + std::string(sqlite3_errmsg(connection)));
        return nullptr;
    }
    
    return stmt;
}

bool SqliteHelper::BindParameters(sqlite3_stmt* stmt, const std::vector<std::string>& parameters) {
    for (size_t i = 0; i < parameters.size(); ++i) {
        if (!BindParameter(stmt, i + 1, parameters[i])) {
            return false;
        }
    }
    
    return true;
}

bool SqliteHelper::BindParameter(sqlite3_stmt* stmt, int index, const std::string& value) {
    if (value.empty()) {
        int rc = sqlite3_bind_null(stmt, index);
        if (rc != SQLITE_OK) {
            logger->Error("Error binding NULL parameter: " + std::string(sqlite3_errmsg(connection)));
            return false;
        }
    } else {
        int rc = sqlite3_bind_text(stmt, index, value.c_str(), -1, SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            logger->Error("Error binding text parameter: " + std::string(sqlite3_errmsg(connection)));
            return false;
        }
    }
    
    return true;
}

bool SqliteHelper::InsertRow(const std::string& tableName, 
                            const std::vector<std::string>& columns,
                            const std::vector<std::string>& values) {
    
    if (columns.size() != values.size() || columns.empty()) {
        logger->Error("Column and value counts don't match or are empty");
        return false;
    }
    
    std::ostringstream sql;
    sql << "INSERT INTO " << tableName << " (";
    
    for (size_t i = 0; i < columns.size(); ++i) {
        sql << "\"" << columns[i] << "\"";
        if (i < columns.size() - 1) {
            sql << ", ";
        }
    }
    
    sql << ") VALUES (";
    
    for (size_t i = 0; i < values.size(); ++i) {
        sql << "?";
        if (i < values.size() - 1) {
            sql << ", ";
        }
    }
    
    sql << ")";
    
    return ExecuteNonQuery(sql.str(), values);
}

bool SqliteHelper::DeleteRows(const std::string& tableName, 
                             const std::string& whereColumn,
                             const std::vector<std::string>& whereValues) {
    
    if (whereValues.empty()) {
        return true; // Nothing to delete
    }
    
    std::ostringstream sql;
    sql << "DELETE FROM " << tableName << " WHERE \"" << whereColumn << "\" IN (";
    
    for (size_t i = 0; i < whereValues.size(); ++i) {
        sql << "?";
        if (i < whereValues.size() - 1) {
            sql << ", ";
        }
    }
    
    sql << ")";
    
    return ExecuteNonQuery(sql.str(), whereValues);
}