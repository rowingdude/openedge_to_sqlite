#ifndef SQLITE_HELPER_H
#define SQLITE_HELPER_H

#include <string>
#include <vector>
#include <memory>
#include <sqlite3.h>
#include "Logger.h"

class SqliteHelper {
public:
    SqliteHelper(sqlite3* connection, std::shared_ptr<Logger> logger);
    
    // Transaction methods
    void BeginTransaction();
    void CommitTransaction();
    void RollbackTransaction();
    
    // Execute SQL with parameters
    bool ExecuteNonQuery(const std::string& sql);
    bool ExecuteNonQuery(const std::string& sql, const std::vector<std::string>& parameters);
    
    // Prepare and bind statement
    sqlite3_stmt* PrepareStatement(const std::string& sql);
    bool BindParameters(sqlite3_stmt* stmt, const std::vector<std::string>& parameters);
    bool BindParameter(sqlite3_stmt* stmt, int index, const std::string& value);
    
    // Insert rows
    bool InsertRow(const std::string& tableName, 
                  const std::vector<std::string>& columns,
                  const std::vector<std::string>& values);
    
    // Delete rows with specified condition
    bool DeleteRows(const std::string& tableName, 
                   const std::string& whereColumn,
                   const std::vector<std::string>& whereValues);

private:
    sqlite3* connection;
    std::shared_ptr<Logger> logger;
};

#endif // SQLITE_HELPER_H