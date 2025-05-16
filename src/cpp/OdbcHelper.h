#ifndef ODBC_HELPER_H
#define ODBC_HELPER_H

#include <string>
#include <vector>
#include <memory>
#include <sql.h>
#include <sqlext.h>
#include "Logger.h"

struct OdbcColumn {
    std::string name;
    SQLSMALLINT dataType;
    SQLULEN columnSize;
};

class OdbcHelper {
public:
    OdbcHelper(SQLHDBC connection, SQLHENV environment, std::shared_ptr<Logger> logger);
    
    // Execute SQL statements
    SQLHSTMT ExecuteQuery(const std::string& sql);
    
    // Execute parameterized queries
    SQLHSTMT PrepareStatement(const std::string& sql);
    bool BindParameter(SQLHSTMT statement, int paramIndex, const std::string& value);
    bool ExecutePreparedStatement(SQLHSTMT statement);
    
    // Fetch row data
    bool FetchRow(SQLHSTMT statement);
    std::string GetColumnData(SQLHSTMT statement, int columnIndex);
    
    // Get metadata
    std::vector<OdbcColumn> GetColumns(SQLHSTMT statement);
    std::vector<std::string> GetTableList(const std::string& schema = "");
    std::string GetPrimaryKeyColumn(const std::string& schema, const std::string& tableName);
    
    // Helper for fetching a batch of rows
    std::vector<std::vector<std::string>> FetchBatch(SQLHSTMT statement, int batchSize);
    
    // Free handles
    void FreeStatement(SQLHSTMT statement);
    
    // Error checking
    std::string GetLastError(SQLHANDLE handle, SQLSMALLINT handleType);

private:
    SQLHDBC connection;
    SQLHENV environment;
    std::shared_ptr<Logger> logger;
    static constexpr size_t SQL_BUFFER_SIZE = 8192;
    
    void CheckError(SQLHANDLE handle, SQLSMALLINT handleType, const std::string& action);
};

#endif 