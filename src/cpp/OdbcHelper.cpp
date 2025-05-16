#include "OdbcHelper.h"
#include <sstream>

OdbcHelper::OdbcHelper(SQLHDBC connection, SQLHENV environment, std::shared_ptr<Logger> logger)
    : connection(connection), environment(environment), logger(logger) {
}

SQLHSTMT OdbcHelper::ExecuteQuery(const std::string& sql) {
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, connection, &stmt);
    
    if (!SQL_SUCCEEDED(ret)) {
        CheckError(connection, SQL_HANDLE_DBC, "allocating statement handle");
        return SQL_NULL_HSTMT;
    }
    
    ret = SQLExecDirect(stmt, (SQLCHAR*)sql.c_str(), SQL_NTS);
    
    if (!SQL_SUCCEEDED(ret)) {
        CheckError(stmt, SQL_HANDLE_STMT, "executing query");
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        return SQL_NULL_HSTMT;
    }
    
    return stmt;
}

SQLHSTMT OdbcHelper::PrepareStatement(const std::string& sql) {
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, connection, &stmt);
    
    if (!SQL_SUCCEEDED(ret)) {
        CheckError(connection, SQL_HANDLE_DBC, "allocating statement handle");
        return SQL_NULL_HSTMT;
    }
    
    ret = SQLPrepare(stmt, (SQLCHAR*)sql.c_str(), SQL_NTS);
    
    if (!SQL_SUCCEEDED(ret)) {
        CheckError(stmt, SQL_HANDLE_STMT, "preparing statement");
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        return SQL_NULL_HSTMT;
    }
    
    return stmt;
}

bool OdbcHelper::BindParameter(SQLHSTMT statement, int paramIndex, const std::string& value) {
    SQLRETURN ret = SQLBindParameter(
        statement,
        paramIndex,
        SQL_PARAM_INPUT,
        SQL_C_CHAR,
        SQL_VARCHAR,
        value.length(),
        0,
        (SQLPOINTER)value.c_str(),
        value.length(),
        nullptr
    );
    
    if (!SQL_SUCCEEDED(ret)) {
        CheckError(statement, SQL_HANDLE_STMT, "binding parameter");
        return false;
    }
    
    return true;
}

bool OdbcHelper::ExecutePreparedStatement(SQLHSTMT statement) {
    SQLRETURN ret = SQLExecute(statement);
    
    if (!SQL_SUCCEEDED(ret)) {
        CheckError(statement, SQL_HANDLE_STMT, "executing prepared statement");
        return false;
    }
    
    return true;
}

bool OdbcHelper::FetchRow(SQLHSTMT statement) {
    SQLRETURN ret = SQLFetch(statement);
    
    if (ret == SQL_NO_DATA) {
        return false;
    }
    
    if (!SQL_SUCCEEDED(ret)) {
        CheckError(statement, SQL_HANDLE_STMT, "fetching row");
        return false;
    }
    
    return true;
}

std::string OdbcHelper::GetColumnData(SQLHSTMT statement, int columnIndex) {
    SQLLEN indicator;
    char dataBuffer[SQL_BUFFER_SIZE];
    
    SQLRETURN ret = SQLGetData(
        statement,
        columnIndex,
        SQL_C_CHAR,
        dataBuffer,
        sizeof(dataBuffer),
        &indicator
    );
    
    if (!SQL_SUCCEEDED(ret)) {
        CheckError(statement, SQL_HANDLE_STMT, "getting column data");
        return "";
    }
    
    if (indicator == SQL_NULL_DATA) {
        return "";
    }
    
    return std::string(dataBuffer, (indicator > sizeof(dataBuffer)) ? sizeof(dataBuffer) : indicator);
}

std::vector<OdbcColumn> OdbcHelper::GetColumns(SQLHSTMT statement) {
    std::vector<OdbcColumn> columns;
    
    SQLSMALLINT columnCount;
    SQLNumResultCols(statement, &columnCount);
    
    for (SQLSMALLINT i = 1; i <= columnCount; i++) {
        SQLCHAR columnName[256];
        SQLSMALLINT columnNameLength;
        SQLSMALLINT dataType;
        SQLULEN columnSize;
        SQLSMALLINT decimalDigits;
        SQLSMALLINT nullable;
        
        SQLRETURN ret = SQLDescribeCol(
            statement,
            i,
            columnName,
            sizeof(columnName),
            &columnNameLength,
            &dataType,
            &columnSize,
            &decimalDigits,
            &nullable
        );
        
        if (SQL_SUCCEEDED(ret)) {
            OdbcColumn column;
            column.name = std::string(reinterpret_cast<char*>(columnName), columnNameLength);
            column.dataType = dataType;
            column.columnSize = columnSize;
            columns.push_back(column);
        }
    }
    
    return columns;
}

std::vector<std::string> OdbcHelper::GetTableList(const std::string& schema) {
    std::vector<std::string> tables;
    
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, connection, &stmt);
    
    if (!SQL_SUCCEEDED(ret)) {
        CheckError(connection, SQL_HANDLE_DBC, "allocating statement handle");
        return tables;
    }
    
    // Get the list of tables
    std::string schemaPattern = schema.empty() ? "%" : schema;
    ret = SQLTables(
        stmt,
        nullptr, 0,                          // Catalog
        (SQLCHAR*)schemaPattern.c_str(), SQL_NTS,  // Schema
        (SQLCHAR*)"%", SQL_NTS,             // Table
        (SQLCHAR*)"TABLE", SQL_NTS          // Table type
    );
    
    if (!SQL_SUCCEEDED(ret)) {
        CheckError(stmt, SQL_HANDLE_STMT, "getting table list");
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        return tables;
    }
    
    // Fetch each table
    while (SQL_SUCCEEDED(SQLFetch(stmt))) {
        SQLLEN indicator;
        char tableNameBuffer[256];
        
        // TABLE_NAME is in the 3rd column
        SQLGetData(stmt, 3, SQL_C_CHAR, tableNameBuffer, sizeof(tableNameBuffer), &indicator);
        
        if (indicator != SQL_NULL_DATA) {
            std::string tableName(tableNameBuffer, indicator);
            tables.push_back(tableName);
        }
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return tables;
}

std::string OdbcHelper::GetPrimaryKeyColumn(const std::string& schema, const std::string& tableName) {
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, connection, &stmt);
    
    if (!SQL_SUCCEEDED(ret)) {
        CheckError(connection, SQL_HANDLE_DBC, "allocating statement handle");
        return "";
    }
    
    // Get primary key information
    ret = SQLPrimaryKeys(
        stmt,
        nullptr, 0,                         // Catalog
        (SQLCHAR*)schema.c_str(), SQL_NTS,  // Schema
        (SQLCHAR*)tableName.c_str(), SQL_NTS  // Table
    );
    
    if (!SQL_SUCCEEDED(ret)) {
        CheckError(stmt, SQL_HANDLE_STMT, "getting primary key info");
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        return "";
    }
    
    std::string pkColumn;
    
    // The column name is in the 4th column
    if (SQL_SUCCEEDED(SQLFetch(stmt))) {
        SQLLEN indicator;
        char columnNameBuffer[256];
        
        SQLGetData(stmt, 4, SQL_C_CHAR, columnNameBuffer, sizeof(columnNameBuffer), &indicator);
        
        if (indicator != SQL_NULL_DATA) {
            pkColumn = std::string(columnNameBuffer, indicator);
        }
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return pkColumn;
}

std::vector<std::vector<std::string>> OdbcHelper::FetchBatch(SQLHSTMT statement, int batchSize) {
    std::vector<std::vector<std::string>> batchData;
    
    // Get column count
    SQLSMALLINT columnCount;
    SQLNumResultCols(statement, &columnCount);
    
    // Fetch rows
    int rowCount = 0;
    while (rowCount < batchSize && FetchRow(statement)) {
        std::vector<std::string> rowData;
        
        for (SQLSMALLINT i = 1; i <= columnCount; i++) {
            std::string value = GetColumnData(statement, i);
            rowData.push_back(value);
        }
        
        batchData.push_back(rowData);
        rowCount++;
    }
    
    return batchData;
}

void OdbcHelper::FreeStatement(SQLHSTMT statement) {
    if (statement != SQL_NULL_HSTMT) {
        SQLFreeHandle(SQL_HANDLE_STMT, statement);
    }
}

std::string OdbcHelper::GetLastError(SQLHANDLE handle, SQLSMALLINT handleType) {
    SQLCHAR sqlState[6];
    SQLINTEGER nativeError;
    SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
    SQLSMALLINT messageLength;
    
    SQLRETURN ret = SQLGetDiagRec(
        handleType,
        handle,
        1,
        sqlState,
        &nativeError,
        message,
        sizeof(message),
        &messageLength
    );
    
    if (SQL_SUCCEEDED(ret)) {
        return std::string(reinterpret_cast<char*>(sqlState)) + ": " + 
               std::string(reinterpret_cast<char*>(message), messageLength);
    }
    
    return "Unknown error";
}

void OdbcHelper::CheckError(SQLHANDLE handle, SQLSMALLINT handleType, const std::string& action) {
    std::string error = GetLastError(handle, handleType);
    logger->Error("ODBC Error when " + action + ": " + error);
}