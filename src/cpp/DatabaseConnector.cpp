#include "DatabaseConnector.h"
#include <stdexcept>

DatabaseConnector::DatabaseConnector(const Config& config, std::shared_ptr<Logger> logger)
    : config(config), logger(logger), odbcEnv(SQL_NULL_HENV), odbcConn(SQL_NULL_HDBC), sqliteConn(nullptr) {
}

DatabaseConnector::~DatabaseConnector() {
    Disconnect();
}

bool DatabaseConnector::Connect() {
    try {
        // Initialize ODBC environment
        SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &odbcEnv);
        if (!SQL_SUCCEEDED(ret)) {
            logger->Error("Failed to allocate ODBC environment handle");
            return false;
        }
        
        ret = SQLSetEnvAttr(odbcEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
        if (!SQL_SUCCEEDED(ret)) {
            CheckOdbcError(odbcEnv, SQL_HANDLE_ENV, "setting environment attributes");
            return false;
        }
        
        // Initialize ODBC connection
        ret = SQLAllocHandle(SQL_HANDLE_DBC, odbcEnv, &odbcConn);
        if (!SQL_SUCCEEDED(ret)) {
            CheckOdbcError(odbcEnv, SQL_HANDLE_ENV, "allocating connection handle");
            return false;
        }

        // Try several different connection string formats
        std::vector<std::string> connectionStrings;
        
        // Standard Progress DataDirect OpenEdge connection string
        connectionStrings.push_back(
            "DRIVER={Progress OpenEdge Wire Protocol};"
            "HOST=" + config.progressDb.host + ";"
            "PORT=" + std::to_string(config.progressDb.port) + ";"
            "DB=" + config.progressDb.dbName + ";"
            "UID=" + config.progressDb.user + ";"
            "PWD=" + config.progressDb.password + ";"
        );

        // Alternative format sometimes used with Progress DataDirect
        connectionStrings.push_back(
            "DRIVER={Progress OpenEdge Wire Protocol};"
            "HostName=" + config.progressDb.host + ";"
            "PortNumber=" + std::to_string(config.progressDb.port) + ";"
            "Database=" + config.progressDb.dbName + ";"
            "User=" + config.progressDb.user + ";"
            "Password=" + config.progressDb.password + ";"
        );

        // Yet another alternative format that might work
        connectionStrings.push_back(
            "DRIVER={Progress OpenEdge Wire Protocol};"
            "ServerName=" + config.progressDb.host + ";"
            "PortNumber=" + std::to_string(config.progressDb.port) + ";"
            "DatabaseName=" + config.progressDb.dbName + ";"
            "LogonID=" + config.progressDb.user + ";"
            "Password=" + config.progressDb.password + ";"
        );
        
        // If DSN is provided, try that too
        if (!config.progressDb.dsn.empty()) {
            connectionStrings.push_back(
                "DSN=" + config.progressDb.dsn + ";"
                "UID=" + config.progressDb.user + ";"
                "PWD=" + config.progressDb.password + ";"
            );
        }
        
        // Try each connection string until one works
        bool connected = false;
        std::string lastError;
        
        for (const auto& connStr : connectionStrings) {
            logger->Info("Attempting connection with: " + connStr.substr(0, connStr.find("PWD=")) + "PWD=********;");
            
            SQLCHAR connStrOut[1024];
            SQLSMALLINT connStrOutLen;
            
            ret = SQLDriverConnect(
                odbcConn, 
                NULL, 
                (SQLCHAR*)connStr.c_str(), 
                SQL_NTS, 
                connStrOut, 
                sizeof(connStrOut), 
                &connStrOutLen, 
                SQL_DRIVER_NOPROMPT
            );
            
            if (SQL_SUCCEEDED(ret)) {
                connected = true;
                logger->Info("Connected to OpenEdge database");
                break;
            } else {
                std::string error = GetOdbcError(odbcConn, SQL_HANDLE_DBC);
                lastError = error;
                logger->Warning("Connection attempt failed: " + error);
            }
        }
        
        if (!connected) {
            logger->Error("All connection attempts failed. Last error: " + lastError);
            SQLFreeHandle(SQL_HANDLE_DBC, odbcConn);
            odbcConn = SQL_NULL_HDBC;
            return false;
        }
        
        // Initialize SQLite connection
        int rc = sqlite3_open(config.sqliteDb.dbPath.c_str(), &sqliteConn);
        if (rc != SQLITE_OK) {
            logger->Error(std::string("Failed to connect to SQLite: ") + sqlite3_errmsg(sqliteConn));
            Disconnect();  // Changed from DisconnectDatabases()
            return false;
        }
        
        sqlite3_exec(sqliteConn, "PRAGMA foreign_keys = ON", nullptr, nullptr, nullptr);
        logger->Info("Connected to SQLite database at " + config.sqliteDb.dbPath);
        
        return true;
    } catch (const std::exception& e) {
        logger->Error(std::string("Error connecting to databases: ") + e.what());
        Disconnect();  // Changed from DisconnectDatabases()
        return false;
    }
}

std::string DatabaseConnector::GetOdbcError(SQLHANDLE handle, SQLSMALLINT handleType) {
    SQLCHAR sqlState[6];
    SQLINTEGER nativeError;
    SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
    SQLSMALLINT messageLen;
    
    SQLRETURN ret = SQLGetDiagRec(handleType, handle, 1, sqlState, &nativeError, 
                                message, sizeof(message), &messageLen);
    
    if (SQL_SUCCEEDED(ret)) {
        return std::string(reinterpret_cast<char*>(sqlState)) + ": " + 
               std::string(reinterpret_cast<char*>(message), messageLen);
    }
    
    return "Unknown ODBC error";
}

void DatabaseConnector::Disconnect() {
    if (odbcConn != SQL_NULL_HDBC) {
        SQLDisconnect(odbcConn);
        SQLFreeHandle(SQL_HANDLE_DBC, odbcConn);
        odbcConn = SQL_NULL_HDBC;
        logger->Info("Closed OpenEdge connection");
    }
    
    if (odbcEnv != SQL_NULL_HENV) {
        SQLFreeHandle(SQL_HANDLE_ENV, odbcEnv);
        odbcEnv = SQL_NULL_HENV;
    }
    
    if (sqliteConn) {
        sqlite3_close(sqliteConn);
        sqliteConn = nullptr;
        logger->Info("Closed SQLite connection");
    }
}

void DatabaseConnector::CheckOdbcError(SQLHANDLE handle, SQLSMALLINT type, const std::string& action) {
    SQLCHAR sqlState[6];
    SQLINTEGER nativeError;
    SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
    SQLSMALLINT messageLen;
    
    SQLRETURN ret = SQLGetDiagRec(type, handle, 1, sqlState, &nativeError, 
                                message, sizeof(message), &messageLen);
    
    if (SQL_SUCCEEDED(ret)) {
        std::string errorMsg = "ODBC Error when " + action + ": [" + 
                             std::string(reinterpret_cast<char*>(sqlState)) + "] " + 
                             std::string(reinterpret_cast<char*>(message));
        logger->Error(errorMsg);
    } else {
        logger->Error("Unknown ODBC error when " + action);
    }
}