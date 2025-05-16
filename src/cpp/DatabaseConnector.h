#ifndef DATABASE_CONNECTOR_H
#define DATABASE_CONNECTOR_H

#include <string>
#include <memory>
#include <sqlite3.h>
#include <sql.h>
#include <sqlext.h>
#include "Config.h"
#include "Logger.h"

class DatabaseConnector {
public:
    DatabaseConnector(const Config& config, std::shared_ptr<Logger> logger);
    ~DatabaseConnector();

    bool Connect();
    void Disconnect();

    // Getters for database handles
    SQLHDBC GetOdbcConnection() const { return odbcConn; }
    SQLHENV GetOdbcEnvironment() const { return odbcEnv; }
    sqlite3* GetSqliteConnection() const { return sqliteConn; }

private:
    const Config& config;
    std::shared_ptr<Logger> logger;
    
    // ODBC handles
    SQLHENV odbcEnv;
    SQLHDBC odbcConn;
    
    // SQLite handle
    sqlite3* sqliteConn;
    
    void CheckOdbcError(SQLHANDLE handle, SQLSMALLINT type, const std::string& action);
    std::string GetOdbcError(SQLHANDLE handle, SQLSMALLINT handleType);
};

#endif