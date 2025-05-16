#ifndef DATA_SYNC_MANAGER_H
#define DATA_SYNC_MANAGER_H

#include <string>
#include <vector>
#include <set>
#include <memory>
#include <time.h>
#include "Config.h"
#include "Logger.h"
#include "SyncState.h"
#include "HashStorage.h"
#include "DatabaseConnector.h"
#include "SqliteHelper.h"
#include "OdbcHelper.h"
#include "TableSyncer.h"

class DataSyncManager {
public:
    DataSyncManager(const std::string& configFile, bool fullSync = false, const std::vector<std::string>& ignoreTables = {});
    ~DataSyncManager();
    
    void RunSync();

private:
    std::string configFile;
    Config config;
    bool fullSync;
    std::shared_ptr<Logger> logger;
    std::set<std::string> ignoredTables;
    
    std::unique_ptr<DatabaseConnector> dbConnector;
    std::unique_ptr<SqliteHelper> sqliteHelper;
    std::unique_ptr<OdbcHelper> odbcHelper;
    std::shared_ptr<SyncState> syncState;
    std::shared_ptr<HashStorage> hashDb;
    std::unique_ptr<TableSyncer> tableSyncer;
    
    struct {
        int tablesProcessed;
        int rowsSynced;
        time_t startTime;
    } metrics;
    
    void LoadIgnoreList();
    void AddToIgnoreList(const std::vector<std::string>& tables);
    std::vector<TableInfo> GetSourceTables();
};

#endif