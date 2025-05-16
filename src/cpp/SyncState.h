#ifndef SYNC_STATE_H
#define SYNC_STATE_H

#include <sqlite3.h>
#include <string>
#include <memory>
#include "Logger.h"

class SyncState {
public:
    SyncState(sqlite3* sqliteConn, std::shared_ptr<Logger> logger);
    
    struct SyncData {
        std::string lastSyncTime;
        std::string lastKeyValue;
        std::string syncMethod;
        int rowCount;
    };
    
    SyncData GetLastSync(const std::string& tableName);
    void UpdateSyncState(const std::string& tableName, 
                        const std::string& lastKeyValue = "", 
                        const std::string& syncMethod = "timestamp", 
                        int rowCount = 0);

private:
    sqlite3* sqliteConn;
    std::shared_ptr<Logger> logger;
    
    void EnsureStateTable();
};

#endif