#ifndef TABLE_SYNCER_H
#define TABLE_SYNCER_H

#include <string>
#include <vector>
#include <memory>
#include "SqliteHelper.h"
#include "OdbcHelper.h"
#include "HashStorage.h"
#include "SyncState.h"
#include "Logger.h"
#include "TableInfo.h"

class TableSyncer {
public:
    TableSyncer(SqliteHelper& sqliteHelper, 
               OdbcHelper& odbcHelper,
               std::shared_ptr<SyncState> syncState,
               std::shared_ptr<HashStorage> hashDb,
               std::shared_ptr<Logger> logger,
               int batchSize);
               
    int SyncTable(const TableInfo& tableInfo, bool fullSync);
    
private:
    SqliteHelper& sqliteHelper;
    OdbcHelper& odbcHelper;
    std::shared_ptr<SyncState> syncState;
    std::shared_ptr<HashStorage> hashDb;
    std::shared_ptr<Logger> logger;
    int batchSize;
    bool hashEnabled;
    
    // Sync strategies
    std::string GetSyncStrategy(const TableInfo& tableInfo, bool fullSync);
    int SyncFullTable(const TableInfo& tableInfo);
    int SyncKeyBased(const TableInfo& tableInfo);
    int SyncTimestampBased(const TableInfo& tableInfo);
    int SyncHashBased(const TableInfo& tableInfo);
    
    // Batch processing
    void ProcessHashBasedBatch(
        const std::string& tableName,
        const std::vector<std::string>& columns,
        const std::string& pkColumn,
        const std::vector<std::string>& pkValues,
        const std::vector<std::vector<std::string>>& batchData);
        
    void ProcessKeyBasedBatch(
        const std::string& tableName, 
        const std::vector<std::string>& columns, 
        const std::string& pkColumn,
        const std::vector<std::string>& pkValues, 
        const std::vector<std::vector<std::string>>& batchData);
        
    // Table management
    bool EnsureTargetTable(const TableInfo& tableInfo);
    int GetSourceRowCount(const std::string& tableName);
    
    // Helper methods
    std::string FindTimestampColumn(const std::vector<std::string>& columns);
};

#endif // TABLE_SYNCER_H