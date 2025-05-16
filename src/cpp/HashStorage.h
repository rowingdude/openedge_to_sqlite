#ifndef HASH_STORAGE_H
#define HASH_STORAGE_H

#include <sqlite3.h>
#include <string>
#include <memory>
#include <vector>
#include "Logger.h"

class HashStorage {
public:
    HashStorage(const std::string& dbPath, std::shared_ptr<Logger> logger);
    ~HashStorage();

    bool Initialize();
    bool StoreHash(const std::string& tableName, const std::string& pkValue, const std::string& rowHash);
    std::string GetHash(const std::string& tableName, const std::string& pkValue);
    bool DeleteHash(const std::string& tableName, const std::string& pkValue);
    bool DeleteTableHashes(const std::string& tableName);
    std::vector<std::string> GetChangedRows(
        const std::string& tableName, 
        const std::vector<std::string>& pkValues,
        const std::vector<std::string>& rowHashes);

private:
    std::string dbPath;
    sqlite3* dbConn;
    std::shared_ptr<Logger> logger;

    bool EnsureHashTable();
};

#endif