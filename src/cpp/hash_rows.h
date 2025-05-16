#ifndef HASH_ROWS_H
#define HASH_ROWS_H

#include <sqlite3.h>
#include <string>
#include <memory>
#include <vector>
#include "logger.h"

class hash_rows {
public:
    hash_rows(const std::string& db_path, std::shared_ptr<Logger> logger);
    ~hash_rows();

    bool initialize();
    bool store_hash(const std::string& table_name, const std::string& pk_value, const std::string& row_hash);
    std::string get_hash(const std::string& table_name, const std::string& pk_value);
    bool delete_hash(const std::string& table_name, const std::string& pk_value);
    bool delete_table_hashes(const std::string& table_name);
    std::vector<std::string> get_changed_rows(const std::string& table_name, 
                                             const std::vector<std::string>& pk_values,
                                             const std::vector<std::string>& row_hashes);

private:
    std::string db_path;
    sqlite3* db_conn;
    std::shared_ptr<Logger> logger;

    bool ensure_hash_table();
};

#endif // HASH_ROWS_H