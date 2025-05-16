#ifndef TABLE_INFO_H
#define TABLE_INFO_H

#include <string>
#include <vector>

struct TableInfo {
    std::string tableName;
    std::vector<std::string> columns;
    std::string pkColumn;
};

#endif