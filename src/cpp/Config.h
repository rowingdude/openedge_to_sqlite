#ifndef CONFIG_H
#define CONFIG_H

#include <string>
#include <vector>
#include <nlohmann/json.hpp>
#include <fstream>
#include <stdexcept>

class Config {
public:
    struct DatabaseConfig {
        std::string host;
        int port;
        std::string dbName;
        std::string user;
        std::string password;
        std::string driverClass;
        std::string jarFile;
        std::string dsn; 
    };

    struct SQLiteConfig {
        std::string dbPath;
    };

    struct HashDbConfig {
        std::string dbPath;
        bool enableHashing;
    };

    struct MirrorSettings {
        int batchSize;
        std::string logFile;
        std::string ignoreFile;
    };

    Config(const std::string& configFile = "config.json");

    DatabaseConfig progressDb;
    SQLiteConfig sqliteDb;
    HashDbConfig hashDb;
    MirrorSettings mirrorSettings;

private:
    void LoadConfig(const std::string& configFile);
};

#endif