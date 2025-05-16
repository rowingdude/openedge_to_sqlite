#include "Config.h"

Config::Config(const std::string& configFile) {
    LoadConfig(configFile);
}

void Config::LoadConfig(const std::string& configFile) {
    std::ifstream file(configFile);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open config file: " + configFile);
    }

    nlohmann::json config;
    file >> config;

    progressDb.host = config["progress_db"]["host"];
    progressDb.port = config["progress_db"]["port"];
    progressDb.dbName = config["progress_db"]["db_name"];
    progressDb.user = config["progress_db"]["user"];
    progressDb.password = config["progress_db"]["password"];
    progressDb.driverClass = config["progress_db"]["driver_class"];
    progressDb.jarFile = config["progress_db"]["jar_file"];

    sqliteDb.dbPath = config["sqlite_db"]["db_path"];
    
    if (config.contains("hash_db")) {
        hashDb.dbPath = config["hash_db"]["db_path"];
        hashDb.enableHashing = config["hash_db"]["enable_hashing"];
    } else {
        hashDb.dbPath = "hashes.db";
        hashDb.enableHashing = false;
    }

    mirrorSettings.batchSize = config["mirror_settings"]["batch_size"];
    mirrorSettings.logFile = config["mirror_settings"]["log_file"];
    mirrorSettings.ignoreFile = config["mirror_settings"]["ignore_file"];
}