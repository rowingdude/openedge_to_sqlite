#include <iostream>
#include <string>
#include <vector>
#include "DataSyncManager.h"

void PrintUsage(const char* programName) {
    std::cout << "Usage: " << programName << " [options]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --full-sync            Perform full sync of all tables" << std::endl;
    std::cout << "  --ignore-table TABLE   Tables to ignore (can be used multiple times)" << std::endl;
    std::cout << "  --config FILE          Path to configuration file (default: config.json)" << std::endl;
    std::cout << "  --help                 Display this help message" << std::endl;
}

int main(int argc, char* argv[]) {
    bool fullSync = false;
    std::vector<std::string> ignoreTables;
    std::string configFile = "config.json";
    
    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        
        if (arg == "--full-sync") {
            fullSync = true;
        } else if (arg == "--ignore-table" && i + 1 < argc) {
            ignoreTables.push_back(argv[++i]);
        } else if (arg == "--config" && i + 1 < argc) {
            configFile = argv[++i];
        } else if (arg == "--help") {
            PrintUsage(argv[0]);
            return 0;
        } else {
            std::cerr << "Unknown option: " << arg << std::endl;
            PrintUsage(argv[0]);
            return 1;
        }
    }
    
    try {
        // Create DataSyncManager with the specified config file
        DataSyncManager syncer(configFile, fullSync, ignoreTables);
        syncer.RunSync();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}