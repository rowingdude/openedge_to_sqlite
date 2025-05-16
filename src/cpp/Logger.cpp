#include "Logger.h"

Logger::Logger(const std::string& logFile) {
    fileStream.open(logFile, std::ios::out | std::ios::app);
    if (!fileStream.is_open()) {
        std::cerr << "Failed to open log file: " << logFile << std::endl;
    }
}

Logger::~Logger() {
    if (fileStream.is_open()) {
        fileStream.close();
    }
}

void Logger::Log(LogLevel level, const std::string& message) {
    std::string timestamp = GetCurrentTime();
    std::string levelStr = LevelToString(level);
    
    std::string formattedMessage = timestamp + " - " + levelStr + " - " + message;
    
    std::lock_guard<std::mutex> lock(logMutex);
    
    // Write to file
    if (fileStream.is_open()) {
        fileStream << formattedMessage << std::endl;
        fileStream.flush();
    }
    
    // Also write to console
    std::cout << formattedMessage << std::endl;
}

void Logger::Info(const std::string& message) {
    Log(LogLevel::INFO, message);
}

void Logger::Warning(const std::string& message) {
    Log(LogLevel::WARNING, message);
}

void Logger::Error(const std::string& message) {
    Log(LogLevel::ERROR, message);
}

std::string Logger::GetCurrentTime() {
    auto now = std::time(nullptr);
    auto tm = *std::localtime(&now);
    
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

std::string Logger::LevelToString(LogLevel level) {
    switch (level) {
        case LogLevel::INFO:
            return "INFO";
        case LogLevel::WARNING:
            return "WARNING";
        case LogLevel::ERROR:
            return "ERROR";
        default:
            return "UNKNOWN";
    }
}