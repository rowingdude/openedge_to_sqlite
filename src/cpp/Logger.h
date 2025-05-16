#ifndef LOGGER_H
#define LOGGER_H

#include <string>
#include <fstream>
#include <iostream>
#include <mutex>
#include <ctime>
#include <iomanip>
#include <sstream>

enum class LogLevel {
    INFO,
    WARNING,
    ERROR
};

class Logger {
public:
    Logger(const std::string& logFile);
    ~Logger();

    void Log(LogLevel level, const std::string& message);
    void Info(const std::string& message);
    void Warning(const std::string& message);
    void Error(const std::string& message);

private:
    std::ofstream fileStream;
    std::mutex logMutex;
    std::string GetCurrentTime();
    std::string LevelToString(LogLevel level);
};

#endif // LOGGER_H