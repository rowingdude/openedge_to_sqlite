#ifndef HASH_CALCULATOR_H
#define HASH_CALCULATOR_H

#include <string>
#include <vector>

class HashCalculator {
public:
    static std::string CalculateRowHash(const std::vector<std::string>& rowData);
    
private:
    static std::string Sha256(const std::string& input);
};

#endif //