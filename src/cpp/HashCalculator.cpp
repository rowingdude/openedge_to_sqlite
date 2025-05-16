#include "HashCalculator.h"
#include <sstream>
#include <iomanip>
#include <openssl/evp.h>

std::string HashCalculator::CalculateRowHash(const std::vector<std::string>& rowData) {
    std::stringstream combinedData;
    
    for (const auto& field : rowData) {
        combinedData << field.length() << ":" << field << "|";
    }
    
    return Sha256(combinedData.str());
}

std::string HashCalculator::Sha256(const std::string& input) {
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hashLen;
    
    EVP_MD_CTX* context = EVP_MD_CTX_new();
    
    if (context != nullptr) {
        if (EVP_DigestInit_ex(context, EVP_sha256(), nullptr)) {
            if (EVP_DigestUpdate(context, input.c_str(), input.length())) {
                if (EVP_DigestFinal_ex(context, hash, &hashLen)) {
                    EVP_MD_CTX_free(context);
                    
                    std::stringstream ss;
                    for (unsigned int i = 0; i < hashLen; i++) {
                        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
                    }
                    
                    return ss.str();
                }
            }
        }
        
        EVP_MD_CTX_free(context);
    }
    
    // Fallback if EVP API fails
    return "";
}