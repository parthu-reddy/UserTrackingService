package com.fooddelivery.advertisement.tracking.util;

import org.springframework.stereotype.Component;
import java.math.BigDecimal;
import java.util.Base64;

@Component
public class CryptoService {
    // In a real system, this would use Google DoubleClick Crypto (AES) or similar
    // For this prototype, we'll assume the string is simply Base64 encoded for demonstration
    public BigDecimal decryptAuctionPrice(String encryptedPrice) {
        if (encryptedPrice == null || encryptedPrice.isEmpty() || encryptedPrice.equals(com.fooddelivery.common.constants.AdMacroConstants.MACRO_AUCTION_PRICE)) {
            throw new IllegalArgumentException("Invalid encrypted price macro");
        }
        try {
            String decoded = new String(Base64.getDecoder().decode(encryptedPrice));
            BigDecimal price = new BigDecimal(decoded);
            if (price.compareTo(BigDecimal.ZERO) < 0) {
                 throw new IllegalStateException("Decrypted price cannot be negative");
            }
            return price;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to decrypt auction price: " + e.getMessage());
        }
    }
}
