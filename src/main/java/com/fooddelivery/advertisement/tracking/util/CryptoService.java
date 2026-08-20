package com.fooddelivery.advertisement.tracking.util;

import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import java.math.BigDecimal;
import java.util.UUID;
import com.fooddelivery.common.security.AuctionTokenService;

@Component
@lombok.extern.slf4j.Slf4j
public class CryptoService {
    @java.lang.SuppressWarnings("all")

    private final AuctionTokenService auctionTokenService;
    private final StringRedisTemplate redisTemplate;
    
    @Value("${tracking.accept-legacy-price:false}")
    private boolean acceptLegacyPrice;

    public CryptoService(AuctionTokenService auctionTokenService, StringRedisTemplate redisTemplate) {
        this.auctionTokenService = auctionTokenService;
        this.redisTemplate = redisTemplate;
    }

    public BigDecimal decryptAuctionPrice(String encryptedPrice, UUID expectedCampaignId, UUID expectedAdvertiserId) {
        if (encryptedPrice == null || encryptedPrice.isEmpty() || encryptedPrice.equals(com.fooddelivery.common.constants.AdMacroConstants.MACRO_AUCTION_PRICE)) {
            throw new IllegalArgumentException("Invalid encrypted price macro");
        }
        return verifyAndCheckReplay(encryptedPrice, expectedCampaignId, expectedAdvertiserId, "default");
    }

    public BigDecimal decryptAuctionPrice(String encryptedPrice, UUID expectedCampaignId, UUID expectedAdvertiserId, String eventContext) {
        if (encryptedPrice == null || encryptedPrice.isEmpty() || encryptedPrice.equals(com.fooddelivery.common.constants.AdMacroConstants.MACRO_AUCTION_PRICE)) {
            throw new IllegalArgumentException("Invalid encrypted price macro");
        }
        return verifyAndCheckReplay(encryptedPrice, expectedCampaignId, expectedAdvertiserId, eventContext);
    }

    private BigDecimal verifyAndCheckReplay(String encryptedPrice, UUID expectedCampaignId, UUID expectedAdvertiserId, String eventContext) {
        try {
            AuctionTokenService.AuctionToken token = auctionTokenService.verify(encryptedPrice);
            
            if (!token.campaignId().equals(expectedCampaignId)) {
                throw new IllegalStateException("Token campaign mismatch");
            }
            if (!token.advertiserId().equals(expectedAdvertiserId)) {
                throw new IllegalStateException("Token advertiser mismatch");
            }
            
            BigDecimal price = token.getPriceAsBigDecimal();
            if (price.compareTo(BigDecimal.ZERO) < 0) {
                throw new IllegalStateException("Decrypted price cannot be negative");
            }
            
            String replayKey = "token:replay:" + eventContext + ":" + token.auctionId();
            long ttlMillis = token.expiry() - System.currentTimeMillis();
            if (ttlMillis > 0) {
                Boolean isNew = redisTemplate.opsForValue().setIfAbsent(replayKey, "1", java.time.Duration.ofMillis(ttlMillis));
                if (Boolean.FALSE.equals(isNew)) {
                    throw new IllegalArgumentException("Token replayed for auction " + token.auctionId());
                }
            } else {
                throw new IllegalArgumentException("Token expired");
            }
            
            return price;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to decrypt auction price: " + e.getMessage());
        }
    }
}
