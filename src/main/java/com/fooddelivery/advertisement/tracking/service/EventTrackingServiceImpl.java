package com.fooddelivery.advertisement.tracking.service;

import com.fooddelivery.advertisement.tracking.enums.AdTrackingType;
import com.fooddelivery.advertisement.tracking.kafka.TrackingEventProducer;
import com.fooddelivery.advertisement.tracking.util.CryptoService;
import com.fooddelivery.common.constants.EventPayloadConstants;
import com.fooddelivery.common.enums.ChargeCategory;
import org.springframework.stereotype.Service;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.time.Duration;

@Service
public class EventTrackingServiceImpl implements ImpressionTracker, ClickTracker, ConversionTracker {
    
    private final CryptoService cryptoService;
    private final TrackingEventProducer producer;
    private final FraudPreventionService fraudPreventionService;
    private final org.springframework.data.redis.core.StringRedisTemplate redisTemplate;
    
    public EventTrackingServiceImpl(CryptoService cryptoService, TrackingEventProducer producer, FraudPreventionService fraudPreventionService, org.springframework.data.redis.core.StringRedisTemplate redisTemplate) {
        this.cryptoService = cryptoService;
        this.producer = producer;
        this.fraudPreventionService = fraudPreventionService;
        this.redisTemplate = redisTemplate;
    }

    private static final String INCREMENT_AND_EXPIRE_SCRIPT = 
            "local current = redis.call('INCRBY', KEYS[1], ARGV[1]); " +
            "if tonumber(current) == tonumber(ARGV[1]) then " +
            "  redis.call('EXPIRE', KEYS[1], ARGV[2]); " +
            "end; " +
            "return current;";

    private final org.springframework.data.redis.core.script.RedisScript<Long> incrExpireScript = 
            new org.springframework.data.redis.core.script.DefaultRedisScript<>(INCREMENT_AND_EXPIRE_SCRIPT, Long.class);

    private void incrementDailySpend(UUID campaignId, BigDecimal amount) {
        String spendKey = "campaign:spend:daily:" + campaignId.toString();
        long micros = amount.multiply(new BigDecimal("10000")).longValue();
        redisTemplate.execute(incrExpireScript, java.util.Collections.singletonList(spendKey), String.valueOf(micros), "86400"); // 24h
    }

    private void incrementLifetimeSpend(UUID campaignId, BigDecimal amount) {
        String spendKey = "campaign:spend:lifetime:" + campaignId.toString();
        long micros = amount.multiply(new BigDecimal("10000")).longValue();
        redisTemplate.execute(incrExpireScript, java.util.Collections.singletonList(spendKey), String.valueOf(micros), "7776000"); // 90 days
    }

    @Override
    public void recordImpression(UUID campaignId, UUID advertiserId, String encryptedPrice, String deviceId, String ipAddress) {
        recordEvent(campaignId, advertiserId, encryptedPrice, deviceId, ipAddress, AdTrackingType.IMPRESSION, ChargeCategory.AD_IMPRESSION, 5, "imp");
        
        String identifier = deviceId != null ? deviceId : ipAddress;
        if (identifier != null && !identifier.isBlank() && !"unknown".equals(identifier)) {
            // Increment Frequency Cap
            String capKey = "ad:cap:" + identifier + ":" + campaignId;
            redisTemplate.execute(incrExpireScript, java.util.Collections.singletonList(capKey), "1", "86400"); // 24h limit
            
            // Store Interaction marker for Attribution
            String interactionKey = "ad:interaction:" + identifier + ":" + campaignId;
            redisTemplate.opsForValue().set(interactionKey, "1", Duration.ofDays(7));
        }
    }

    @Override
    public void recordClick(UUID campaignId, UUID advertiserId, String encryptedPrice, String deviceId, String ipAddress) {
        recordEvent(campaignId, advertiserId, encryptedPrice, deviceId, ipAddress, AdTrackingType.CLICK, ChargeCategory.AD_CLICK, 5, "click");
        
        String identifier = deviceId != null ? deviceId : ipAddress;
        if (identifier != null && !identifier.isBlank() && !"unknown".equals(identifier)) {
            // Store Interaction marker for Attribution
            String interactionKey = "ad:interaction:" + identifier + ":" + campaignId;
            redisTemplate.opsForValue().set(interactionKey, "1", Duration.ofDays(7));
        }
    }

    @Override
    public void recordConversion(UUID campaignId, UUID advertiserId, String encryptedPrice, String deviceId, String ipAddress) {
        String identifier = deviceId != null ? deviceId : ipAddress;
        if (identifier != null && !identifier.isBlank() && !"unknown".equals(identifier)) {
            String interactionKey = "ad:interaction:" + identifier + ":" + campaignId;
            Boolean hasInteracted = redisTemplate.hasKey(interactionKey);
            if (!Boolean.TRUE.equals(hasInteracted)) {
                throw new IllegalArgumentException("Conversion not attributed to recent impression/click");
            }
        }
        
        recordEvent(campaignId, advertiserId, encryptedPrice, deviceId, ipAddress, AdTrackingType.CONVERSION, ChargeCategory.AD_CONVERSION, 30, "conv");
    }

    private void recordEvent(UUID campaignId, UUID advertiserId, String encryptedPrice, String deviceId, String ipAddress, AdTrackingType trackingType, ChargeCategory chargeCategory, int idemExpiryMinutes, String idemPrefix) {
        String identifier = deviceId != null ? deviceId : ipAddress;
        if (!fraudPreventionService.isAllowed(campaignId.toString(), identifier)) {
            throw new IllegalArgumentException("Rate limit exceeded for this device/IP");
        }

        String idemKey = "idem:" + idemPrefix + ":" + campaignId + ":" + identifier;
        Boolean isNew = redisTemplate.opsForValue().setIfAbsent(idemKey, "1", Duration.ofMinutes(idemExpiryMinutes));
        if (Boolean.FALSE.equals(isNew)) {
            // Already tracked this event for this device on this campaign within the deduplication window.
            return;
        }

        // 1. Decrypt Price (Fail fast if invalid)
        BigDecimal price = cryptoService.decryptAuctionPrice(encryptedPrice, campaignId, advertiserId, idemPrefix);

        // 2. Publish Billing Event for Wallet Service (Only for Impressions)
        long timeBucket = System.currentTimeMillis() / 10_000;
        String eventId = UUID.nameUUIDFromBytes(
            (idemPrefix + ":" + campaignId + ":" + identifier + ":" + timeBucket).getBytes(StandardCharsets.UTF_8)
        ).toString();

        Map<String, Object> baseEvent = new HashMap<>();
        baseEvent.put(EventPayloadConstants.EVENT_ID, eventId);
        baseEvent.put(EventPayloadConstants.CAMPAIGN_ID, campaignId.toString());
        baseEvent.put(EventPayloadConstants.ADVERTISER_ID, advertiserId.toString());
        baseEvent.put(EventPayloadConstants.AMOUNT, price.toPlainString());
        baseEvent.put(EventPayloadConstants.CHARGE_CATEGORY, chargeCategory.name());
        baseEvent.put(EventPayloadConstants.TIMESTAMP, System.currentTimeMillis());

        if (AdTrackingType.IMPRESSION == trackingType) {
            producer.publishBillingEvent(eventId, baseEvent);
            // 4. Update Spend for Pacing
            incrementDailySpend(campaignId, price);
            incrementLifetimeSpend(campaignId, price);
        }

        // 3. Publish generic tracking event for Analytics (Clickhouse)
        Map<String, Object> trackingEvent = new HashMap<>(baseEvent);
        trackingEvent.put(EventPayloadConstants.EVENT_TYPE, trackingType.name());
        trackingEvent.put(EventPayloadConstants.DEVICE_ID, deviceId);
        producer.publishTrackingEvent(eventId, trackingEvent);
    }
}
