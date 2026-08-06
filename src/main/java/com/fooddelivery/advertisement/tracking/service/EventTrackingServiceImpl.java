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

    private String getCampaignBid(UUID campaignId) {
        String key = String.format(com.fooddelivery.common.constants.RedisKeyConstants.PREFIX_AD_CAMPAIGN_MAX_BID, campaignId.toString());
        String bidStr = redisTemplate.opsForValue().get(key);
        if (bidStr == null) {
            throw new IllegalStateException("Cannot process billing: Campaign bid price not found in cache for campaign " + campaignId);
        }
        return bidStr;
    }

    @Override
    public void recordImpression(UUID campaignId, UUID advertiserId, String encryptedPrice, String deviceId, String ipAddress) {
        String identifier = deviceId != null ? deviceId : ipAddress;
        if (!fraudPreventionService.isAllowed(campaignId.toString(), identifier)) {
            throw new IllegalArgumentException("Rate limit exceeded for this device/IP");
        }

        String idemKey = "idem:imp:" + campaignId + ":" + identifier;
        Boolean isNew = redisTemplate.opsForValue().setIfAbsent(idemKey, "1", Duration.ofMinutes(5));
        if (Boolean.FALSE.equals(isNew)) {
            // Already tracked an impression for this device on this campaign within 5 minutes.
            return;
        }
        
        // 1. Decrypt Price (Fail fast if invalid)
        BigDecimal price = cryptoService.decryptAuctionPrice(encryptedPrice);
        
        // 2. Publish Billing Event for Wallet Service
        // Deterministic eventId to deduplicate browser retries (same campaign+device within 10s bucket)
        long timeBucket = System.currentTimeMillis() / 10_000; // 10-second dedup window
        String eventId = UUID.nameUUIDFromBytes(
            ("imp:" + campaignId + ":" + identifier + ":" + timeBucket).getBytes(StandardCharsets.UTF_8)
        ).toString();
        Map<String, Object> billingEvent = new HashMap<>();
        billingEvent.put(EventPayloadConstants.EVENT_ID, eventId);
        billingEvent.put(EventPayloadConstants.CAMPAIGN_ID, campaignId.toString());
        billingEvent.put(EventPayloadConstants.ADVERTISER_ID, advertiserId.toString());
        billingEvent.put(EventPayloadConstants.AMOUNT, price.toPlainString()); // Strict BigDecimal to string
        billingEvent.put(EventPayloadConstants.CHARGE_CATEGORY, ChargeCategory.AD_IMPRESSION.name());
        billingEvent.put(EventPayloadConstants.TIMESTAMP, System.currentTimeMillis());
        producer.publishBillingEvent(eventId, billingEvent);
        
        // 3. Publish generic tracking event for Analytics (Clickhouse)
        Map<String, Object> trackingEvent = new HashMap<>(billingEvent);
        trackingEvent.put(EventPayloadConstants.EVENT_TYPE, AdTrackingType.IMPRESSION.name());
        trackingEvent.put(EventPayloadConstants.DEVICE_ID, deviceId);
        producer.publishTrackingEvent(eventId, trackingEvent);
    }

    @Override
    public void recordClick(UUID campaignId, UUID advertiserId, String deviceId, String ipAddress) {
        String identifier = deviceId != null ? deviceId : ipAddress;
        if (!fraudPreventionService.isAllowed(campaignId.toString(), identifier)) {
            throw new IllegalArgumentException("Rate limit exceeded for this device/IP");
        }
        
        String idemKey = "idem:click:" + campaignId + ":" + identifier;
        Boolean isNew = redisTemplate.opsForValue().setIfAbsent(idemKey, "1", Duration.ofMinutes(5));
        if (Boolean.FALSE.equals(isNew)) {
            // Already tracked a click for this device on this campaign within 5 minutes.
            return;
        }
        
        String bidStr = getCampaignBid(campaignId);
        
        long timeBucket = System.currentTimeMillis() / 10_000;
        String eventId = UUID.nameUUIDFromBytes(
            ("click:" + campaignId + ":" + identifier + ":" + timeBucket).getBytes(StandardCharsets.UTF_8)
        ).toString();
        
        // 1. Publish Billing Event (CPC)
        Map<String, Object> billingEvent = new HashMap<>();
        billingEvent.put(EventPayloadConstants.EVENT_ID, eventId);
        billingEvent.put(EventPayloadConstants.CAMPAIGN_ID, campaignId.toString());
        billingEvent.put(EventPayloadConstants.ADVERTISER_ID, advertiserId.toString());
        billingEvent.put(EventPayloadConstants.AMOUNT, bidStr);
        billingEvent.put(EventPayloadConstants.CHARGE_CATEGORY, ChargeCategory.AD_CLICK.name());
        billingEvent.put(EventPayloadConstants.TIMESTAMP, System.currentTimeMillis());
        producer.publishBillingEvent(eventId, billingEvent);

        // 2. Publish Tracking Event
        Map<String, Object> trackingEvent = new HashMap<>(billingEvent);
        trackingEvent.put(EventPayloadConstants.EVENT_TYPE, AdTrackingType.CLICK.name());
        trackingEvent.put(EventPayloadConstants.DEVICE_ID, deviceId);
        producer.publishTrackingEvent(eventId, trackingEvent);
    }

    @Override
    public void recordConversion(UUID campaignId, UUID advertiserId, String deviceId, String ipAddress) {
        String identifier = deviceId != null ? deviceId : ipAddress;
        if (!fraudPreventionService.isAllowed(campaignId.toString(), identifier)) {
            throw new IllegalArgumentException("Rate limit exceeded for this device/IP");
        }
        
        String idemKey = "idem:conv:" + campaignId + ":" + identifier;
        Boolean isNew = redisTemplate.opsForValue().setIfAbsent(idemKey, "1", Duration.ofMinutes(30));
        if (Boolean.FALSE.equals(isNew)) {
            // Already tracked a conversion for this device on this campaign within 30 minutes.
            return;
        }
        
        String bidStr = getCampaignBid(campaignId);
        
        long timeBucket = System.currentTimeMillis() / 10_000;
        String eventId = UUID.nameUUIDFromBytes(
            ("conv:" + campaignId + ":" + identifier + ":" + timeBucket).getBytes(StandardCharsets.UTF_8)
        ).toString();
        
        // 1. Publish Billing Event (CPA)
        Map<String, Object> billingEvent = new HashMap<>();
        billingEvent.put(EventPayloadConstants.EVENT_ID, eventId);
        billingEvent.put(EventPayloadConstants.CAMPAIGN_ID, campaignId.toString());
        billingEvent.put(EventPayloadConstants.ADVERTISER_ID, advertiserId.toString());
        billingEvent.put(EventPayloadConstants.AMOUNT, bidStr);
        billingEvent.put(EventPayloadConstants.CHARGE_CATEGORY, ChargeCategory.AD_CONVERSION.name());
        billingEvent.put(EventPayloadConstants.TIMESTAMP, System.currentTimeMillis());
        producer.publishBillingEvent(eventId, billingEvent);

        // 2. Publish Tracking Event
        Map<String, Object> trackingEvent = new HashMap<>(billingEvent);
        trackingEvent.put(EventPayloadConstants.EVENT_TYPE, AdTrackingType.CONVERSION.name());
        trackingEvent.put(EventPayloadConstants.DEVICE_ID, deviceId);
        producer.publishTrackingEvent(eventId, trackingEvent);
    }
}
