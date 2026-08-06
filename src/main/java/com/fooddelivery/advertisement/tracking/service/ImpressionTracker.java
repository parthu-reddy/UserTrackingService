package com.fooddelivery.advertisement.tracking.service;
import java.util.UUID;
public interface ImpressionTracker {
    void recordImpression(UUID campaignId, UUID advertiserId, String encryptedPrice, String deviceId, String ipAddress);
}
