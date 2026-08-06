package com.fooddelivery.advertisement.tracking.service;
import java.util.UUID;
public interface ConversionTracker {
    void recordConversion(UUID campaignId, UUID advertiserId, String deviceId, String ipAddress);
}
