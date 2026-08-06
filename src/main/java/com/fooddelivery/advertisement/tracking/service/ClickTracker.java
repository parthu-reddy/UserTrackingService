package com.fooddelivery.advertisement.tracking.service;
import java.util.UUID;
public interface ClickTracker {
    void recordClick(UUID campaignId, UUID advertiserId, String deviceId, String ipAddress);
}
