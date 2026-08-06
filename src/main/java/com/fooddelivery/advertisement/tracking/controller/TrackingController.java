package com.fooddelivery.advertisement.tracking.controller;

import com.fooddelivery.advertisement.tracking.service.ClickTracker;
import com.fooddelivery.advertisement.tracking.service.ConversionTracker;
import com.fooddelivery.advertisement.tracking.service.ImpressionTracker;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
@RequestMapping("/api/v1/tracking")
public class TrackingController {
    
    private final ImpressionTracker impressionTracker;
    private final ClickTracker clickTracker;
    private final ConversionTracker conversionTracker;
    
    // Dedicated bounded thread pool for tracking I/O (Kafka + Redis).
    // Prevents saturation of ForkJoinPool.commonPool() under peak load.
    private final ExecutorService trackingExecutor = Executors.newFixedThreadPool(
        Math.max(4, Runtime.getRuntime().availableProcessors() * 2),
        r -> {
            Thread t = new Thread(r, "tracking-worker");
            t.setDaemon(true);
            return t;
        }
    );
    
    public TrackingController(ImpressionTracker impressionTracker, ClickTracker clickTracker, ConversionTracker conversionTracker) {
        this.impressionTracker = impressionTracker;
        this.clickTracker = clickTracker;
        this.conversionTracker = conversionTracker;
    }
    
    @GetMapping("/impression")
    public CompletableFuture<ResponseEntity<Void>> trackImpression(
            @RequestParam UUID campaignId,
            @RequestParam UUID advertiserId,
            @RequestParam String wp, // winning price (encrypted)
            @RequestHeader(value = com.fooddelivery.common.constants.HeaderConstants.HEADER_DEVICE_ID, required = false) String deviceId,
            @RequestHeader(value = com.fooddelivery.common.constants.HeaderConstants.HEADER_FORWARDED_FOR, required = false) String ipAddress) {
            
        return CompletableFuture.supplyAsync(() -> {
            try {
                impressionTracker.recordImpression(campaignId, advertiserId, wp, deviceId, ipAddress);
                return ResponseEntity.noContent().<Void>build();
            } catch (IllegalArgumentException e) {
                // Bad macro/decryption -> drop event, return 400
                return ResponseEntity.badRequest().<Void>build();
            }
        }, trackingExecutor);
    }
    
    @GetMapping("/click")
    public CompletableFuture<ResponseEntity<Void>> trackClick(
            @RequestParam UUID campaignId,
            @RequestParam UUID advertiserId,
            @RequestHeader(value = com.fooddelivery.common.constants.HeaderConstants.HEADER_DEVICE_ID, required = false) String deviceId,
            @RequestHeader(value = com.fooddelivery.common.constants.HeaderConstants.HEADER_FORWARDED_FOR, required = false) String ipAddress) {
            
        return CompletableFuture.supplyAsync(() -> {
            clickTracker.recordClick(campaignId, advertiserId, deviceId, ipAddress);
            return ResponseEntity.noContent().<Void>build();
        }, trackingExecutor);
    }
    
    @GetMapping("/conversion")
    public CompletableFuture<ResponseEntity<Void>> trackConversion(
            @RequestParam UUID campaignId,
            @RequestParam UUID advertiserId,
            @RequestHeader(value = com.fooddelivery.common.constants.HeaderConstants.HEADER_DEVICE_ID, required = false) String deviceId,
            @RequestHeader(value = com.fooddelivery.common.constants.HeaderConstants.HEADER_FORWARDED_FOR, required = false) String ipAddress) {
            
        return CompletableFuture.supplyAsync(() -> {
            conversionTracker.recordConversion(campaignId, advertiserId, deviceId, ipAddress);
            return ResponseEntity.noContent().<Void>build();
        }, trackingExecutor);
    }
}

