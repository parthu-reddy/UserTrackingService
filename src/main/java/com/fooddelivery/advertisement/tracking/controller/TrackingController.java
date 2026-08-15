package com.fooddelivery.advertisement.tracking.controller;

import com.fooddelivery.advertisement.tracking.service.ClickTracker;
import com.fooddelivery.advertisement.tracking.service.ConversionTracker;
import com.fooddelivery.advertisement.tracking.service.ImpressionTracker;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/api/v1/tracking")
@lombok.extern.slf4j.Slf4j
public class TrackingController {
    @java.lang.SuppressWarnings("all")

    private final ImpressionTracker impressionTracker;
    private final ClickTracker clickTracker;
    private final ConversionTracker conversionTracker;
    // Dedicated bounded thread pool for tracking I/O (Kafka + Redis).
    // Prevents saturation of ForkJoinPool.commonPool() under peak load.
    private final ExecutorService trackingExecutor = Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors() * 2), r -> {
        Thread t = new Thread(r, "tracking-worker");
        t.setDaemon(true);
        return t;
    });
    // In-memory sliding-window rate limiter: max 100 requests per 10s per client key.
    // Prevents malicious actors from flooding impressions to drain advertiser budgets.
    private static final int MAX_REQUESTS_PER_WINDOW = 100;
    private static final long WINDOW_MS = 10000L;
    private final ConcurrentHashMap<String, long[]> rateLimitMap = new ConcurrentHashMap<>();

    // rateLimitMap value: [windowStart, count]
    private final ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "rate-limit-cleaner");
        t.setDaemon(true);
        return t;
    });

    {
        // Scheduled cleanup of stale rate-limit entries every 60s to prevent memory leaks
        cleaner.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            rateLimitMap.entrySet().removeIf(e -> (now - e.getValue()[0]) > WINDOW_MS * 6);
        }, 60, 60, TimeUnit.SECONDS);
    }

    @jakarta.annotation.PreDestroy
    public void shutdown() {
        log.info("Shutting down TrackingController executors");
        cleaner.shutdownNow();
        trackingExecutor.shutdownNow();
    }

    private boolean isRateLimited(String clientKey) {
        long now = System.currentTimeMillis();
        long[] bucket = rateLimitMap.compute(clientKey, (key, val) -> {
            if (val == null || (now - val[0]) > WINDOW_MS) {
                return new long[] {now, 1};
            }
            val[1]++;
            return val;
        });
        return bucket[1] > MAX_REQUESTS_PER_WINDOW;
    }

    private String getClientKey(String deviceId, String ipAddress) {
        if (deviceId != null && !deviceId.isBlank()) return "dev:" + deviceId;
        if (ipAddress != null && !ipAddress.isBlank()) return "ip:" + ipAddress.split(",")[0].trim();
        return "unknown";
    }

    public TrackingController(ImpressionTracker impressionTracker, ClickTracker clickTracker, ConversionTracker conversionTracker) {
        this.impressionTracker = impressionTracker;
        this.clickTracker = clickTracker;
        this.conversionTracker = conversionTracker;
    }

    @GetMapping("/impression")
    public CompletableFuture<ResponseEntity<Void>> trackImpression(@RequestParam UUID campaignId, @RequestParam UUID advertiserId, @RequestParam String wp,  // winning price (encrypted)
    @RequestHeader(value = com.fooddelivery.common.constants.HeaderConstants.HEADER_DEVICE_ID, required = false) String deviceId, @RequestHeader(value = com.fooddelivery.common.constants.HeaderConstants.HEADER_FORWARDED_FOR, required = false) String ipAddress) {
        if (isRateLimited(getClientKey(deviceId, ipAddress))) {
            return CompletableFuture.completedFuture(ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build());
        }
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
    public CompletableFuture<ResponseEntity<Void>> trackClick(@RequestParam UUID campaignId, @RequestParam UUID advertiserId, @RequestParam String wp, @RequestHeader(value = com.fooddelivery.common.constants.HeaderConstants.HEADER_DEVICE_ID, required = false) String deviceId, @RequestHeader(value = com.fooddelivery.common.constants.HeaderConstants.HEADER_FORWARDED_FOR, required = false) String ipAddress) {
        if (isRateLimited(getClientKey(deviceId, ipAddress))) {
            return CompletableFuture.completedFuture(ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build());
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                clickTracker.recordClick(campaignId, advertiserId, wp, deviceId, ipAddress);
                return ResponseEntity.noContent().<Void>build();
            } catch (IllegalArgumentException e) {
                return ResponseEntity.badRequest().<Void>build();
            }
        }, trackingExecutor);
    }

    @GetMapping("/conversion")
    public CompletableFuture<ResponseEntity<Void>> trackConversion(@RequestParam UUID campaignId, @RequestParam UUID advertiserId, @RequestParam String wp, @RequestHeader(value = com.fooddelivery.common.constants.HeaderConstants.HEADER_DEVICE_ID, required = false) String deviceId, @RequestHeader(value = com.fooddelivery.common.constants.HeaderConstants.HEADER_FORWARDED_FOR, required = false) String ipAddress) {
        if (isRateLimited(getClientKey(deviceId, ipAddress))) {
            return CompletableFuture.completedFuture(ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build());
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                conversionTracker.recordConversion(campaignId, advertiserId, wp, deviceId, ipAddress);
                return ResponseEntity.noContent().<Void>build();
            } catch (IllegalArgumentException e) {
                return ResponseEntity.badRequest().<Void>build();
            }
        }, trackingExecutor);
    }
}
