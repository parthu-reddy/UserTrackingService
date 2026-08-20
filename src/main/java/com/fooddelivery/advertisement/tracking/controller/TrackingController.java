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
    private final com.fooddelivery.common.service.RateLimitingService rateLimitingService;
    // Dedicated bounded thread pool for tracking I/O (Kafka + Redis).
    // Prevents saturation of ForkJoinPool.commonPool() under peak load.
    private final ExecutorService trackingExecutor = Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors() * 2), r -> {
        Thread t = new Thread(r, "tracking-worker");
        t.setDaemon(true);
        return t;
    });

    @jakarta.annotation.PreDestroy
    public void shutdown() {
        log.info("Shutting down TrackingController executors");
        trackingExecutor.shutdownNow();
    }

    private boolean isRateLimited(String clientKey) {
        if (clientKey == null || clientKey.isBlank() || clientKey.equals("unknown")) return true;
        io.github.bucket4j.Bucket bucket = rateLimitingService.resolveBucket("tracking:" + clientKey, 100, 100, java.time.Duration.ofSeconds(10));
        return !bucket.tryConsume(1);
    }

    private String getClientKey(String fingerprint) {
        if (fingerprint != null && !fingerprint.isBlank()) return fingerprint;
        return "unknown";
    }

    public TrackingController(ImpressionTracker impressionTracker, ClickTracker clickTracker, ConversionTracker conversionTracker, com.fooddelivery.common.service.RateLimitingService rateLimitingService) {
        this.impressionTracker = impressionTracker;
        this.clickTracker = clickTracker;
        this.conversionTracker = conversionTracker;
        this.rateLimitingService = rateLimitingService;
    }

    @GetMapping("/impression")
    public CompletableFuture<ResponseEntity<Void>> trackImpression(@RequestParam UUID campaignId, @RequestParam UUID advertiserId, @RequestParam String wp,  // winning price (encrypted)
    @RequestHeader(value = "X-Client-Fingerprint", required = false) String fingerprint) {
        if (isRateLimited(getClientKey(fingerprint))) {
            return CompletableFuture.completedFuture(ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build());
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                impressionTracker.recordImpression(campaignId, advertiserId, wp, fingerprint, fingerprint);
                return ResponseEntity.noContent().<Void>build();
            } catch (IllegalArgumentException e) {
                // Bad macro/decryption -> drop event, return 400
                return ResponseEntity.badRequest().<Void>build();
            }
        }, trackingExecutor);
    }

    @GetMapping("/click")
    public CompletableFuture<ResponseEntity<Void>> trackClick(@RequestParam UUID campaignId, @RequestParam UUID advertiserId, @RequestParam String wp, @RequestHeader(value = "X-Client-Fingerprint", required = false) String fingerprint) {
        if (isRateLimited(getClientKey(fingerprint))) {
            return CompletableFuture.completedFuture(ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build());
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                clickTracker.recordClick(campaignId, advertiserId, wp, fingerprint, fingerprint);
                return ResponseEntity.noContent().<Void>build();
            } catch (IllegalArgumentException e) {
                return ResponseEntity.badRequest().<Void>build();
            }
        }, trackingExecutor);
    }

    @GetMapping("/conversion")
    public CompletableFuture<ResponseEntity<Void>> trackConversion(@RequestParam UUID campaignId, @RequestParam UUID advertiserId, @RequestParam String wp, @RequestHeader(value = "X-Client-Fingerprint", required = false) String fingerprint) {
        if (isRateLimited(getClientKey(fingerprint))) {
            return CompletableFuture.completedFuture(ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build());
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                conversionTracker.recordConversion(campaignId, advertiserId, wp, fingerprint, fingerprint);
                return ResponseEntity.noContent().<Void>build();
            } catch (IllegalArgumentException e) {
                return ResponseEntity.badRequest().<Void>build();
            }
        }, trackingExecutor);
    }
}
