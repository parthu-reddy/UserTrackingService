package com.fooddelivery.advertisement.tracking.service;

import com.fooddelivery.common.constants.LuaScripts;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class FraudPreventionService {

    private static final Logger log = LoggerFactory.getLogger(FraudPreventionService.class);

    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<List> rateLimitScript;
    
    // Local fallback cache if Redis goes down. 
    // Acts as a simple rate limiter: presence of key means rate limited.
    // Expire after 5 seconds to match the 0.2 rps rate limit.
    private final Cache<String, Boolean> localFallbackCache;

    public FraudPreventionService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
        this.rateLimitScript = new DefaultRedisScript<>();
        this.rateLimitScript.setScriptText(LuaScripts.RATE_LIMITER_SCRIPT);
        this.rateLimitScript.setResultType(List.class);
        
        this.localFallbackCache = Caffeine.newBuilder()
            .expireAfterWrite(5, TimeUnit.SECONDS)
            .maximumSize(100_000)
            .build();
    }

    public boolean isAllowed(String campaignId, String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return true; // If no identifier (e.g. no IP/DeviceID), we can't reliably rate limit
        }
        
        String key = "ratelimit:ad:" + campaignId + ":" + identifier;
        long now = System.currentTimeMillis() / 1000;
        
        try {
            List result = redisTemplate.execute(
                rateLimitScript, 
                Collections.singletonList(key), 
                "0.2", "1", String.valueOf(now), "1"
            );
            return result != null && !result.isEmpty() && ((Number) result.get(0)).longValue() == 1L;
        } catch (Exception e) {
            log.warn("Redis rate limit failed. Using local Caffeine fallback for key {}", key);
            // Local fallback logic
            if (localFallbackCache.getIfPresent(key) != null) {
                return false; // Already seen within 5 seconds -> Block
            }
            localFallbackCache.put(key, Boolean.TRUE);
            return true; // First time seeing within 5 seconds -> Allow
        }
    }
}

