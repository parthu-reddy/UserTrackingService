package com.fooddelivery.advertisement.tracking.service;

import com.fooddelivery.advertisement.tracking.kafka.TrackingEventProducer;
import com.fooddelivery.advertisement.tracking.util.CryptoService;
import com.fooddelivery.common.security.AuctionTokenService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Defect D6 (RandomDocuments/TimezoneCorrectness_2026-09-25): an impression counts against the day
 * of the ADVERTISER's calendar it happened on, decided once, here, from the zone signed into the
 * auction token. That day is in the Redis budget key and on the tracking event, so pacing and the
 * performance report use the same day. This build runs in Pacific/Chatham.
 */
class SpendDayTest {

    @Test
    @SuppressWarnings("unchecked")
    void anImpressionAt2045ZCountsAgainstThe26thForAKolkataAdvertiser() {
        UUID campaignId = UUID.randomUUID();
        UUID advertiserId = UUID.randomUUID();
        CryptoService crypto = mock(CryptoService.class);
        when(crypto.verifyAuctionToken(eq("tok"), eq(campaignId), eq(advertiserId), anyString()))
                .thenReturn(new AuctionTokenService.AuctionToken(campaignId, advertiserId, "0.50", UUID.randomUUID(),
                        Long.MAX_VALUE, ZoneId.of("Asia/Kolkata")));
        FraudPreventionService fraud = mock(FraudPreventionService.class);
        when(fraud.isAllowed(any(), any())).thenReturn(true);
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(anyString(), anyString(), any(java.time.Duration.class))).thenReturn(true);
        TrackingEventProducer producer = mock(TrackingEventProducer.class);

        // 20:45Z is 02:15 on the 26th in Kolkata (and 09:30 on the 26th in Chatham, the JVM's zone here).
        new EventTrackingServiceImpl(crypto, producer, fraud, redis,
                Clock.fixed(Instant.parse("2026-09-25T20:45:00Z"), ZoneOffset.UTC))
                .recordImpression(campaignId, advertiserId, "tok", "device-1", "10.0.0.1");

        ArgumentCaptor<List<String>> keys = ArgumentCaptor.forClass(List.class);
        verify(redis, atLeastOnce()).execute(any(RedisScript.class), keys.capture(), any(), any());
        assertThat(keys.getAllValues()).extracting(k -> k.get(0))
                .contains("campaign:spend:daily:" + campaignId + ":2026-09-26");

        ArgumentCaptor<Map<String, Object>> event = ArgumentCaptor.forClass(Map.class);
        verify(producer).publishTrackingEvent(anyString(), event.capture());
        assertThat(event.getValue()).containsEntry("spendDay", "2026-09-26");
    }
}
