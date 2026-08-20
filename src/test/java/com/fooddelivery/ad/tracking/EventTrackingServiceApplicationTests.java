package com.fooddelivery.ad.tracking;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/*
 * A bare @SpringBootTest searches UPWARD from this test's own package for a @SpringBootConfiguration.
 * The application lives in com.fooddelivery.advertisement.tracking -- a sibling branch of this test's
 * com.fooddelivery.ad.tracking -- so the search never reaches it. Name it explicitly.
 */
@org.springframework.test.context.ActiveProfiles("contract-test")
@SpringBootTest(
        classes = com.fooddelivery.advertisement.tracking.EventTrackingApplication.class,
        properties = {
                "spring.redis.enabled=false",
                "spring.main.allow-bean-definition-overriding=true",
                "eureka.client.enabled=false",
                "spring.cloud.config.enabled=false"
        })
class EventTrackingServiceApplicationTests {

    /* RateLimitingService is @ConditionalOnProperty("spring.redis.enabled"), which this test turns
       off -- but the controllers require it. */
    @org.springframework.boot.test.mock.mockito.MockBean
    private com.fooddelivery.common.service.RateLimitingService rateLimitingService;

    /* Redis autoconfig is off in the contract-test profile; FraudPreventionService needs this. */
    @org.springframework.boot.test.mock.mockito.MockBean
    private org.springframework.data.redis.core.StringRedisTemplate stringRedisTemplate;

    @Test
    void contextLoads() {
    }
}
