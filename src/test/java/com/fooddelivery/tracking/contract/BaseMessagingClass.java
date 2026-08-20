package com.fooddelivery.tracking.contract;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.verifier.messaging.boot.AutoConfigureMessageVerifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.context.annotation.Bean;

@SpringBootTest(classes = BaseMessagingClass.TestConfig.class, webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {"spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration,org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration"})
@org.springframework.test.context.ActiveProfiles("contract-test")
@AutoConfigureMessageVerifier
@EmbeddedKafka(partitions = 1, topics = {"ad-billing-events", "ad-tracking-events"})
public abstract class BaseMessagingClass {

    @org.springframework.boot.SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    static class TestConfig {
        @Bean
        public KafkaMessageVerifier kafkaMessageVerifier() {
            return new KafkaMessageVerifier();
        }
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> System.getProperty("spring.embedded.kafka.brokers", "localhost:9092"));
    }

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Publishes through the real TrackingEventProducer with the exact Map that
     * EventTrackingServiceImpl builds, so the contract is bound to the production payload keys
     * (EventPayloadConstants) and to the JsonSerializer wire format -- not to a hand-written blob.
     */
    private java.util.Map<String, Object> billingEvent() {
        java.util.Map<String, Object> billingEvent = new java.util.HashMap<>();
        billingEvent.put(com.fooddelivery.common.constants.EventPayloadConstants.EVENT_ID,
                "b64752ce-65f7-503c-a1cb-2ecd2b5412ff");
        billingEvent.put(com.fooddelivery.common.constants.EventPayloadConstants.CAMPAIGN_ID,
                "1d9c4f70-2a83-4b16-9e5d-7c0a3b8f6e41");
        billingEvent.put(com.fooddelivery.common.constants.EventPayloadConstants.ADVERTISER_ID,
                "3e14926d-0c98-5840-abcd-37ec439ddc25");
        billingEvent.put(com.fooddelivery.common.constants.EventPayloadConstants.AMOUNT,
                new java.math.BigDecimal("0.50").toPlainString());
        billingEvent.put(com.fooddelivery.common.constants.EventPayloadConstants.CHARGE_CATEGORY,
                com.fooddelivery.common.enums.ChargeCategory.AD_IMPRESSION.name());
        billingEvent.put(com.fooddelivery.common.constants.EventPayloadConstants.TIMESTAMP,
                1699999999999L);
        return billingEvent;
    }

    public void fireAdBilling() {
        java.util.Map<String, Object> billingEvent = billingEvent();
        new com.fooddelivery.advertisement.tracking.kafka.TrackingEventProducer(kafkaTemplate)
                .publishBillingEvent((String) billingEvent.get("eventId"), billingEvent);
    }

    public void fireAdTracking() {
        // EventTrackingServiceImpl derives the tracking event from the billing event.
        java.util.Map<String, Object> trackingEvent = new java.util.HashMap<>(billingEvent());
        trackingEvent.put(com.fooddelivery.common.constants.EventPayloadConstants.EVENT_TYPE,
                com.fooddelivery.advertisement.tracking.enums.AdTrackingType.IMPRESSION.name());
        trackingEvent.put(com.fooddelivery.common.constants.EventPayloadConstants.DEVICE_ID,
                "device-abc-123");
        new com.fooddelivery.advertisement.tracking.kafka.TrackingEventProducer(kafkaTemplate)
                .publishTrackingEvent((String) trackingEvent.get("eventId"), trackingEvent);
    }

}
