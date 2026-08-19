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

@SpringBootTest(classes = BaseMessagingClass.TestConfig.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@AutoConfigureMessageVerifier
@EmbeddedKafka(partitions = 1, topics = {"ad-billing-events", "ad-tracking-events"})
public abstract class BaseMessagingClass {

    @org.springframework.boot.test.context.TestConfiguration
    
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
    private KafkaTemplate<String, String> kafkaTemplate;

    public void fireAdBilling() {
        String payload = """
{
  "eventId": "ad-555",
  "type": "AD_CLICK_BILLED",
  "payload": {
    "campaignId": 999,
    "cost": 0.50
  }
}""";
        kafkaTemplate.send("ad-billing-events", payload);
    }
    public void fireAdTracking() {
        String payload = """
{
  "eventId": "ad-666",
  "type": "AD_CLICKED",
  "payload": {
    "campaignId": 999,
    "userId": "user-123"
  }
}""";
        kafkaTemplate.send("ad-tracking-events", payload);
    }

}
