package com.fooddelivery.advertisement.tracking.kafka;

import com.fooddelivery.common.constants.KafkaConstants;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import java.util.Map;

@Component
public class TrackingEventProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    public TrackingEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    public void publishBillingEvent(String eventId, Map<String, Object> payload) {
        kafkaTemplate.send(KafkaConstants.TOPIC_AD_BILLING_EVENTS, eventId, payload);
    }
    
    public void publishTrackingEvent(String eventId, Map<String, Object> payload) {
        String campaignId = (String) payload.get(com.fooddelivery.common.constants.EventPayloadConstants.CAMPAIGN_ID);
        String key = campaignId != null ? campaignId : eventId;
        kafkaTemplate.send(KafkaConstants.TOPIC_AD_TRACKING_EVENTS, key, payload);
    }
}
