package contracts.messaging

/*
 * Real wire payload for ad-billing-events, built in EventTrackingServiceImpl and published by
 * TrackingEventProducer.publishBillingEvent via a JsonSerializer KafkaTemplate.
 *
 * Flat map keyed by EventPayloadConstants -- BillingEventConsumer reads eventId, advertiserId,
 * amount and chargeCategory at the ROOT. Note `amount` is a STRING (price.toPlainString()), not a
 * JSON number, and `timestamp` is epoch millis.
 */
org.springframework.cloud.contract.spec.Contract.make {
    description("Should publish an ad billing event to ad-billing-events")
    label("ad_billing_events")
    input {
        triggeredBy('fireAdBilling()')
    }
    outputMessage {
        sentTo('ad-billing-events')
        body([
            eventId: $(producer(regex('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'))),
            campaignId: $(producer(regex('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'))),
            advertiserId: $(producer(regex('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'))),
            amount: "0.50",
            chargeCategory: "AD_IMPRESSION",
            timestamp: $(producer(regex('[0-9]{13}')))
        ])
    }
}
