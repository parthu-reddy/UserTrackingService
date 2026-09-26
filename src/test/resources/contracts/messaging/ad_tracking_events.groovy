package contracts.messaging

/*
 * Real wire payload for ad-tracking-events. EventTrackingServiceImpl copies the billing event
 * (new HashMap<>(billingEvent)) and adds eventType + deviceId, so this carries every billing field
 * plus those two, and spendDay: the day of the advertiser's calendar the spend counts against,
 * decided once when it happened (TimezoneCorrectness_2026-09-25). Consumed by CampaignService analytics.
 * spendDay has a fixed consumer value: a producer-only regex would hand the stub a random string.
 */
org.springframework.cloud.contract.spec.Contract.make {
    description("Should publish an ad tracking event to ad-tracking-events")
    label("ad_tracking_events")
    input {
        triggeredBy('fireAdTracking()')
    }
    outputMessage {
        sentTo('ad-tracking-events')
        body([
            eventId: $(producer(regex('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'))),
            campaignId: $(producer(regex('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'))),
            advertiserId: $(producer(regex('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'))),
            amount: "0.50",
            chargeCategory: "AD_IMPRESSION",
            timestamp: $(producer(regex('[0-9]{13}'))),
            eventType: "IMPRESSION",
            deviceId: "device-abc-123",
            spendDay: $(consumer('2026-09-25'), producer(regex('[0-9]{4}-[0-9]{2}-[0-9]{2}')))
        ])
    }
}
