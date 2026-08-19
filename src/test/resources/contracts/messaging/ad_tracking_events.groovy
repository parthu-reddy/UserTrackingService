package contracts.messaging

org.springframework.cloud.contract.spec.Contract.make {
    description("Should send ad-tracking-events events")
    label("ad_tracking_events")
    input {
        triggeredBy('fireAdTracking()')
    }
    outputMessage {
        sentTo('ad-tracking-events')
        body([
            eventId: "ad-666",
            type: "AD_CLICKED",
            payload: [
                campaignId: 999,
                userId: "user-123"
            ]
        ])
    }
}
