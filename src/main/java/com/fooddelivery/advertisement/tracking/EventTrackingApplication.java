package com.fooddelivery.advertisement.tracking;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(
    scanBasePackages = {"com.fooddelivery.advertisement.tracking", "com.fooddelivery.common"}
)
@org.springframework.context.annotation.Import({com.fooddelivery.common.security.CommonSecurityConfig.class, com.fooddelivery.common.security.SecurityContextFilter.class})
public class EventTrackingApplication {
    public static void main(String[] args) {
        SpringApplication.run(EventTrackingApplication.class, args);
    }
}
