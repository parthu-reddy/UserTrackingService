package com.fooddelivery.advertisement.tracking;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Generates this service's OpenAPI spec from the running controllers and writes it to
 * {@code target/openapi.json}.
 *
 * <p>Until 2026-08-27 UserTrackingService was the only spec the frontend generates a client from
 * that had no generation test — so {@code UserTrackingService/openapi.json} was hand-maintained,
 * nothing compared it to the controllers, and it was excluded from the SPEC-DRIFT check.
 *
 * <p>Leaner than the other fifteen: this service's datasource is ClickHouse and its controllers
 * do not use it, so the datasource and JPA autoconfiguration are excluded outright rather than
 * pointed at an H2 stand-in. What it shares with them is the shape that matters:
 *
 * <ul>
 *   <li>{@code @Configuration} + {@code @ComponentScan} + {@code @EnableAutoConfiguration} rather
 *       than {@code @SpringBootApplication}. The latter is meta-annotated
 *       {@code @SpringBootConfiguration}, which makes every sibling test that does not pass
 *       {@code classes=} fail with "Found multiple @SpringBootConfiguration annotated classes".
 *       That took out six modules on 2026-08-26.</li>
 *   <li>The two CommonLibrary OpenAPI customizers are {@code @Import}ed explicitly. They live in
 *       {@code com.fooddelivery.common.config}, which the scoped scan below never reaches. Without
 *       the first, springdoc labels every response {@code * / *} and each generated Zod validator
 *       degrades to {@code z.void()}.</li>
 *   <li>Kafka admin is not fail-fast and listeners do not auto-start, so no broker is required.</li>
 * </ul>
 */
@SpringBootTest(classes = OpenApiGenerationTest.TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "springdoc.writer-with-default-pretty-printer=true",
                "spring.cloud.config.enabled=false",
                "eureka.client.enabled=false",
                "spring.kafka.bootstrap-servers=localhost:9092",
                "spring.kafka.listener.auto-startup=false",
                "spring.kafka.admin.fail-fast=false",
                "spring.kafka.consumer.group-id=test",
                // This service's datasource is ClickHouse; the controllers do not touch it, and the
                // driver is not on the test classpath. Excluded rather than stubbed -- a spec
                // context should build the least that renders the controllers.
                "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
                        + "org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration,"
                        + "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,"
                        + "org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration,"
                        + "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration,"
                        + "org.springframework.boot.autoconfigure.sql.init.SqlInitializationAutoConfiguration",
                "management.health.redis.enabled=false",
                "spring.main.allow-bean-definition-overriding=true"
        })
@ActiveProfiles("test")
@AutoConfigureMockMvc(addFilters = false)
public class OpenApiGenerationTest {

    @org.springframework.context.annotation.Configuration
    @org.springframework.context.annotation.ComponentScan(
            basePackages = {"com.fooddelivery.advertisement.tracking.controller"})
    @org.springframework.context.annotation.Import({
            com.fooddelivery.common.config.OpenApiJsonMediaTypeCustomizer.class,
            com.fooddelivery.common.config.OpenApiPaginationRequiredCustomizer.class})
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration(excludeName = {
            "org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration",
            "org.springframework.boot.actuate.autoconfigure.security.servlet.ManagementWebSecurityAutoConfiguration",
            "org.springframework.boot.autoconfigure.security.reactive.ReactiveSecurityAutoConfiguration",
            "org.springframework.boot.actuate.autoconfigure.security.reactive.ManagementReactiveSecurityAutoConfiguration",
            "org.springframework.boot.autoconfigure.security.oauth2.resource.servlet.OAuth2ResourceServerAutoConfiguration"})
    static class TestApp {
    }

    // The controller's collaborators live outside the scanned package, so they are supplied here.
    @MockBean
    private com.fooddelivery.advertisement.tracking.service.ImpressionTracker impressionTracker;
    @MockBean
    private com.fooddelivery.advertisement.tracking.service.ClickTracker clickTracker;
    @MockBean
    private com.fooddelivery.advertisement.tracking.service.ConversionTracker conversionTracker;
    @MockBean
    private com.fooddelivery.common.service.RateLimitingService rateLimitingService;

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void generateOpenApi() throws Exception {
        String openApiJson = mockMvc.perform(MockMvcRequestBuilders.get("/v3/api-docs"))
                .andReturn().getResponse().getContentAsString(StandardCharsets.UTF_8);

        if (openApiJson == null || openApiJson.isEmpty()) {
            throw new IllegalStateException("Failed to retrieve the OpenAPI spec.");
        }
        Path path = Paths.get("target/openapi.json");
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        Files.write(path, openApiJson.getBytes(StandardCharsets.UTF_8));
        System.out.println("OpenAPI spec written to target/openapi.json");
    }
}
