package com.example.payment;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class GlobalExceptionHandlerWebMvcTest {

    @RestController
    static class TestController {
        @GetMapping("/__test/idempotent-not-found")
        void throwIdempotentNotFound() {
            throw new IdempotentPaymentNotFoundAfterDuplicateException("test-idem-key");
        }

        @GetMapping("/__test/kafka-publish")
        void throwKafkaPublish() {
            throw new KafkaPublishException(77L, "Kafka send failed for payment 77", new RuntimeException("root"));
        }
    }

    @Test
    void idempotentPaymentNotFoundReturns503WithRetryAfter() throws Exception {
        MockMvc mockMvc =
                MockMvcBuilders.standaloneSetup(new TestController())
                        .setControllerAdvice(new GlobalExceptionHandler())
                        .build();

        mockMvc.perform(get("/__test/idempotent-not-found"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string(HttpHeaders.RETRY_AFTER, "3"))
                .andExpect(jsonPath("$.error").value("IDEMPOTENCY_CONFIRMATION_PENDING"))
                .andExpect(jsonPath("$.retryable").value(true))
                .andExpect(jsonPath("$.idempotencyKey").value("test-idem-key"));
    }

    @Test
    void kafkaPublishExceptionReturns503WithRetryAfter() throws Exception {
        MockMvc mockMvc =
                MockMvcBuilders.standaloneSetup(new TestController())
                        .setControllerAdvice(new GlobalExceptionHandler())
                        .build();

        mockMvc.perform(get("/__test/kafka-publish"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string(HttpHeaders.RETRY_AFTER, "3"))
                .andExpect(jsonPath("$.error").value("KAFKA_UNAVAILABLE"))
                .andExpect(jsonPath("$.retryable").value(true))
                .andExpect(jsonPath("$.paymentId").value(77));
    }
}
