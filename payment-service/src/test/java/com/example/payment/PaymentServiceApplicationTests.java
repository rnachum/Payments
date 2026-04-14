package com.example.payment;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 1,
        topics = {
            "payments.processing.requested",
            "payments.processing.requested.DLT",
            "payments.completed"
        })
class PaymentServiceApplicationTests {

    @Test
    void contextLoads() {
    }
}
