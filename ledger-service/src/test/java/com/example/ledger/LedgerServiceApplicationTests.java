package com.example.ledger;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"payments.completed", "payments.completed.DLT"})
class LedgerServiceApplicationTests {

    @Test
    void contextLoads() {
    }
}
