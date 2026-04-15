package com.example.ledger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"payments.completed", "payments.completed.DLT"})
class PaymentCompletedConsumerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, PaymentCompletedForLedgerEvent> kafkaTemplate;

    @Autowired
    private AppliedLedgerEventRepository appliedLedgerEventRepository;

    @Autowired
    private JournalLineRepository journalLineRepository;

    @Value("${ledger.kafka.payments-completed-topic}")
    private String topic;

    @BeforeEach
    void clean() {
        journalLineRepository.deleteAll();
        appliedLedgerEventRepository.deleteAll();
    }

    @Test
    void booksJournalLinesAndSecondDeliveryIsIdempotent() throws Exception {
        String eventId = UUID.randomUUID().toString();
        var event = new PaymentCompletedForLedgerEvent(
                eventId,
                42L,
                new BigDecimal("10.00"),
                "USD",
                "idem-1",
                Instant.parse("2026-01-15T12:00:00Z"));

        kafkaTemplate.send(topic, eventId, event).get();

        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertThat(appliedLedgerEventRepository.findByEventId(eventId)).isPresent());

        assertThat(journalLineRepository.findByEventId(eventId)).hasSize(2);

        kafkaTemplate.send(topic, eventId, event).get();

        await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertThat(journalLineRepository.findAll()).hasSize(2));
    }
}
