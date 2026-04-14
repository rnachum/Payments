package com.example.payment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 1,
        topics = {
            "payments.processing.requested",
            "payments.processing.requested.DLT",
            "payments.completed"
        })
class PaymentApiIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private PaymentProcessingOutboxRepository outboxRepository;

    @Autowired
    private PaymentOutboxRelayService outboxRelayService;

    @BeforeEach
    void cleanDatabase() {
        outboxRepository.deleteAll();
        paymentRepository.deleteAll();
    }

    @Test
    void postTwiceWithSameIdempotencyKeySameIdAndSingleRow() {
        String base = "http://localhost:" + port + "/payments";
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", "shared-idem-key");
        headers.setContentType(MediaType.APPLICATION_JSON);

        CreatePaymentRequest body = new CreatePaymentRequest();
        body.setAmount(new BigDecimal("12.34"));
        body.setCurrency("USD");

        HttpEntity<CreatePaymentRequest> entity = new HttpEntity<>(body, headers);

        ResponseEntity<AcceptedPaymentResponse> first =
                restTemplate.postForEntity(base, entity, AcceptedPaymentResponse.class);
        ResponseEntity<AcceptedPaymentResponse> second =
                restTemplate.postForEntity(base, entity, AcceptedPaymentResponse.class);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(first.getBody()).isNotNull();
        assertThat(second.getBody()).isNotNull();
        assertThat(first.getBody().id()).isEqualTo(second.getBody().id());
        assertThat(paymentRepository.findAll()).hasSize(1);
        assertThat(outboxRepository.findTop50ByPublishedAtIsNullOrderByIdAsc()).hasSize(1);
    }

    @Test
    void postWithInvalidAmountReturns400() {
        String base = "http://localhost:" + port + "/payments";
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", "bad-amount");
        headers.setContentType(MediaType.APPLICATION_JSON);

        CreatePaymentRequest body = new CreatePaymentRequest();
        body.setAmount(BigDecimal.valueOf(-1));
        body.setCurrency("USD");

        ResponseEntity<String> response =
                restTemplate.postForEntity(base, new HttpEntity<>(body, headers), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).contains("VALIDATION_FAILED");
    }

    @Test
    void createPaymentWritesOutboxAndRelayMarksPublished() throws InterruptedException {
        String base = "http://localhost:" + port + "/payments";
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", "outbox-relay-idem");
        headers.setContentType(MediaType.APPLICATION_JSON);

        CreatePaymentRequest body = new CreatePaymentRequest();
        body.setAmount(new BigDecimal("5.00"));
        body.setCurrency("USD");

        ResponseEntity<AcceptedPaymentResponse> created =
                restTemplate.postForEntity(base, new HttpEntity<>(body, headers), AcceptedPaymentResponse.class);
        assertThat(created.getBody()).isNotNull();
        long paymentId = created.getBody().id();

        assertThat(outboxRepository.findTop50ByPublishedAtIsNullOrderByIdAsc()).hasSize(1);

        drainOutboxUntilProcessingAndLedgerPublished(paymentId);

        assertThat(outboxRepository.findTop50ByPublishedAtIsNullOrderByIdAsc()).isEmpty();
        List<PaymentProcessingOutbox> processingRows =
                outboxRepository.findAll().stream()
                        .filter(o -> "outbox-relay-idem".equals(o.getIdempotencyKey()))
                        .toList();
        assertThat(processingRows).hasSize(1);
        assertThat(processingRows.get(0).getPublishedAt()).isNotNull();
        assertThat(outboxRepository.findAll().stream()
                        .filter(o -> o.getEventType() == OutboxEventType.PAYMENT_COMPLETED_FOR_LEDGER
                                && Long.valueOf(paymentId).equals(o.getPaymentId())))
                .singleElement()
                .satisfies(o -> assertThat(o.getPublishedAt()).isNotNull());
    }

    /**
     * Relays until both the processing-request row and the ledger COMPLETED row for this payment are
     * published. We cannot stop when there are no unpublished rows right after the first relay: the
     * consumer runs asynchronously and only then inserts the ledger outbox.
     */
    private void drainOutboxUntilProcessingAndLedgerPublished(long paymentId) throws InterruptedException {
        for (int i = 0; i < 120; i++) {
            outboxRelayService.relayPendingBatch();
            boolean processingPublished =
                    outboxRepository.findAll().stream()
                            .anyMatch(
                                    o -> o.getEventType() == OutboxEventType.PROCESSING_REQUESTED
                                            && "outbox-relay-idem".equals(o.getIdempotencyKey())
                                            && o.getPublishedAt() != null);
            boolean ledgerPublished =
                    outboxRepository.findAll().stream()
                            .anyMatch(
                                    o -> o.getEventType() == OutboxEventType.PAYMENT_COMPLETED_FOR_LEDGER
                                            && Long.valueOf(paymentId).equals(o.getPaymentId())
                                            && o.getPublishedAt() != null);
            if (processingPublished && ledgerPublished) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
    }
}
