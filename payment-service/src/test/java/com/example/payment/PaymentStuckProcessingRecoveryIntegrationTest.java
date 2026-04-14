package com.example.payment;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "payment.processing.stuck-threshold=PT1S")
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 1,
        topics = {
            "payments.processing.requested",
            "payments.processing.requested.DLT",
            "payments.completed"
        })
class PaymentStuckProcessingRecoveryIntegrationTest {

    @Autowired
    private PaymentStuckProcessingRecoveryService recoveryService;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PaymentProcessingOutboxRepository outboxRepository;

    @Autowired
    private PaymentOutboxRelayService outboxRelayService;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @MockBean
    private PaymentProcessingRequestPublisher publisher;

    @Test
    void staleProcessingResetToPendingAndRepublished() {
        Long[] idHolder = new Long[1];
        String[] idemHolder = new String[1];
        transactionTemplate.executeWithoutResult(status -> {
            Payment p = new Payment();
            p.setAmount(BigDecimal.ONE);
            p.setCurrency("USD");
            p.setStatus(PaymentStatus.PENDING);
            p.setIdempotencyKey("recovery-" + UUID.randomUUID());
            Payment saved = paymentRepository.save(p);
            idHolder[0] = saved.getId();
            idemHolder[0] = saved.getIdempotencyKey();

            Instant claimTime = Instant.now();
            paymentRepository.updateStatusIfExpected(
                    idHolder[0], PaymentStatus.PENDING, PaymentStatus.PROCESSING, claimTime);

            jdbcTemplate.update(
                    "UPDATE payments SET updated_at = ? WHERE id = ?",
                    Timestamp.from(Instant.now().minus(2, ChronoUnit.HOURS)),
                    idHolder[0]);
        });

        Long id = idHolder[0];
        String idempotencyKey = idemHolder[0];

        recoveryService.recoverStaleProcessing();

        assertThat(paymentRepository.findById(id).orElseThrow().getStatus()).isEqualTo(PaymentStatus.PENDING);
        assertThat(outboxRepository.findTop50ByPublishedAtIsNullOrderByIdAsc()).hasSize(1);

        outboxRelayService.relayPendingBatch();

        verify(publisher).publishProcessingRequested(eq(id), eq(idempotencyKey));
        assertThat(outboxRepository.findTop50ByPublishedAtIsNullOrderByIdAsc()).isEmpty();
    }
}
