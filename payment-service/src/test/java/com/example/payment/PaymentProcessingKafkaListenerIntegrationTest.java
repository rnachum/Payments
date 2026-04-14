package com.example.payment;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 1,
        topics = {
            "payments.processing.requested",
            "payments.processing.requested.DLT",
            "payments.completed"
        })
class PaymentProcessingKafkaListenerIntegrationTest {

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private KafkaTemplate<String, PaymentProcessingRequested> kafkaTemplate;

    @Autowired
    private PaymentKafkaProperties kafkaProperties;

    @Autowired
    private PaymentProcessingOutboxRepository outboxRepository;

    @Autowired
    private PaymentOutboxRelayService outboxRelayService;

    @Test
    void listenerCompletesPendingPayment() throws Exception {
        Payment payment = new Payment();
        payment.setAmount(BigDecimal.ONE);
        payment.setCurrency("USD");
        payment.setStatus(PaymentStatus.PENDING);
        payment.setIdempotencyKey("idem-" + UUID.randomUUID());
        payment = paymentRepository.save(payment);
        long id = payment.getId();

        kafkaTemplate
                .send(
                        kafkaProperties.topic(),
                        payment.getIdempotencyKey(),
                        new PaymentProcessingRequested(id, payment.getIdempotencyKey()))
                .get(15, TimeUnit.SECONDS);

        PaymentStatus finalStatus = waitForStatus(id, PaymentStatus.COMPLETED, 10_000);
        assertThat(finalStatus).isEqualTo(PaymentStatus.COMPLETED);

        var ledgerPending =
                outboxRepository.findAll().stream()
                        .filter(o -> o.getEventType() == OutboxEventType.PAYMENT_COMPLETED_FOR_LEDGER
                                && id == o.getPaymentId()
                                && o.getPublishedAt() == null)
                        .toList();
        assertThat(ledgerPending).hasSize(1);
        outboxRelayService.relayPendingBatch();
        assertThat(outboxRepository.findById(ledgerPending.get(0).getId()).orElseThrow().getPublishedAt())
                .isNotNull();
    }

    @Test
    void duplicateKafkaDeliveryStillLeavesPaymentCompleted() throws Exception {
        Payment payment = new Payment();
        payment.setAmount(BigDecimal.ONE);
        payment.setCurrency("USD");
        payment.setStatus(PaymentStatus.PENDING);
        payment.setIdempotencyKey("idem-dup-" + UUID.randomUUID());
        payment = paymentRepository.save(payment);
        long id = payment.getId();

        var record =
                new PaymentProcessingRequested(id, payment.getIdempotencyKey());
        kafkaTemplate.send(kafkaProperties.topic(), payment.getIdempotencyKey(), record).get(15, TimeUnit.SECONDS);
        kafkaTemplate.send(kafkaProperties.topic(), payment.getIdempotencyKey(), record).get(15, TimeUnit.SECONDS);

        PaymentStatus finalStatus = waitForStatus(id, PaymentStatus.COMPLETED, 10_000);
        assertThat(finalStatus).isEqualTo(PaymentStatus.COMPLETED);
    }

    private PaymentStatus waitForStatus(long paymentId, PaymentStatus expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            PaymentStatus status = paymentRepository
                    .findById(paymentId)
                    .map(Payment::getStatus)
                    .orElse(PaymentStatus.FAILED);
            if (status == expected) {
                return status;
            }
            Thread.sleep(50);
        }
        return paymentRepository.findById(paymentId).map(Payment::getStatus).orElse(null);
    }
}
