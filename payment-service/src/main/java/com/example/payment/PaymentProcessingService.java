package com.example.payment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
public class PaymentProcessingService {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessingService.class);
    private final PaymentRepository paymentRepository;
    private final PaymentProcessingOutboxRepository processingOutboxRepository;
    private final ObjectMapper objectMapper;

    PaymentProcessingService(
            PaymentRepository paymentRepository,
            PaymentProcessingOutboxRepository processingOutboxRepository,
            ObjectMapper objectMapper) {
        this.paymentRepository = paymentRepository;
        this.processingOutboxRepository = processingOutboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void processPaymentRequest(long paymentId) {
        Instant now = Instant.now();
        if (paymentRepository.updateStatusIfExpected(paymentId, PaymentStatus.PENDING, PaymentStatus.PROCESSING, now)
                == 0) {
            log.info("Skipped processing paymentId={} (not in PENDING or already claimed)", paymentId);
            return;
        }
        try {
            placeholderWork();
            if (paymentRepository.updateStatusIfExpected(
                            paymentId, PaymentStatus.PROCESSING, PaymentStatus.COMPLETED, Instant.now())
                    == 1) {
                enqueueLedgerCompletedOutbox(paymentId);
                log.info("Completed processing paymentId={}", paymentId);
            } else {
                log.warn("Expected PROCESSING when completing paymentId={}", paymentId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            paymentRepository.updateStatusIfExpected(
                    paymentId, PaymentStatus.PROCESSING, PaymentStatus.FAILED, Instant.now());
            log.info("Marked paymentId={} FAILED after interrupt", paymentId);
        }
    }

    private void enqueueLedgerCompletedOutbox(long paymentId) {
        Payment p = paymentRepository.findById(paymentId).orElseThrow();
        String eventId = UUID.randomUUID().toString();
        var event = new PaymentCompletedForLedgerEvent(
                eventId,
                paymentId,
                p.getAmount(),
                p.getCurrency(),
                p.getIdempotencyKey(),
                Instant.now());
        try {
            String json = objectMapper.writeValueAsString(event);
            processingOutboxRepository.save(
                    new PaymentProcessingOutbox(
                            paymentId, eventId, OutboxEventType.PAYMENT_COMPLETED_FOR_LEDGER, json));
            log.info("Enqueued ledger outbox paymentId={} eventId={}", paymentId, eventId);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize ledger event for payment " + paymentId, e);
        }
    }

    private static void placeholderWork() throws InterruptedException {
        Thread.sleep(10);
    }
}
