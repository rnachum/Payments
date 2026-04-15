package com.example.payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;

@Service
public class PaymentStuckProcessingRecoveryService {

    private static final Logger log = LoggerFactory.getLogger(PaymentStuckProcessingRecoveryService.class);

    private final PaymentRepository paymentRepository;
    private final PaymentProcessingOutboxRepository processingOutboxRepository;
    private final Duration stuckThreshold;

    PaymentStuckProcessingRecoveryService(
            PaymentRepository paymentRepository,
            PaymentProcessingOutboxRepository processingOutboxRepository,
            @Value("${payment.processing.stuck-threshold:PT5M}") Duration stuckThreshold) {
        this.paymentRepository = paymentRepository;
        this.processingOutboxRepository = processingOutboxRepository;
        this.stuckThreshold = stuckThreshold;
    }

    @Transactional
    public void recoverStaleProcessing() {
        Instant now = Instant.now();
        Instant staleBefore = now.minus(stuckThreshold);
        for (Payment p : paymentRepository.findByStatusAndUpdatedAtBefore(PaymentStatus.PROCESSING, staleBefore)) {
            int n = paymentRepository.updateStatusIfExpected(
                    p.getId(), PaymentStatus.PROCESSING, PaymentStatus.PENDING, now);
            if (n == 1) {
                processingOutboxRepository.save(new PaymentProcessingOutbox(p.getId(), p.getIdempotencyKey()));
                log.info(
                        "Recovered stale PROCESSING paymentId={} reset to PENDING; processing request enqueued to outbox",
                        p.getId());
            }
        }
    }
}
