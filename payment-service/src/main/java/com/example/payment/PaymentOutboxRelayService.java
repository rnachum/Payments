package com.example.payment;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;

/**
 * Drains {@link PaymentProcessingOutbox} rows: each publish runs in {@code REQUIRES_NEW} so one failure
 * does not roll back other successful publishes in the same batch.
 */
@Service
public class PaymentOutboxRelayService {

    private static final Logger log = LoggerFactory.getLogger(PaymentOutboxRelayService.class);

    private final PaymentProcessingOutboxRepository outboxRepository;
    private final PaymentProcessingRequestPublisher processingPublisher;
    private final PaymentCompletedForLedgerPublisher ledgerPublisher;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate requiresNewTx;

    PaymentOutboxRelayService(
            PaymentProcessingOutboxRepository outboxRepository,
            PaymentProcessingRequestPublisher processingPublisher,
            PaymentCompletedForLedgerPublisher ledgerPublisher,
            ObjectMapper objectMapper,
            PlatformTransactionManager transactionManager) {
        this.outboxRepository = outboxRepository;
        this.processingPublisher = processingPublisher;
        this.ledgerPublisher = ledgerPublisher;
        this.objectMapper = objectMapper;
        this.requiresNewTx = new TransactionTemplate(transactionManager);
        this.requiresNewTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    /**
     * Publishes up to 50 unpublished outbox rows. Safe to call from a scheduler or tests.
     */
    public void relayPendingBatch() {
        List<PaymentProcessingOutbox> batch = outboxRepository.findTop50ByPublishedAtIsNullOrderByIdAsc();
        for (PaymentProcessingOutbox row : batch) {
            requiresNewTx.executeWithoutResult(status -> relayOne(row.getId()));
        }
    }

    private void relayOne(Long outboxId) {
        PaymentProcessingOutbox row = outboxRepository.findById(outboxId).orElse(null);
        if (row == null || row.getPublishedAt() != null) {
            return;
        }
        try {
            switch (row.getEventType()) {
                case PROCESSING_REQUESTED -> processingPublisher.publishProcessingRequested(
                        row.getPaymentId(), row.getIdempotencyKey());
                case PAYMENT_COMPLETED_FOR_LEDGER -> {
                    if (row.getPayload() == null || row.getPayload().isBlank()) {
                        log.warn("Outbox row {} missing payload for ledger event; skipping publish", outboxId);
                        return;
                    }
                    PaymentCompletedForLedgerEvent event =
                            objectMapper.readValue(row.getPayload(), PaymentCompletedForLedgerEvent.class);
                    ledgerPublisher.publish(event);
                }
            }
            row.setPublishedAt(Instant.now());
            outboxRepository.save(row);
        } catch (KafkaPublishException e) {
            log.warn(
                    "Outbox relay deferred paymentId={} outboxId={}: {}",
                    row.getPaymentId(),
                    outboxId,
                    e.getMessage());
        } catch (Exception e) {
            log.warn("Outbox relay failed outboxId={} eventType={}: {}", outboxId, row.getEventType(), e.toString());
        }
    }
}
