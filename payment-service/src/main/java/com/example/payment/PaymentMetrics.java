package com.example.payment;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class PaymentMetrics {

    private final Counter paymentsCreated;
    private final Counter processingRequestsPublished;
    private final Counter processingRequestsConsumed;
    private final Counter ledgerCompletedEventsPublished;
    private final Counter processingRequestConsumerFailedDelivery;
    private final Counter processingRequestConsumerDltPublished;

    PaymentMetrics(MeterRegistry registry) {
        this.paymentsCreated = Counter.builder("payments.created")
                .description("New payment rows persisted (excludes idempotent duplicate hits)")
                .register(registry);
        this.processingRequestsPublished = Counter.builder("payments.processing.request.published")
                .description("Kafka sends of payment processing request events that completed successfully")
                .register(registry);
        this.processingRequestsConsumed = Counter.builder("payments.processing.request.consumed")
                .description("Kafka deliveries of payment processing request events handled by the listener")
                .register(registry);
        this.ledgerCompletedEventsPublished = Counter.builder("payments.ledger.completed.event.published")
                .description("Kafka sends of payment-completed events for the internal ledger")
                .register(registry);
        this.processingRequestConsumerFailedDelivery =
                Counter.builder("payments.processing.request.consumer.failed_delivery")
                        .description("Processing-request listener failures (each failed attempt incl. retries)")
                        .register(registry);
        this.processingRequestConsumerDltPublished =
                Counter.builder("payments.processing.request.consumer.dlt_published")
                        .description("Records sent to processing-request DLT after retries exhausted")
                        .register(registry);
    }

    public void incrementPaymentsCreated() {
        paymentsCreated.increment();
    }

    public void incrementProcessingRequestPublished() {
        processingRequestsPublished.increment();
    }

    public void incrementProcessingRequestConsumed() {
        processingRequestsConsumed.increment();
    }

    public void incrementLedgerCompletedEventPublished() {
        ledgerCompletedEventsPublished.increment();
    }

    public void incrementProcessingRequestConsumerFailedDelivery() {
        processingRequestConsumerFailedDelivery.increment();
    }

    public void incrementProcessingRequestConsumerDltPublished() {
        processingRequestConsumerDltPublished.increment();
    }
}
