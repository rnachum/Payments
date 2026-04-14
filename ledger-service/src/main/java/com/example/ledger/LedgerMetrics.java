package com.example.ledger;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class LedgerMetrics {

    private final Counter paymentCompletedConsumerFailedDelivery;
    private final Counter paymentCompletedConsumerDltPublished;

    LedgerMetrics(MeterRegistry registry) {
        this.paymentCompletedConsumerFailedDelivery =
                Counter.builder("ledger.payments.completed.consumer.failed_delivery")
                        .description("Payment-completed listener failures (each failed attempt incl. retries)")
                        .register(registry);
        this.paymentCompletedConsumerDltPublished =
                Counter.builder("ledger.payments.completed.consumer.dlt_published")
                        .description("Records sent to payments-completed DLT after retries exhausted")
                        .register(registry);
    }

    public void incrementPaymentCompletedConsumerFailedDelivery() {
        paymentCompletedConsumerFailedDelivery.increment();
    }

    public void incrementPaymentCompletedConsumerDltPublished() {
        paymentCompletedConsumerDltPublished.increment();
    }
}
