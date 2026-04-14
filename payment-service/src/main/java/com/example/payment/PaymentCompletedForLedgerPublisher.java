package com.example.payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class PaymentCompletedForLedgerPublisher {

    private static final Logger log = LoggerFactory.getLogger(PaymentCompletedForLedgerPublisher.class);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    private final KafkaTemplate<String, PaymentCompletedForLedgerEvent> kafkaTemplate;
    private final PaymentKafkaProperties kafkaProperties;
    private final PaymentMetrics paymentMetrics;

    PaymentCompletedForLedgerPublisher(
            KafkaTemplate<String, PaymentCompletedForLedgerEvent> kafkaTemplate,
            PaymentKafkaProperties kafkaProperties,
            PaymentMetrics paymentMetrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProperties = kafkaProperties;
        this.paymentMetrics = paymentMetrics;
    }

    /**
     * Synchronous send to {@link PaymentKafkaProperties#ledgerCompletedTopic()}; Kafka message key is {@code event.eventId()}.
     */
    public void publish(PaymentCompletedForLedgerEvent event) {
        try {
            kafkaTemplate
                    .send(kafkaProperties.ledgerCompletedTopic(), event.eventId(), event)
                    .get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.info(
                    "Sent payment completed for ledger topic={} paymentId={} eventId={}",
                    kafkaProperties.ledgerCompletedTopic(),
                    event.paymentId(),
                    event.eventId());
            paymentMetrics.incrementLedgerCompletedEventPublished();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException(event.paymentId(), "Ledger event send interrupted for payment " + event.paymentId(), e);
        } catch (ExecutionException | TimeoutException e) {
            throw new KafkaPublishException(event.paymentId(), "Ledger event send failed for payment " + event.paymentId(), e);
        }
    }
}
