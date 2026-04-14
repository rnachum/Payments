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
public class PaymentProcessingRequestPublisher {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessingRequestPublisher.class);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    private final KafkaTemplate<String, PaymentProcessingRequested> kafkaTemplate;
    private final PaymentKafkaProperties kafkaProperties;
    private final PaymentMetrics paymentMetrics;

    PaymentProcessingRequestPublisher(
            KafkaTemplate<String, PaymentProcessingRequested> kafkaTemplate,
            PaymentKafkaProperties kafkaProperties,
            PaymentMetrics paymentMetrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProperties = kafkaProperties;
        this.paymentMetrics = paymentMetrics;
    }

    /**
     * Synchronous send; fails fast if the broker cannot accept within {@link #SEND_TIMEOUT}.
     */
    public void publishProcessingRequested(long paymentId, String idempotencyKey) {
        var message = new PaymentProcessingRequested(paymentId, idempotencyKey);
        try {
            kafkaTemplate
                    .send(kafkaProperties.topic(), idempotencyKey, message)
                    .get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.info(
                    "Sent payment processing request event topic={} paymentId={} kafkaKeyPresent={}",
                    kafkaProperties.topic(),
                    paymentId,
                    idempotencyKey != null && !idempotencyKey.isEmpty());
            paymentMetrics.incrementProcessingRequestPublished();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException(paymentId, "Kafka send interrupted for payment " + paymentId, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new KafkaPublishException(paymentId, "Kafka send failed for payment " + paymentId, e);
        }
    }
}
