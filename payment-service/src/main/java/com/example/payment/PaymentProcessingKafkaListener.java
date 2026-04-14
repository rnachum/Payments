package com.example.payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentProcessingKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessingKafkaListener.class);

    private final PaymentProcessingService paymentProcessingService;
    private final PaymentMetrics paymentMetrics;

    PaymentProcessingKafkaListener(
            PaymentProcessingService paymentProcessingService, PaymentMetrics paymentMetrics) {
        this.paymentProcessingService = paymentProcessingService;
        this.paymentMetrics = paymentMetrics;
    }

    @KafkaListener(
            topics = "${payment.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void onProcessingRequested(PaymentProcessingRequested message) {
        paymentMetrics.incrementProcessingRequestConsumed();
        log.info(
                "Received payment processing request event paymentId={} kafkaKeyPresent={}",
                message.paymentId(),
                message.idempotencyKey() != null && !message.idempotencyKey().isEmpty());
        paymentProcessingService.processPaymentRequest(message.paymentId());
    }
}
