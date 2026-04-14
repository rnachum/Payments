package com.example.payment;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "payment.kafka")
public record PaymentKafkaProperties(
        @DefaultValue("payments.processing.requested") String topic,
        @DefaultValue("payments.completed") String ledgerCompletedTopic
) {
}
