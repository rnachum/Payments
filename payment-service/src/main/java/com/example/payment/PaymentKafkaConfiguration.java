package com.example.payment;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class PaymentKafkaConfiguration {

    /**
     * Boot skips auto-configured {@code kafkaTemplate} when other {@link KafkaTemplate} beans exist;
     * DLT / error handler needs this bean name for {@link org.springframework.beans.factory.annotation.Qualifier}.
     */
    @Bean(name = "kafkaTemplate")
    KafkaTemplate<Object, Object> kafkaTemplate(ProducerFactory<Object, Object> kafkaProducerFactory) {
        return new KafkaTemplate<>(kafkaProducerFactory);
    }

    @Bean
    @SuppressWarnings("unchecked")
    KafkaTemplate<String, PaymentProcessingRequested> paymentProcessingKafkaTemplate(
            ProducerFactory<Object, Object> kafkaProducerFactory) {
        ProducerFactory<String, PaymentProcessingRequested> pf =
                (ProducerFactory<String, PaymentProcessingRequested>) (Object) kafkaProducerFactory;
        return new KafkaTemplate<>(pf);
    }

    @Bean
    @SuppressWarnings("unchecked")
    KafkaTemplate<String, PaymentCompletedForLedgerEvent> paymentLedgerCompletedKafkaTemplate(
            ProducerFactory<Object, Object> kafkaProducerFactory) {
        ProducerFactory<String, PaymentCompletedForLedgerEvent> pf =
                (ProducerFactory<String, PaymentCompletedForLedgerEvent>) (Object) kafkaProducerFactory;
        return new KafkaTemplate<>(pf);
    }
}
