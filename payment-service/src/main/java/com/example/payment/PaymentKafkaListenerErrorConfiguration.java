package com.example.payment;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Retries transient listener failures, then publishes to DLT. Applied via {@link CommonErrorHandler}
 * to Spring Boot's auto-configured {@code kafkaListenerContainerFactory}.
 */
@Configuration
public class PaymentKafkaListenerErrorConfiguration {

    @Bean
    CommonErrorHandler paymentProcessingKafkaErrorHandler(
            @Qualifier("kafkaTemplate") KafkaTemplate<?, ?> kafkaTemplate,
            PaymentMetrics paymentMetrics,
            @Value("${payment.kafka.processing-requested-dlt-topic}") String dltTopic) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (ConsumerRecord<?, ?> record, Exception ex) -> new TopicPartition(
                        dltTopic, record.partition() >= 0 ? record.partition() : 0));

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
        handler.setCommitRecovered(true);
        handler.addNotRetryableExceptions(
                SerializationException.class,
                RecordDeserializationException.class,
                MessageConversionException.class);

        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                paymentMetrics.incrementProcessingRequestConsumerFailedDelivery();
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                paymentMetrics.incrementProcessingRequestConsumerDltPublished();
            }
        });

        return handler;
    }
}
