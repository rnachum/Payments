package com.example.ledger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

@Component
class LedgerKafkaErrorHandlerFactory {

    private final CommonErrorHandler paymentCompletedErrorHandler;

    LedgerKafkaErrorHandlerFactory(
            @Qualifier("kafkaTemplate") KafkaTemplate<?, ?> kafkaTemplate,
            LedgerMetrics ledgerMetrics,
            @Value("${ledger.kafka.payments-completed-dlt-topic}") String dltTopic) {

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
                ledgerMetrics.incrementPaymentCompletedConsumerFailedDelivery();
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                ledgerMetrics.incrementPaymentCompletedConsumerDltPublished();
            }
        });

        this.paymentCompletedErrorHandler = handler;
    }

    CommonErrorHandler paymentCompletedErrorHandler() {
        return paymentCompletedErrorHandler;
    }
}
