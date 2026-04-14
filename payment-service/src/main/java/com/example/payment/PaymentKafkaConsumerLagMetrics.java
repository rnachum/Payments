package com.example.payment;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Publishes {@code payment.kafka.consumer.lag.total} (sum of end offset minus committed offset per partition).
 * Disable with {@code payment.actuator.consumer-lag-metrics=false} or turn off Kafka health similarly for deploys without broker.
 */
@Component
@ConditionalOnProperty(prefix = "payment.actuator", name = "consumer-lag-metrics", havingValue = "true")
public class PaymentKafkaConsumerLagMetrics {

    private static final Logger log = LoggerFactory.getLogger(PaymentKafkaConsumerLagMetrics.class);
    private static final int ADMIN_TIMEOUT_SEC = 10;

    private final MeterRegistry registry;
    private final KafkaAdmin kafkaAdmin;
    private final String groupId;
    private final String topic;
    private final AtomicReference<Double> totalLag = new AtomicReference<>(0.0);

    PaymentKafkaConsumerLagMetrics(
            MeterRegistry registry,
            KafkaAdmin kafkaAdmin,
            @Value("${spring.kafka.consumer.group-id}") String groupId,
            @Value("${payment.kafka.topic}") String topic) {
        this.registry = registry;
        this.kafkaAdmin = kafkaAdmin;
        this.groupId = groupId;
        this.topic = topic;
    }

    @PostConstruct
    void registerGauge() {
        Gauge.builder("payment.kafka.consumer.lag.total", totalLag, AtomicReference::get)
                .description("Approximate total consumer lag (messages) for the payment processing topic and consumer group")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${payment.actuator.lag-poll-interval-ms:30000}", initialDelayString = "15000")
    void refreshLag() {
        Map<String, Object> config = new HashMap<>(kafkaAdmin.getConfigurationProperties());
        try (AdminClient admin = AdminClient.create(config)) {
            Map<String, TopicDescription> descriptions =
                    admin.describeTopics(java.util.List.of(topic)).all().get(ADMIN_TIMEOUT_SEC, TimeUnit.SECONDS);
            TopicDescription td = descriptions.get(topic);
            if (td == null) {
                totalLag.set(0.0);
                return;
            }
            var partitions = td.partitions().stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .toList();

            if (partitions.isEmpty()) {
                totalLag.set(0.0);
                return;
            }

            Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
            for (TopicPartition tp : partitions) {
                latest.put(tp, OffsetSpec.latest());
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                    admin.listOffsets(latest).all().get(ADMIN_TIMEOUT_SEC, TimeUnit.SECONDS);

            Map<TopicPartition, Long> committed = new HashMap<>();
            try {
                var groupOffsets = admin.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata()
                        .get(ADMIN_TIMEOUT_SEC, TimeUnit.SECONDS);
                groupOffsets.forEach((tp, meta) -> committed.put(tp, meta.offset()));
            } catch (ExecutionException e) {
                log.debug("Could not read consumer group offsets for groupId={}: {}", groupId, e.getMessage());
            }

            double sum = 0.0;
            for (TopicPartition tp : partitions) {
                var endInfo = endOffsets.get(tp);
                if (endInfo == null) {
                    continue;
                }
                long end = endInfo.offset();
                long current = committed.getOrDefault(tp, 0L);
                sum += Math.max(0L, end - current);
            }
            totalLag.set(sum);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Lag refresh interrupted", e);
        } catch (ExecutionException | TimeoutException e) {
            log.debug("Lag refresh failed: {}", e.toString());
        }
    }
}
