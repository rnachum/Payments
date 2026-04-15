package com.example.payment;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "payment_processing_outbox")
public class PaymentProcessingOutbox {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "payment_id", nullable = false)
    private Long paymentId;

    @Column(name = "idempotency_key", nullable = false, length = 255)
    private String idempotencyKey;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, length = 64)
    private OutboxEventType eventType;

    /** JSON; Flyway uses TEXT — avoid @Lob (Hibernate 6 maps String+Lob to TINYTEXT on MySQL). */
    @Column(name = "payload", columnDefinition = "TEXT")
    private String payload;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected PaymentProcessingOutbox() {
    }

    /**
     * Processing-request outbox row (Kafka key = client idempotency key).
     */
    public PaymentProcessingOutbox(Long paymentId, String idempotencyKey) {
        this(paymentId, idempotencyKey, OutboxEventType.PROCESSING_REQUESTED, null);
    }

    /**
     * @param kafkaKey stored in {@code idempotency_key} column (e.g. ledger {@code eventId}).
     */
    public PaymentProcessingOutbox(
            Long paymentId, String kafkaKey, OutboxEventType eventType, String payload) {
        this.paymentId = paymentId;
        this.idempotencyKey = kafkaKey;
        this.eventType = eventType;
        this.payload = payload;
    }

    @PrePersist
    void prePersist() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }

    public Long getId() {
        return id;
    }

    public Long getPaymentId() {
        return paymentId;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public OutboxEventType getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public void setPublishedAt(Instant publishedAt) {
        this.publishedAt = publishedAt;
    }
}
