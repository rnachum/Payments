package com.example.ledger;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import java.time.Instant;

@Entity
@Table(
        name = "applied_ledger_events",
        uniqueConstraints = @UniqueConstraint(name = "uk_applied_ledger_events_event_id", columnNames = "event_id"))
public class AppliedLedgerEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, length = 64, unique = true)
    private String eventId;

    @Column(name = "payment_id", nullable = false)
    private Long paymentId;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    protected AppliedLedgerEvent() {
    }

    public AppliedLedgerEvent(String eventId, Long paymentId, Instant receivedAt) {
        this.eventId = eventId;
        this.paymentId = paymentId;
        this.receivedAt = receivedAt;
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public Long getPaymentId() {
        return paymentId;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }
}
