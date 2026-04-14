package com.example.ledger;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Must stay JSON-compatible with payment-service {@code PaymentCompletedForLedgerEvent} on topic
 * {@code payments.completed}. Dedupe on {@code eventId}.
 */
public record PaymentCompletedForLedgerEvent(
        @JsonProperty("eventId") String eventId,
        @JsonProperty("paymentId") long paymentId,
        @JsonProperty("amount") BigDecimal amount,
        @JsonProperty("currency") String currency,
        @JsonProperty("idempotencyKey") String idempotencyKey,
        @JsonProperty("completedAt") Instant completedAt) {
}
