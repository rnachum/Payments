package com.example.payment;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Emitted to the ledger topic when a payment becomes COMPLETED. Ledger should dedupe on {@code eventId}.
 */
public record PaymentCompletedForLedgerEvent(
        @JsonProperty("eventId") String eventId,
        @JsonProperty("paymentId") long paymentId,
        @JsonProperty("amount") BigDecimal amount,
        @JsonProperty("currency") String currency,
        @JsonProperty("idempotencyKey") String idempotencyKey,
        @JsonProperty("completedAt") Instant completedAt) {
}
