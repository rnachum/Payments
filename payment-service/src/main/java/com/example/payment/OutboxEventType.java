package com.example.payment;

/**
 * Kind of message stored in {@code payment_processing_outbox}; relay routes by this value.
 */
public enum OutboxEventType {
    /** Initial async processing request (existing flow). */
    PROCESSING_REQUESTED,
    /** Payment reached COMPLETED; internal ledger consumes this from {@code payment.kafka.ledger-completed-topic}. */
    PAYMENT_COMPLETED_FOR_LEDGER
}
