package com.example.payment;

/**
 * Published when a payment row is ready for asynchronous processing.
 *
 * @param paymentId        persisted payment id
 * @param idempotencyKey   optional; useful for log correlation when present
 */
public record PaymentProcessingRequested(Long paymentId, String idempotencyKey) {
}
