package com.example.payment;

/**
 * Domain exception: duplicate {@code idempotency_key} on insert, but the winning row was not visible after retries.
 * Mapped to HTTP 503 with {@code retryable: true} by {@link GlobalExceptionHandler}.
 */
public class IdempotentPaymentNotFoundAfterDuplicateException extends RuntimeException {

    private final String idempotencyKey;    

    public IdempotentPaymentNotFoundAfterDuplicateException(String idempotencyKey) {
        super(
                "Duplicate key conflict for idempotency key, but payment row was not visible after retries. key="
                        + idempotencyKey);
        this.idempotencyKey = idempotencyKey;
    }

    public IdempotentPaymentNotFoundAfterDuplicateException(String idempotencyKey, Throwable cause) {
        super(
                "Interrupted while reloading payment after duplicate key conflict. key=" + idempotencyKey,
                cause);
        this.idempotencyKey = idempotencyKey;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }
}
