package com.example.payment;

/**
 * Publishing the processing request to Kafka failed after the payment row was committed.
 * Mapped to HTTP 503 by {@link GlobalExceptionHandler}.
 */
public class KafkaPublishException extends RuntimeException {

    private final long paymentId;

    public KafkaPublishException(long paymentId, String message, Throwable cause) {
        super(message, cause);
        this.paymentId = paymentId;
    }

    public long getPaymentId() {
        return paymentId;
    }
}
