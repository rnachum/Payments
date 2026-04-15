package com.example.payment;

import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.util.LinkedHashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final String RETRY_AFTER_SECONDS = "3";

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(MethodArgumentNotValidException ex) {
        Map<String, String> fieldErrors = new LinkedHashMap<>();
        ex.getBindingResult()
                .getFieldErrors()
                .forEach(fe -> fieldErrors.put(fe.getField(), fe.getDefaultMessage()));
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", "VALIDATION_FAILED");
        body.put("fieldErrors", fieldErrors);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
    }

    @ExceptionHandler(KafkaPublishException.class)
    public ResponseEntity<Map<String, Object>> handleKafkaPublish(KafkaPublishException ex) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", "KAFKA_UNAVAILABLE");
        body.put(
                "message",
                "Could not publish payment processing event; retry later with the same Idempotency-Key.");
        body.put("retryable", true);
        body.put("paymentId", ex.getPaymentId());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS)
                .body(body);
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    public ResponseEntity<Map<String, String>> handleMissingRequestHeader(MissingRequestHeaderException ex) {
        String message = "Idempotency-Key".equalsIgnoreCase(ex.getHeaderName())
                ? "Idempotency-Key header is required"
                : ex.getHeaderName() + " header is required";
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", message));
    }

    @ExceptionHandler(IdempotentPaymentNotFoundAfterDuplicateException.class)
    public ResponseEntity<Map<String, Object>> handleIdempotentPaymentNotFound(
            IdempotentPaymentNotFoundAfterDuplicateException ex) {
        return retryableServiceUnavailable(
                "IDEMPOTENCY_CONFIRMATION_PENDING",
                "Could not load payment after duplicate key; retry the same request with the same Idempotency-Key.",
                ex.getIdempotencyKey());
    }

    @ExceptionHandler(CannotCreateTransactionException.class)
    public ResponseEntity<Map<String, Object>> handleCannotCreateTransaction(CannotCreateTransactionException ex) {
        if (isTransientInfrastructure(ex)) {
            return retryableServiceUnavailable(
                    "DATABASE_TEMPORARILY_UNAVAILABLE",
                    "Database connection could not be acquired; retry later with the same Idempotency-Key.",
                    null);
        }
        return retryableServiceUnavailable(
                "TRANSACTION_UNAVAILABLE",
                "Could not start database transaction; retry later with the same Idempotency-Key.",
                null);
    }

    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<Map<String, Object>> handleDataAccess(DataAccessException ex) {
        if (isTransientInfrastructure(ex)) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of(
                        "error", "DATABASE_TEMPORARILY_UNAVAILABLE",
                        "message", "Temporary database issue. Please retry later."
                ));
        }
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(Map.of(
                    "error", "DATABASE_ERROR",
                    "message", "Unexpected database error"
            ));
    }

    private static ResponseEntity<Map<String, Object>> retryableServiceUnavailable(
            String code, String message, String idempotencyKey) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", code);
        body.put("message", message);
        body.put("retryable", true);
        if (idempotencyKey != null) {
            body.put("idempotencyKey", idempotencyKey);
        }
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS)
                .body(body);
    }

    private static boolean isTransientInfrastructure(Throwable throwable) {
        for (Throwable t = throwable; t != null; t = t.getCause()) {
            if (t instanceof SQLTransientConnectionException || t instanceof SQLTimeoutException) {
                return true;
            }
            String msg = t.getMessage();
            if (msg != null) {
                if (msg.contains("Connection is not available")) {
                    return true;
                }
                if (msg.contains("HikariPool") && msg.contains("timeout")) {
                    return true;
                }
            }
        }
        return false;
    }
}
