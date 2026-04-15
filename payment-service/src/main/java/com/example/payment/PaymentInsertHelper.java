package com.example.payment;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Inserts a payment in a new transaction so a duplicate {@code idempotency_key}
 * rolls back only this transaction and does not mark the caller's transaction rollback-only.
 */
@Component
public class PaymentInsertHelper {

    private final PaymentRepository paymentRepository;

    PaymentInsertHelper(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Payment insert(Payment payment) {
        return paymentRepository.save(payment);
    }
}
