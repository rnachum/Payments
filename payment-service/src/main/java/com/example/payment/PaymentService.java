package com.example.payment;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentProcessingOutboxRepository processingOutboxRepository;
    private final PaymentMetrics paymentMetrics;

    PaymentService(
            PaymentRepository paymentRepository,
            PaymentProcessingOutboxRepository processingOutboxRepository,
            PaymentMetrics paymentMetrics) {
        this.paymentRepository = paymentRepository;
        this.processingOutboxRepository = processingOutboxRepository;
        this.paymentMetrics = paymentMetrics;
    }

    @Transactional
    public AcceptedPaymentResponse createPayment(CreatePaymentRequest request, String idempotencyKey) {
        Optional<Payment> existing = paymentRepository.findByIdempotencyKey(idempotencyKey);
        if (existing.isPresent()) {
            Payment p = existing.get();
            return new AcceptedPaymentResponse(p.getId(), p.getStatus());
        }

        Payment payment = new Payment();
        payment.setAmount(request.getAmount());
        payment.setCurrency(request.getCurrency());
        payment.setStatus(PaymentStatus.PENDING);
        payment.setIdempotencyKey(idempotencyKey);

        try {
            payment = paymentRepository.save(payment);
            paymentMetrics.incrementPaymentsCreated();
            processingOutboxRepository.save(new PaymentProcessingOutbox(payment.getId(), idempotencyKey));
        } catch (DataIntegrityViolationException ex) {
            Payment reloaded = paymentRepository
                    .findByIdempotencyKey(idempotencyKey)
                    .orElseThrow(() -> new IdempotentPaymentNotFoundAfterDuplicateException(idempotencyKey, ex));
            return new AcceptedPaymentResponse(reloaded.getId(), reloaded.getStatus());
        }

        return new AcceptedPaymentResponse(payment.getId(), payment.getStatus());
    }

    @Transactional(readOnly = true)
    public Optional<PaymentDetailsResponse> getPayment(Long id) {
        return paymentRepository.findById(id).map(PaymentDetailsResponse::from);
    }
}
