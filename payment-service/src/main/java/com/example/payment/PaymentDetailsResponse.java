package com.example.payment;

import java.math.BigDecimal;

public record PaymentDetailsResponse(Long id, BigDecimal amount, String currency, PaymentStatus status) {

    static PaymentDetailsResponse from(Payment payment) {
        return new PaymentDetailsResponse(
                payment.getId(), payment.getAmount(), payment.getCurrency(), payment.getStatus());
    }
}
