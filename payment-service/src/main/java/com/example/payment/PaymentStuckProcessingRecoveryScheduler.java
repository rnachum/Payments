package com.example.payment;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(
        prefix = "payment.processing",
        name = "stuck-recovery-scheduler-enabled",
        havingValue = "true",
        matchIfMissing = true)
public class PaymentStuckProcessingRecoveryScheduler {

    private final PaymentStuckProcessingRecoveryService recoveryService;

    PaymentStuckProcessingRecoveryScheduler(PaymentStuckProcessingRecoveryService recoveryService) {
        this.recoveryService = recoveryService;
    }

    @Scheduled(fixedDelayString = "${payment.processing.stuck-recovery-interval-ms:60000}")
    public void runRecovery() {
        recoveryService.recoverStaleProcessing();
    }
}
