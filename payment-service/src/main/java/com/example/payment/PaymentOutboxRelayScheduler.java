package com.example.payment;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "payment.outbox", name = "relay-enabled", havingValue = "true", matchIfMissing = true)
public class PaymentOutboxRelayScheduler {

    private final PaymentOutboxRelayService relayService;

    PaymentOutboxRelayScheduler(PaymentOutboxRelayService relayService) {
        this.relayService = relayService;
    }

    @Scheduled(fixedDelayString = "${payment.outbox.relay-interval-ms:1000}")
    public void relayTick() {
        relayService.relayPendingBatch();
    }
}
