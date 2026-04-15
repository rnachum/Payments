package com.example.ledger;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentCompletedKafkaListener {

    private final LedgerPostingService ledgerPostingService;

    PaymentCompletedKafkaListener(LedgerPostingService ledgerPostingService) {
        this.ledgerPostingService = ledgerPostingService;
    }

    @KafkaListener(
            topics = "${ledger.kafka.payments-completed-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "paymentCompletedKafkaListenerContainerFactory")
    public void onPaymentCompleted(PaymentCompletedForLedgerEvent event) {
        ledgerPostingService.recordPaymentCompleted(event);
    }
}
