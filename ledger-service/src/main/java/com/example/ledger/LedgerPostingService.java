package com.example.ledger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;

@Service
public class LedgerPostingService {

    private static final Logger log = LoggerFactory.getLogger(LedgerPostingService.class);

    private final AppliedLedgerEventRepository appliedLedgerEventRepository;
    private final JournalLineRepository journalLineRepository;

    LedgerPostingService(
            AppliedLedgerEventRepository appliedLedgerEventRepository,
            JournalLineRepository journalLineRepository) {
        this.appliedLedgerEventRepository = appliedLedgerEventRepository;
        this.journalLineRepository = journalLineRepository;
    }

    /**
     * Idempotent: same {@code eventId} only applies postings once (unique PK on applied_ledger_events).
     */
    @Transactional
    public void recordPaymentCompleted(PaymentCompletedForLedgerEvent event) {
        var marker = new AppliedLedgerEvent(event.eventId(), event.paymentId(), Instant.now());
        try {
            appliedLedgerEventRepository.saveAndFlush(marker);
        } catch (DataIntegrityViolationException e) {
            log.debug("Skipping duplicate ledger eventId={}", event.eventId());
            return;
        }

        BigDecimal amount = event.amount();
        String ccy = event.currency();
        journalLineRepository.save(new JournalLine(event.eventId(), "PAYMENT_CLEARING", amount, null, ccy));
        journalLineRepository.save(new JournalLine(event.eventId(), "PAYMENT_REVENUE", null, amount, ccy));
        log.info("Booked paymentId={} eventId={} amount={} {}", event.paymentId(), event.eventId(), amount, ccy);
    }
}
