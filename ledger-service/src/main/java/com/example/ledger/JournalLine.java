package com.example.ledger;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;

@Entity
@Table(name = "journal_lines")
public class JournalLine {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, length = 64)
    private String eventId;

    @Column(name = "account_code", nullable = false, length = 64)
    private String accountCode;

    @Column(name = "debit_amount", precision = 19, scale = 4)
    private BigDecimal debitAmount;

    @Column(name = "credit_amount", precision = 19, scale = 4)
    private BigDecimal creditAmount;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    protected JournalLine() {
    }

    public JournalLine(String eventId, String accountCode, BigDecimal debitAmount, BigDecimal creditAmount, String currency) {
        this.eventId = eventId;
        this.accountCode = accountCode;
        this.debitAmount = debitAmount;
        this.creditAmount = creditAmount;
        this.currency = currency;
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getAccountCode() {
        return accountCode;
    }

    public BigDecimal getDebitAmount() {
        return debitAmount;
    }

    public BigDecimal getCreditAmount() {
        return creditAmount;
    }

    public String getCurrency() {
        return currency;
    }
}
