CREATE TABLE applied_ledger_events (
    id BIGINT NOT NULL AUTO_INCREMENT,
    event_id VARCHAR(64) NOT NULL,
    payment_id BIGINT NOT NULL,
    received_at TIMESTAMP(3) NOT NULL,
    CONSTRAINT pk_applied_ledger_events PRIMARY KEY (id),
    CONSTRAINT uk_applied_ledger_events_event_id UNIQUE (event_id)
);

CREATE TABLE journal_lines (
    id BIGINT NOT NULL AUTO_INCREMENT,
    event_id VARCHAR(64) NOT NULL,
    account_code VARCHAR(64) NOT NULL,
    debit_amount DECIMAL(19, 4),
    credit_amount DECIMAL(19, 4),
    currency VARCHAR(3) NOT NULL,
    CONSTRAINT pk_journal_lines PRIMARY KEY (id)
);

CREATE INDEX idx_journal_lines_event_id ON journal_lines (event_id);
