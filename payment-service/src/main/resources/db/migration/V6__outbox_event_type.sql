-- Support multiple outbox event kinds (processing request vs ledger COMPLETED, etc.).
ALTER TABLE payment_processing_outbox
    ADD COLUMN event_type VARCHAR(64) NOT NULL DEFAULT 'PROCESSING_REQUESTED' AFTER idempotency_key,
    ADD COLUMN payload TEXT NULL AFTER event_type;
