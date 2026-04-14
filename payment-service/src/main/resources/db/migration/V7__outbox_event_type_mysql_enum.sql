-- Align with @Enumerated(EnumType.STRING) + Hibernate MySQL validation (same pattern as V3 payments.status).
ALTER TABLE payment_processing_outbox
    MODIFY COLUMN event_type ENUM ('PROCESSING_REQUESTED', 'PAYMENT_COMPLETED_FOR_LEDGER') NOT NULL DEFAULT 'PROCESSING_REQUESTED';
