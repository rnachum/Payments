-- Transactional outbox: enqueue payment processing requests in the same DB transaction as business writes.
CREATE TABLE payment_processing_outbox (
    id BIGINT NOT NULL AUTO_INCREMENT,
    payment_id BIGINT NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    published_at TIMESTAMP(3) NULL,
    PRIMARY KEY (id),
    KEY idx_outbox_unpublished (published_at, id),
    CONSTRAINT fk_outbox_payment FOREIGN KEY (payment_id) REFERENCES payments (id)
);
