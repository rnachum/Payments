-- Initial payments table. status is VARCHAR until V3 converts to MySQL ENUM.
CREATE TABLE payments (
    id BIGINT NOT NULL AUTO_INCREMENT,
    amount DECIMAL(19, 4) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    status VARCHAR(32) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_payments_idempotency_key (idempotency_key)
);
