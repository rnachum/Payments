-- Match PaymentStatus enum names used by @Enumerated(EnumType.STRING)
ALTER TABLE payments
    MODIFY COLUMN status ENUM ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED') NOT NULL;
