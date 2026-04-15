-- Map legacy string statuses into the new PaymentStatus enum set
UPDATE payments SET status = 'PENDING' WHERE status = 'CREATED';

