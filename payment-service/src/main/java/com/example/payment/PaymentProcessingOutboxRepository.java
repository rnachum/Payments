package com.example.payment;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface PaymentProcessingOutboxRepository extends JpaRepository<PaymentProcessingOutbox, Long> {

    List<PaymentProcessingOutbox> findTop50ByPublishedAtIsNullOrderByIdAsc();
}
