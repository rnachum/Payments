package com.example.payment;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface PaymentRepository extends JpaRepository<Payment, Long> {

    Optional<Payment> findByIdempotencyKey(String idempotencyKey);

    List<Payment> findByStatusAndUpdatedAtBefore(PaymentStatus status, Instant before);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE Payment p SET p.status = :newStatus, p.updatedAt = :ts WHERE p.id = :id AND p.status = :expectedStatus")
    int updateStatusIfExpected(
            @Param("id") Long id,
            @Param("expectedStatus") PaymentStatus expectedStatus,
            @Param("newStatus") PaymentStatus newStatus,
            @Param("ts") Instant ts);
}
