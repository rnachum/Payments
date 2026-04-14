package com.example.ledger;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface AppliedLedgerEventRepository extends JpaRepository<AppliedLedgerEvent, Long> {

    Optional<AppliedLedgerEvent> findByEventId(String eventId);
}
