package com.jeontongju.subscriptionPaymentBatch.batch.repository;

import com.jeontongju.subscriptionPaymentBatch.entity.Consumer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ConsumerRepository extends JpaRepository<Consumer, Long> {
}
