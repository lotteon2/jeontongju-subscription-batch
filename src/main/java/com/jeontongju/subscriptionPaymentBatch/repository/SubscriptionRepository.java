package com.jeontongju.subscriptionPaymentBatch.repository;

import com.jeontongju.subscriptionPaymentBatch.entity.Subscription;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SubscriptionRepository extends JpaRepository<Subscription, Long> {
}
