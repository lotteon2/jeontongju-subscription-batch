package com.jeontongju.subscriptionPaymentBatch.repository;

import com.jeontongju.subscriptionPaymentBatch.entity.Subscription;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface SubscriptionRepository extends JpaRepository<Subscription, Long> {
    @Query("SELECT s FROM Subscription s " +
            "JOIN FETCH s.consumer c " +
            "LEFT JOIN FETCH Subscription s2 ON s.consumer.consumerId = s2.consumer.consumerId AND s.endDate < s2.endDate " +
            "WHERE s2.consumer.consumerId IS NULL " +
            "AND c.consumerId IN :consumerIds " +
            "AND REPLACE(SUBSTRING(s.endDate, 1, 10), '-', '') = :date")
    List<Subscription> findSubscriptionsByConsumerIdsAndEndDate(List<Long> consumerIds, String date);
}
