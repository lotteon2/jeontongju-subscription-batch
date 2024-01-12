package com.jeontongju.subscriptionPaymentBatch.repository;

import com.jeontongju.subscriptionPaymentBatch.entity.Consumer;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface ConsumerRepository extends JpaRepository<Consumer, Long> {
    @Modifying
    @Query("UPDATE Consumer c SET c.isRegularPayment = false WHERE c.consumerId IN :consumerIds")
    void updateConsumerGrade(List<? extends Long> consumerIds);
}
