package com.jeontongju.subscriptionPaymentBatch.entity;

import com.jeontongju.subscriptionPaymentBatch.entity.common.BaseEntity;
import com.sun.istack.NotNull;
import io.github.bitbox.bitbox.enums.PaymentMethodEnum;
import io.github.bitbox.bitbox.enums.SubscriptionTypeEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.time.LocalDateTime;

import static javax.persistence.GenerationType.IDENTITY;

@Entity
@Table(name = "subscription")
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
public class Subscription extends BaseEntity {

  @Id
  @GeneratedValue(strategy = IDENTITY)
  @Column(name = "subscription_id")
  private Long subscriptionId;

  @ManyToOne
  @JoinColumn(name = "consumer_id", nullable = false)
  private Consumer consumer;


  @Column(name = "subscription_type", nullable = false)
  @Enumerated(EnumType.STRING)
  @NotNull
  private SubscriptionTypeEnum subscriptionType;

  @Column(name = "payment_amount", nullable = false)
  @NotNull
  private Long paymentAmount;

  @Column(name = "start_date", nullable = false)
  @NotNull
  private LocalDateTime startDate;

  @Column(name = "end_date", nullable = false)
  @NotNull
  private LocalDateTime endDate;

  @Column(name = "payment_type", nullable = false)
  @Enumerated(EnumType.STRING)
  @NotNull
  private PaymentMethodEnum paymentMethod;

  public void addSubscriptionTime() {
    this.endDate = this.endDate.plusDays(30);
  }
}