package com.jeontongju.subscriptionPaymentBatch.entity;

import static javax.persistence.GenerationType.IDENTITY;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import com.jeontongju.subscriptionPaymentBatch.entity.common.BaseEntity;
import com.sun.istack.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "subscription_kakao")
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
public class SubscriptionKakao extends BaseEntity {

  @Id
  @GeneratedValue(strategy = IDENTITY)
  @Column(name = "kakao_subscription_id")
  private Long kakaoSubscriptionId;

  @Column(name = "kakao_sid", nullable = false)
  @NotNull
  private String kakaoSid;

  @Column(name = "kakao_store_code", nullable = false)
  @NotNull
  private String kakaoStoreCode;

  @Column(name = "kakao_order_id", nullable = false)
  @NotNull
  private String kakaoOrderId;

  @OneToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "subscription_id")
  @NotNull
  private Subscription subscription;
}
