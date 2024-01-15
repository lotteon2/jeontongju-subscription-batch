package com.jeontongju.subscriptionPaymentBatch.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import com.jeontongju.subscriptionPaymentBatch.entity.common.BaseEntity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@Entity
@Table(name = "consumer")
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
public class Consumer extends BaseEntity {

  @Id
  @Column(name = "consumer_id", nullable = false)
  private Long consumerId;

  @Column(name = "email", nullable = false, unique = true)
  private String email;

  @Column(name = "name")
  private String name;

  @Column(name = "point", nullable = false)
  @Builder.Default
  private Long point = 0L;

  @Column(name = "auction_credit", nullable = false)
  @Builder.Default
  private Long auctionCredit = 0L;

  @Column(name = "profile_image_url")
  private String profileImageUrl;

  @Column(name = "phone_number")
  private String phoneNumber;

  @Column(name = "is_default", nullable = false)
  @Builder.Default
  private Boolean isAdult = false;

  @Column(name = "is_regular_payment", nullable = false)
  @Builder.Default
  private Boolean isRegularPayment = false;

  @Column(name = "is_payment_reservation", nullable = false)
  @Builder.Default
  private Boolean isPaymentReservation = false;

  @Column(name = "is_deleted", nullable = false)
  @Builder.Default
  private Boolean isDeleted = false;

  @Column(name = "age")
  private Integer age;

  @OneToMany(mappedBy = "consumer")
  private List<Subscription> subscriptionList;
}
