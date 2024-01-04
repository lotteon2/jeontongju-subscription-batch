package com.jeontongju.subscriptionPaymentBatch.batch;

import com.jeontongju.TestBatchConfig;
import com.jeontongju.subscriptionPaymentBatch.entity.Consumer;
import com.jeontongju.subscriptionPaymentBatch.entity.Subscription;
import com.jeontongju.subscriptionPaymentBatch.repository.ConsumerRepository;
import com.jeontongju.subscriptionPaymentBatch.repository.SubscriptionRepository;
import io.github.bitbox.bitbox.enums.PaymentMethodEnum;
import io.github.bitbox.bitbox.enums.SubscriptionTypeEnum;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBatchTest
@SpringBootTest(classes = {ConsumerGradeUpdateBatch.class, TestBatchConfig.class})
public class ConsumerBatchTest {
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private ConsumerRepository consumerRepository;
    @Autowired
    private SubscriptionRepository subscriptionRepository;

    @AfterEach
    public void deleteData(){
        subscriptionRepository.deleteAll();
        consumerRepository.deleteAll();
    }

    @Test
    public void 삭제된_유저는_갱신되지_않는다() throws Exception {
        LocalDateTime startTime = LocalDateTime.parse("2023-12-07 01:49:03", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime endTime = LocalDateTime.parse("2024-01-04 01:49:03", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        Consumer consumer1 = Consumer.builder()
                .consumerId(1L).email("seonghun7304@naver.com").name("최성훈").point(1L).auctionCredit(1L).profileImageUrl("test")
                .phoneNumber("01012345678").isFirstLogin(false).isAdult(false).isRegularPayment(true).isPaymentReservation(false).isDeleted(false).build();
        Consumer consumer2 = Consumer.builder()
                .consumerId(2L).email("seonghun7305@naver.com").name("최성훈2").point(12L).auctionCredit(12L).profileImageUrl("test2")
                .phoneNumber("01012345679").isFirstLogin(false).isAdult(false).isRegularPayment(true).isPaymentReservation(false).isDeleted(true).build();
        Subscription subscription1 = Subscription.builder().consumer(consumer1).subscriptionType(SubscriptionTypeEnum.YANGBAN)
                .paymentAmount(3900L).startDate(startTime).endDate(endTime).paymentMethod(PaymentMethodEnum.KAKAO).build();
        Subscription subscription2 = Subscription.builder().consumer(consumer2).subscriptionType(SubscriptionTypeEnum.YANGBAN)
                .paymentAmount(3900L).startDate(startTime).endDate(endTime).paymentMethod(PaymentMethodEnum.KAKAO).build();
        consumerRepository.save(consumer1);
        consumerRepository.save(consumer2);
        subscriptionRepository.save(subscription1);
        subscriptionRepository.save(subscription2);

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("date", "20240104")
                .addLong("version",1L)
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertEquals(consumerRepository.findById(1L).orElseThrow(RuntimeException::new).getIsRegularPayment(), false);
        assertEquals(consumerRepository.findById(2L).orElseThrow(RuntimeException::new).getIsRegularPayment(), true);
    }

    @Test
    public void 배치를_돌리는날과_다르다면_갱신되지않는다() throws Exception {
        LocalDateTime startTime = LocalDateTime.parse("2023-12-07 01:49:03", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime endTime = LocalDateTime.parse("2024-01-05 01:49:03", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        Consumer consumer1 = Consumer.builder()
                .consumerId(1L).email("seonghun7304@naver.com").name("최성훈").point(1L).auctionCredit(1L).profileImageUrl("test")
                .phoneNumber("01012345678").isFirstLogin(false).isAdult(false).isRegularPayment(true).isPaymentReservation(false).isDeleted(false).build();
        Consumer consumer2 = Consumer.builder()
                .consumerId(2L).email("seonghun7305@naver.com").name("최성훈2").point(12L).auctionCredit(12L).profileImageUrl("test2")
                .phoneNumber("01012345679").isFirstLogin(false).isAdult(false).isRegularPayment(true).isPaymentReservation(false).isDeleted(false).build();
        Subscription subscription1 = Subscription.builder().consumer(consumer1).subscriptionType(SubscriptionTypeEnum.YANGBAN)
                .paymentAmount(3900L).startDate(startTime).endDate(endTime).paymentMethod(PaymentMethodEnum.KAKAO).build();
        Subscription subscription2 = Subscription.builder().consumer(consumer2).subscriptionType(SubscriptionTypeEnum.YANGBAN)
                .paymentAmount(3900L).startDate(startTime).endDate(endTime).paymentMethod(PaymentMethodEnum.KAKAO).build();
        consumerRepository.save(consumer1);
        consumerRepository.save(consumer2);
        subscriptionRepository.save(subscription1);
        subscriptionRepository.save(subscription2);

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("date", "20240104")
                .addLong("version",2L)
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertEquals(consumerRepository.findById(1L).orElseThrow(RuntimeException::new).getIsRegularPayment(), true);
        assertEquals(consumerRepository.findById(2L).orElseThrow(RuntimeException::new).getIsRegularPayment(), true);
    }

    @Test
    public void 사용자가_정기결제를_이어간다면_유저등급은_갱신되지않는다() throws Exception {
        LocalDateTime startTime = LocalDateTime.parse("2023-12-07 01:49:03", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime endTime = LocalDateTime.parse("2024-01-04 01:49:03", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        Consumer consumer1 = Consumer.builder()
                .consumerId(1L).email("seonghun7304@naver.com").name("최성훈").point(1L).auctionCredit(1L).profileImageUrl("test")
                .phoneNumber("01012345678").isFirstLogin(false).isAdult(false).isRegularPayment(true).isPaymentReservation(true).isDeleted(false).build();
        Consumer consumer2 = Consumer.builder()
                .consumerId(2L).email("seonghun7305@naver.com").name("최성훈2").point(12L).auctionCredit(12L).profileImageUrl("test2")
                .phoneNumber("01012345679").isFirstLogin(false).isAdult(false).isRegularPayment(true).isPaymentReservation(true).isDeleted(false).build();
        Subscription subscription1 = Subscription.builder().consumer(consumer1).subscriptionType(SubscriptionTypeEnum.YANGBAN)
                .paymentAmount(3900L).startDate(startTime).endDate(endTime).paymentMethod(PaymentMethodEnum.KAKAO).build();
        Subscription subscription2 = Subscription.builder().consumer(consumer2).subscriptionType(SubscriptionTypeEnum.YANGBAN)
                .paymentAmount(3900L).startDate(startTime).endDate(endTime).paymentMethod(PaymentMethodEnum.KAKAO).build();
        consumerRepository.save(consumer1);
        consumerRepository.save(consumer2);
        subscriptionRepository.save(subscription1);
        subscriptionRepository.save(subscription2);

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("date", "20240104")
                .addLong("version",3L)
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertEquals(consumerRepository.findById(1L).orElseThrow(RuntimeException::new).getIsRegularPayment(), true);
        assertEquals(consumerRepository.findById(2L).orElseThrow(RuntimeException::new).getIsRegularPayment(), true);
    }
}