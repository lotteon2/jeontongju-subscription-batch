package com.jeontongju.subscriptionPaymentBatch.batch;

import com.jeontongju.KafkaConsumerMock;
import com.jeontongju.TestBatchConfig;
import com.jeontongju.subscriptionPaymentBatch.entity.Consumer;
import com.jeontongju.subscriptionPaymentBatch.entity.Subscription;
import com.jeontongju.subscriptionPaymentBatch.entity.SubscriptionKakao;
import com.jeontongju.subscriptionPaymentBatch.repository.ConsumerRepository;
import com.jeontongju.subscriptionPaymentBatch.repository.SubscriptionKakaoRepository;
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
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBatchTest
@SpringBootTest(classes = {SubscriptionBatch.class, TestBatchConfig.class, KafkaConsumerMock.class})
@EmbeddedKafka( partitions = 1,
                brokerProperties = {"listeners=PLAINTEXT://localhost:7777"},
                ports={7777})
public class SubscriptionBatchTest {
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private ConsumerRepository consumerRepository;
    @Autowired
    private SubscriptionRepository subscriptionRepository;
    @Autowired
    private SubscriptionKakaoRepository subscriptionKakaoRepository;
    @Autowired
    private KafkaConsumerMock kafkaConsumer;

    @AfterEach
    public void deleteData(){
        subscriptionKakaoRepository.deleteAll();
        subscriptionRepository.deleteAll();
        consumerRepository.deleteAll();
    }

    @Test
    public void 정기구독여부가_존재하면서_오늘이_구독연장일이면_구독이_연장되고_카프카를발송하고_endTime을_30일추가한다() throws Exception {
        LocalDateTime startTime = LocalDateTime.parse("2023-12-07 01:49:03", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime endTime = LocalDateTime.parse("2023-12-07 01:49:03", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        Consumer consumer = Consumer.builder()
                .consumerId(1L).email("seonghun7304@naver.com").name("최성훈").point(1L).auctionCredit(1L).profileImageUrl("test")
                .phoneNumber("01012345678").isFirstLogin(false).isAdult(false).isPaymentReservation(false).isDeleted(false).build();
        Consumer consumer2 = Consumer.builder()
                .consumerId(2L).email("seonghun7305@naver.com").name("최성훈2").point(12L).auctionCredit(12L).profileImageUrl("test2")
                .phoneNumber("01012345679").isFirstLogin(false).isAdult(false).isPaymentReservation(true).isDeleted(false).build();
        Subscription subscription = Subscription.builder().consumer(consumer2).subscriptionType(SubscriptionTypeEnum.YANGBAN)
                .paymentAmount(3900L).startDate(startTime).endDate(endTime).paymentMethod(PaymentMethodEnum.KAKAO).build();
        SubscriptionKakao subscriptionKakao = SubscriptionKakao.builder().kakaoSid("1234").kakaoStoreCode("test").kakaoOrderId("test").subscription(subscription).build();
        consumerRepository.save(consumer);
        consumerRepository.save(consumer2);
        subscriptionRepository.save(subscription);
        subscriptionKakaoRepository.save(subscriptionKakao);

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("date", "20231207")
        .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        for(Subscription target : subscriptionRepository.findAll()){
            assertEquals(target.getEndDate(), endTime.plusDays(30));
        }
    }

    @Test
    public void 정기구독_날이_아니라면_갱신되지_않는다() throws Exception{
        LocalDateTime startTime = LocalDateTime.parse("2023-12-07 01:49:03", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime endTime = LocalDateTime.parse("2023-12-09 01:49:03", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        Consumer consumer = Consumer.builder()
                .consumerId(1L).email("seonghun7304@naver.com").name("최성훈").point(1L).auctionCredit(1L).profileImageUrl("test")
                .phoneNumber("01012345678").isFirstLogin(false).isAdult(false).isPaymentReservation(false).isDeleted(false).build();
        Consumer consumer2 = Consumer.builder()
                .consumerId(2L).email("seonghun7305@naver.com").name("최성훈2").point(12L).auctionCredit(12L).profileImageUrl("test2")
                .phoneNumber("01012345679").isFirstLogin(false).isAdult(false).isPaymentReservation(true).isDeleted(false).build();
        Subscription subscription = Subscription.builder().consumer(consumer2).subscriptionType(SubscriptionTypeEnum.YANGBAN)
                .paymentAmount(3900L).startDate(startTime).endDate(endTime).paymentMethod(PaymentMethodEnum.KAKAO).build();
        SubscriptionKakao subscriptionKakao = SubscriptionKakao.builder().kakaoSid("1234").kakaoStoreCode("test").kakaoOrderId("test").subscription(subscription).build();
        consumerRepository.save(consumer);
        consumerRepository.save(consumer2);
        subscriptionRepository.save(subscription);
        subscriptionKakaoRepository.save(subscriptionKakao);

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("date", "20231208")
        .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        for(Subscription target : subscriptionRepository.findAll()){
            assertEquals(target.getEndDate(), endTime);
        }
    }

    @Test
    public void 정기구독_대상이_없으면_갱신되지_않는다() throws Exception{
        LocalDateTime endTime = LocalDateTime.parse("2023-12-09 01:49:03", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        Consumer consumer = Consumer.builder()
                .consumerId(1L).email("seonghun7304@naver.com").name("최성훈").point(1L).auctionCredit(1L).profileImageUrl("test")
                .phoneNumber("01012345678").isFirstLogin(false).isAdult(false).isPaymentReservation(false).isDeleted(false).build();
        Consumer consumer2 = Consumer.builder()
                .consumerId(2L).email("seonghun7305@naver.com").name("최성훈2").point(12L).auctionCredit(12L).profileImageUrl("test2")
                .phoneNumber("01012345679").isFirstLogin(false).isAdult(false).isPaymentReservation(false).isDeleted(false).build();
        consumerRepository.save(consumer);
        consumerRepository.save(consumer2);

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("date", "20231209")
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        for(Subscription target : subscriptionRepository.findAll()){
            assertEquals(target.getEndDate(), endTime);
        }
    }

    @Test
    public void 구독권정보가_여러개지만_최신구독권이_갱신날짜랑_같으면_갱신된다() throws Exception{
        LocalDateTime startTime = LocalDateTime.parse("2023-12-07 01:49:03", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime endTime = LocalDateTime.parse("2023-12-07 01:49:03", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime endTime2 = LocalDateTime.parse("2023-12-10 01:49:03", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        Consumer consumer = Consumer.builder()
                .consumerId(1L).email("seonghun7304@naver.com").name("최성훈").point(1L).auctionCredit(1L).profileImageUrl("test")
                .phoneNumber("01012345678").isFirstLogin(false).isAdult(false).isPaymentReservation(false).isDeleted(false).build();
        Consumer consumer2 = Consumer.builder()
                .consumerId(2L).email("seonghun7305@naver.com").name("최성훈2").point(12L).auctionCredit(12L).profileImageUrl("test2")
                .phoneNumber("01012345679").isFirstLogin(false).isAdult(false).isPaymentReservation(true).isDeleted(false).build();
        Subscription subscription = Subscription.builder().consumer(consumer2).subscriptionType(SubscriptionTypeEnum.YANGBAN)
                .paymentAmount(3900L).startDate(startTime).endDate(endTime).paymentMethod(PaymentMethodEnum.KAKAO).build();
        SubscriptionKakao subscriptionKakao = SubscriptionKakao.builder().kakaoSid("1234").kakaoStoreCode("test").kakaoOrderId("test").subscription(subscription).build();
        Subscription subscription2 = Subscription.builder().consumer(consumer2).subscriptionType(SubscriptionTypeEnum.YANGBAN)
                .paymentAmount(3900L).startDate(startTime).endDate(endTime2).paymentMethod(PaymentMethodEnum.KAKAO).build();
        SubscriptionKakao subscriptionKakao2 = SubscriptionKakao.builder().kakaoSid("1235").kakaoStoreCode("test2").kakaoOrderId("test2").subscription(subscription2).build();
        consumerRepository.save(consumer);
        consumerRepository.save(consumer2);
        subscriptionRepository.save(subscription);
        subscriptionRepository.save(subscription2);
        subscriptionKakaoRepository.save(subscriptionKakao);
        subscriptionKakaoRepository.save(subscriptionKakao2);

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("date", "20231210")
        .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertEquals(subscriptionRepository.findById(subscription2.getSubscriptionId()).orElseThrow(RuntimeException::new).getEndDate(), endTime2.plusDays(30));
    }

    @Test
    public void 구독권정보가_여러개지만_최신구독권이_갱신날짜랑_같지않으면_갱신되지않는다() throws Exception{
        LocalDateTime startTime = LocalDateTime.parse("2023-12-07 01:49:03", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime endTime = LocalDateTime.parse("2023-12-07 01:49:03", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime endTime2 = LocalDateTime.parse("2023-12-10 01:49:03", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        Consumer consumer = Consumer.builder()
                .consumerId(1L).email("seonghun7304@naver.com").name("최성훈").point(1L).auctionCredit(1L).profileImageUrl("test")
                .phoneNumber("01012345678").isFirstLogin(false).isAdult(false).isPaymentReservation(false).isDeleted(false).build();
        Consumer consumer2 = Consumer.builder()
                .consumerId(2L).email("seonghun7305@naver.com").name("최성훈2").point(12L).auctionCredit(12L).profileImageUrl("test2")
                .phoneNumber("01012345679").isFirstLogin(false).isAdult(false).isPaymentReservation(true).isDeleted(false).build();
        Subscription subscription = Subscription.builder().consumer(consumer2).subscriptionType(SubscriptionTypeEnum.YANGBAN)
                .paymentAmount(3900L).startDate(startTime).endDate(endTime).paymentMethod(PaymentMethodEnum.KAKAO).build();
        SubscriptionKakao subscriptionKakao = SubscriptionKakao.builder().kakaoSid("1234").kakaoStoreCode("test").kakaoOrderId("test").subscription(subscription).build();
        Subscription subscription2 = Subscription.builder().consumer(consumer2).subscriptionType(SubscriptionTypeEnum.YANGBAN)
                .paymentAmount(3900L).startDate(startTime).endDate(endTime2).paymentMethod(PaymentMethodEnum.KAKAO).build();
        SubscriptionKakao subscriptionKakao2 = SubscriptionKakao.builder().kakaoSid("1235").kakaoStoreCode("test2").kakaoOrderId("test2").subscription(subscription2).build();
        consumerRepository.save(consumer);
        consumerRepository.save(consumer2);
        subscriptionRepository.save(subscription);
        subscriptionRepository.save(subscription2);
        subscriptionKakaoRepository.save(subscriptionKakao);
        subscriptionKakaoRepository.save(subscriptionKakao2);

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("date", "20231211")
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertEquals(subscriptionRepository.findById(subscription2.getSubscriptionId()).orElseThrow(RuntimeException::new).getEndDate(), endTime2);
    }

}
