package com.jeontongju.subscriptionPaymentBatch.batch;

import com.jeontongju.subscriptionPaymentBatch.entity.Consumer;
import com.jeontongju.subscriptionPaymentBatch.entity.Subscription;
import com.jeontongju.subscriptionPaymentBatch.entity.SubscriptionKakao;
import com.jeontongju.subscriptionPaymentBatch.repository.SubscriptionKakaoRepository;
import com.jeontongju.subscriptionPaymentBatch.repository.SubscriptionRepository;
import io.github.bitbox.bitbox.dto.KakaoBatchDto;
import io.github.bitbox.bitbox.dto.MemberInfoForNotificationDto;
import io.github.bitbox.bitbox.dto.ServerErrorForNotificationDto;
import io.github.bitbox.bitbox.dto.SubscriptionBatchDto;
import io.github.bitbox.bitbox.dto.SubscriptionBatchInterface;
import io.github.bitbox.bitbox.enums.NotificationTypeEnum;
import io.github.bitbox.bitbox.enums.PaymentMethodEnum;
import io.github.bitbox.bitbox.enums.RecipientTypeEnum;
import io.github.bitbox.bitbox.util.KafkaTopicNameInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import javax.persistence.EntityManagerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.time.LocalDateTime.now;

@RequiredArgsConstructor
@Configuration
@Slf4j
public class SubscriptionBatch {
    private final SubscriptionKakaoRepository subscriptionKakaoRepository;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory emf;
    private int chunkSize = 500;
    private final KafkaTemplate<String, SubscriptionBatchDto> kafkaTemplate;
    private final KafkaTemplate<String, MemberInfoForNotificationDto> memberInfoForNotificationDtoKafkaProcessor;
    private final SubscriptionRepository subscriptionRepository;

    // 매 00:00에 도는 배치
    @Bean
    public Job subscriptionJob() {
        return jobBuilderFactory.get("subscriptionJob")
                .start(subscriptionStep()).build();
    }

    @Bean
    public Step subscriptionStep() {
        return stepBuilderFactory.get("subscriptionStep")
                .<Consumer, Consumer>chunk(chunkSize)
                .reader(subscriptionReader())
                .writer(subscriptionWriter(null))
                .build();
    }

    @Bean
    public JpaPagingItemReader<Consumer> subscriptionReader() {
        Map<String, Object> params = new HashMap<>();
        params.put("isDeleted", false);
        params.put("isRegularPayment", true);

        return new JpaPagingItemReaderBuilder<Consumer>()
                .name("subscriptionReader")
                .entityManagerFactory(emf)
                .pageSize(chunkSize)
                .queryString("SELECT c FROM Consumer c WHERE c.isDeleted = :isDeleted AND c.isRegularPayment = :isRegularPayment ORDER BY c.consumerId ASC")
                .parameterValues(params)
        .build();
    }

    @Bean
    @StepScope
    public JpaItemWriter<Consumer> subscriptionWriter(@Value("#{jobParameters[date]}") String date) {
        JpaItemWriter<Consumer> jpaItemWriter = new JpaItemWriter<>() {
            @Override
            public void write(List<? extends Consumer> items) {
                List<SubscriptionBatchInterface> subscriptionBatchDtoList = new ArrayList<>();

                List<Long> consumerIds = items.stream().map(Consumer::getConsumerId).collect(Collectors.toList());
                List<Subscription> subscriptionList = subscriptionRepository.findSubscriptionsByConsumerIdsAndEndDate(consumerIds, date);

                for(Subscription subscription : subscriptionList){
                    subscription.addSubscriptionTime();
                    subscriptionBatchDtoList.add(getSubscriptionBatchInfo(subscription));
                }

                if(!subscriptionList.isEmpty()) {
                    kafkaTemplate.send(KafkaTopicNameInfo.PAYMENT_SUBSCRIPTION,
                            SubscriptionBatchDto.builder().subscriptionBatchInterface(subscriptionBatchDtoList).build()
                    );

                    for(Long id : consumerIds){
                        memberInfoForNotificationDtoKafkaProcessor.send(KafkaTopicNameInfo.SEND_NOTIFICATION,
                                ServerErrorForNotificationDto.builder()
                                        .recipientId(id)
                                        .recipientType(RecipientTypeEnum.ROLE_CONSUMER)
                                        .notificationType(NotificationTypeEnum.SUCCESS_SUBSCRIPTION_PAYMENTS)
                                        .createdAt(now())
                                .build());
                    }
                }
            }
        };

        jpaItemWriter.setEntityManagerFactory(emf);
        return jpaItemWriter;
    }

    private SubscriptionBatchInterface getSubscriptionBatchInfo(Subscription subscription){
        SubscriptionBatchInterface subscriptionBatchInterface = null;
        if(subscription.getPaymentMethod() == PaymentMethodEnum.KAKAO){
            SubscriptionKakao subscriptionKakao = subscriptionKakaoRepository.findById(subscription.getSubscriptionId()).orElseThrow(() -> new RuntimeException(""));
            subscriptionBatchInterface =  KakaoBatchDto.builder().cid(subscriptionKakao.getKakaoStoreCode()).sid(subscriptionKakao.getKakaoSid())
                    .partnerOrderId(subscriptionKakao.getKakaoOrderId()).partnerUserId(String.valueOf(subscription.getConsumer().getConsumerId()))
                    .quantity(1L).totalAmount(subscription.getPaymentAmount()).taxFreeAmount(0L).build();
        }

        return subscriptionBatchInterface;
    }

}