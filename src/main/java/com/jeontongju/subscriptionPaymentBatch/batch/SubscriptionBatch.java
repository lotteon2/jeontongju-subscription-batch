package com.jeontongju.subscriptionPaymentBatch.batch;

import com.jeontongju.subscriptionPaymentBatch.entity.Consumer;
import com.jeontongju.subscriptionPaymentBatch.entity.Subscription;
import com.jeontongju.subscriptionPaymentBatch.entity.SubscriptionKakao;
import com.jeontongju.subscriptionPaymentBatch.repository.SubscriptionKakaoRepository;
import com.jeontongju.subscriptionPaymentBatch.repository.SubscriptionRepository;
import io.github.bitbox.bitbox.dto.KakaoBatchDto;
import io.github.bitbox.bitbox.dto.SubscriptionBatchDto;
import io.github.bitbox.bitbox.dto.SubscriptionBatchInterface;
import io.github.bitbox.bitbox.enums.PaymentMethodEnum;
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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
                List<Subscription> subscriptionsToUpdate = new ArrayList<>();

                for (Consumer consumer : items) {
                    Optional<Subscription> latestSubscription = consumer.getSubscriptionList().stream().max(Comparator.comparing(Subscription::getEndDate));

                    if (latestSubscription.isPresent()) {
                        Subscription subscription = latestSubscription.get();
                        LocalDate endDate = subscription.getEndDate().toLocalDate();
                        if (endDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")).equals(date)) {
                            subscription.addSubscriptionTime();
                            subscriptionsToUpdate.add(subscription);
                            subscriptionBatchDtoList.add(getSubscriptionBatchInfo(subscription));
                        }
                    }
                }

                if(!subscriptionsToUpdate.isEmpty()) {
                    subscriptionRepository.saveAll(subscriptionsToUpdate);
                    kafkaTemplate.send(KafkaTopicNameInfo.PAYMENT_SUBSCRIPTION,
                            SubscriptionBatchDto.builder().subscriptionBatchInterface(subscriptionBatchDtoList).build()
                    );
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