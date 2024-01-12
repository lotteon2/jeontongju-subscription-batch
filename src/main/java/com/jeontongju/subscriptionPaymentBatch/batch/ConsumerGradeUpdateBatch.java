package com.jeontongju.subscriptionPaymentBatch.batch;

import com.jeontongju.subscriptionPaymentBatch.entity.Consumer;
import com.jeontongju.subscriptionPaymentBatch.repository.ConsumerRepository;
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

import javax.persistence.EntityManagerFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
@Configuration
@Slf4j
public class ConsumerGradeUpdateBatch {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory emf;
    private int chunkSize = 500;
    private final ConsumerRepository consumerRepository;

    @Bean
    public Job consumerGradeUpdateJob() {
        return jobBuilderFactory.get("consumerGradeUpdateJob")
                .start(consumerGradeUpdateStep()).build();
    }

    @Bean
    public Step consumerGradeUpdateStep() {
        return stepBuilderFactory.get("consumerGradeUpdateStep")
                .<Consumer, Long>chunk(chunkSize)
                .reader(consumerGradeUpdateReader(null))
                .writer(consumerGradeUpdateWriter())
                .build();
    }

    @Bean
    @StepScope
    public JpaPagingItemReader<Consumer> consumerGradeUpdateReader(@Value("#{jobParameters[date]}") String date) {
        Map<String, Object> params = new HashMap<>();
        params.put("date", date);
        params.put("isDeleted", false);
        params.put("isPaymentReservation", false);
        params.put("isRegularPayment", true);

        return new JpaPagingItemReaderBuilder<Consumer>()
                .name("consumerGradeUpdateReader")
                .entityManagerFactory(emf)
                .pageSize(chunkSize)
                .queryString("SELECT s.consumer.consumerId FROM Subscription s " +
                                "JOIN s.consumer c " +
                                "LEFT JOIN Subscription s2 ON s.consumer.consumerId = s2.consumer.consumerId AND s.endDate < s2.endDate " +
                                "WHERE s2.consumer.consumerId IS NULL " +
                                "AND REPLACE(SUBSTRING(s.endDate, 1, 10), '-', '') = :date "+
                                "AND c.isDeleted = :isDeleted "+
                                "AND c.isPaymentReservation = :isPaymentReservation "+
                                "AND c.isRegularPayment = :isRegularPayment "+
                                "ORDER BY c.consumerId ASC"
                        )
                .parameterValues(params)
        .build();
    }

    @Bean
    public JpaItemWriter<Long> consumerGradeUpdateWriter() {
        JpaItemWriter<Long> jpaItemWriter = new JpaItemWriter<>() {
            @Override
            public void write(List<? extends Long> items) {
                consumerRepository.updateConsumerGrade(items);
            }
        };

        jpaItemWriter.setEntityManagerFactory(emf);
        return jpaItemWriter;
    }
}