package com.example.analytics.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Component
public class AsyncKafkaAnalyticsConsumer {
    private static final Logger logger = LoggerFactory.getLogger(AsyncKafkaAnalyticsConsumer.class);

    private final AnalyticsEventProcessor eventProcessor;
    private final ExecutorService processingExecutor;
    private final int maxAsyncInflightBatches;
    private final int maxRetryAttempts;
    private final Duration retryBackoff;

    public AsyncKafkaAnalyticsConsumer(
            AnalyticsEventProcessor eventProcessor,
            @Value("${consumer.async.threads:8}") int threadPoolSize,
            @Value("${consumer.async.inflight.batches:10}") int maxAsyncInflightBatches,
            @Value("${consumer.retry.maxAttempts:3}") int maxRetryAttempts,
            @Value("${consumer.retry.backoff.ms:200}") long retryBackoffMs
    ) {
        this.eventProcessor = eventProcessor;
        this.processingExecutor = new ThreadPoolExecutor(
                threadPoolSize, threadPoolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(maxAsyncInflightBatches),
                Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy()
        );
        this.maxAsyncInflightBatches = maxAsyncInflightBatches;
        this.maxRetryAttempts = maxRetryAttempts;
        this.retryBackoff = Duration.ofMillis(retryBackoffMs);
    }

    @PreDestroy
    public void shutdownExecutor() {
        processingExecutor.shutdown();
        try {
            if (!processingExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                processingExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @KafkaListener(
        topics = "${kafka.analytics-topic}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void processEventBatch(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        if (records.isEmpty()) return;
        logger.debug("Received Kafka batch: size={}", records.size());
        CountDownLatch latch = new CountDownLatch(1);

        submitBatchWithRetries(records, 0, latch, ack);
    }

    private void submitBatchWithRetries(List<ConsumerRecord<String, String>> records, int attempt, CountDownLatch latch, Acknowledgment ack) {
        CompletableFuture.supplyAsync(() -> {
            try {
                long start = System.currentTimeMillis();
                eventProcessor.process(records);
                long took = System.currentTimeMillis() - start;
                logger.debug("Processed batch of {} events in {} ms", records.size(), took);
                return null;
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, processingExecutor).whenComplete((res, ex) -> {
            if (ex == null) {
                try {
                    ack.acknowledge(); // manual, immediate offset commit
                    logger.debug("Batch offsets committed");
                } catch (Exception commitEx) {
                    logger.error("Ack commit failed!", commitEx);
                }
                latch.countDown();
            } else {
                if (attempt + 1 < maxRetryAttempts) {
                    logger.warn("Batch processing failed, attempt {}/{}. Retrying in {}ms", attempt + 1, maxRetryAttempts, retryBackoff.toMillis(), ex.getCause());
                    sleepQuiet(retryBackoff.toMillis());
                    submitBatchWithRetries(records, attempt + 1, latch, ack);
                } else {
                    logger.error("Batch failed after {} attempts. Discarding batch to avoid consumer stalling.", maxRetryAttempts, ex.getCause());
                    // Optionally send to DLQ here
                    ack.acknowledge(); // Commit offset to avoid stalling forever
                    latch.countDown();
                }
            }
        });
        try {
            if (!latch.await(10, TimeUnit.MINUTES)) {
                logger.error("Batch stuck for over 10 minutes, discarding!");
                ack.acknowledge();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void sleepQuiet(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }
}
