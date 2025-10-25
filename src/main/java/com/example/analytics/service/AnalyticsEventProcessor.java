package com.example.analytics.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class AnalyticsEventProcessor {
    private final AnalyticsEventRepository analyticsEventRepository;

    @Autowired
    public AnalyticsEventProcessor(AnalyticsEventRepository analyticsEventRepository) {
        this.analyticsEventRepository = analyticsEventRepository;
    }

    public void process(List<ConsumerRecord<String, String>> records) {
        // Process events efficiently in batch - avoid costly string concatenation etc.
        // Here, assume event parsing and bulk DB insert is handled inside repository
        analyticsEventRepository.saveAll(records.stream().map(r -> r.value()).toList());
    }
}
