package com.example.analytics.service;

import java.util.List;

public interface AnalyticsEventRepository {
    void saveAll(List<String> eventPayloads);
}
