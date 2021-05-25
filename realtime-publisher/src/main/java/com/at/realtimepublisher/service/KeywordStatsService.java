package com.at.realtimepublisher.service;



import com.at.realtimepublisher.bean.KeywordStats;

import java.util.List;


public interface KeywordStatsService {
    public List<KeywordStats> getKeywordStats(int date, int limit);
}
