package com.at.realtimepublisher.service.impl;


import com.at.realtimepublisher.bean.KeywordStats;
import com.at.realtimepublisher.mapper.KeywordStatsMapper;
import com.at.realtimepublisher.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {

    @Autowired
    KeywordStatsMapper keywordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStats(int date, int limit) {
        return keywordStatsMapper.selectKeywordStats(date,limit);
    }
}
