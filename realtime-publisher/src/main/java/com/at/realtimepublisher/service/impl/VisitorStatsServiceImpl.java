package com.at.realtimepublisher.service.impl;

import com.at.realtimepublisher.bean.VisitorStats;
import com.at.realtimepublisher.mapper.VisitorStatsMapper;
import com.at.realtimepublisher.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
public class VisitorStatsServiceImpl implements VisitorStatsService {

    @Autowired
    VisitorStatsMapper visitorStatsMapper;
    @Override
    public List<VisitorStats> getVisitorStatsByNewFlag(int date) {
        return visitorStatsMapper.selectVisitorStatsByNewFlag(date);
    }

    @Override
    public List<VisitorStats> getVisitorStatsByHr(int date) {
        return visitorStatsMapper.selectVisitorStatsByHr(date);
    }
}
