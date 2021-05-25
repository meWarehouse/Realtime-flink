package com.at.realtimepublisher.service.impl;

import com.at.realtimepublisher.bean.ProvinceStats;
import com.at.realtimepublisher.mapper.ProvinceStatsMapper;
import com.at.realtimepublisher.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {
    //注入mapper
    @Autowired
    ProvinceStatsMapper provinceStatsMapper;

    @Override
    public List<ProvinceStats> getProvinceStats(int date) {
        return provinceStatsMapper.selectProvinceStats(date);
    }
}
