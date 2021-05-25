package com.at.realtimepublisher.service;


import com.at.realtimepublisher.bean.ProvinceStats;

import java.util.List;


public interface ProvinceStatsService {
    List<ProvinceStats> getProvinceStats(int date);
}
