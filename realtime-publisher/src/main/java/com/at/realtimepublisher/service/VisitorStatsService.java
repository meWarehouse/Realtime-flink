package com.at.realtimepublisher.service;


import com.at.realtimepublisher.bean.VisitorStats;

import java.util.List;


public interface VisitorStatsService {

    List<VisitorStats> getVisitorStatsByNewFlag(int date);

    List<VisitorStats> getVisitorStatsByHr(int date);

}
