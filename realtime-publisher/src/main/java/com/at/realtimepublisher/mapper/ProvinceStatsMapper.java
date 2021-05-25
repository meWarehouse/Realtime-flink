package com.at.realtimepublisher.mapper;

import com.at.realtimepublisher.bean.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;


public interface ProvinceStatsMapper {
    @Select("select province_name,sum(order_amount) order_amount from province_stats " +
        "where toYYYYMMDD(stt)=#{date} group by province_id,province_name")
    List<ProvinceStats> selectProvinceStats(int date);
}
