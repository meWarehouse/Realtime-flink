package com.at.loggercreate;

import com.at.loggercreate.bean.AppStart.Builder;
import com.alibaba.fastjson.JSON;
import com.at.loggercreate.bean.*;
import com.at.loggercreate.common.ConfigUtil;
import com.at.loggercreate.common.ParamUtil;
import com.at.loggercreate.common.RandomNum;
import com.at.loggercreate.common.RandomOptionGroup;
import com.at.loggercreate.config.AppConfig;
import com.at.loggercreate.enums.PageId;
import com.at.loggercreate.util.HttpUtil;
import com.at.loggercreate.util.KafkaUtil;
import com.at.loggercreate.util.LogUtil;
import org.apache.commons.lang3.EnumUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @author zero
 * @create 2021-06-26 17:58
 */
@Component
public class Mocker implements Runnable {
    private Long ts;
    @Autowired
    KafkaTemplate kafkaTemplate;

    public Mocker() {
    }

    public List<AppMain> doAppMock() {
        List<AppMain> logList = new ArrayList();
        Date curDate = ParamUtil.checkDate(AppConfig.mock_date);
        this.ts = curDate.getTime();
        AppMain.AppMainBuilder appMainBuilder = AppMain.builder();
        AppCommon appCommon = AppCommon.build();
        appMainBuilder.common(appCommon);
        appMainBuilder.checkError();
        AppStart appStart = (new Builder()).build();
        appMainBuilder.start(appStart);
        appMainBuilder.ts(this.ts);
        logList.add(appMainBuilder.build());
        String jsonFile = ConfigUtil.loadJsonFile("path.json");
        List<Map> pathList = JSON.parseArray(jsonFile, Map.class);
       RandomOptionGroup.Builder<List> builder = RandomOptionGroup.builder();
        Iterator var9 = pathList.iterator();

        while(var9.hasNext()) {
            Map map = (Map)var9.next();
            List path = (List)map.get("path");
            Integer rate = (Integer)map.get("rate");
            builder.add(path, rate);
        }

        List chosenPath = (List)builder.build().getRandomOpt().getValue();
        PageId lastPageId = null;
        Iterator var22 = chosenPath.iterator();

        while(var22.hasNext()) {
            Object o = var22.next();
            AppMain.AppMainBuilder pageBuilder = AppMain.builder().common(appCommon);
            String path = (String)o;
            int pageDuringTime = RandomNum.getRandInt(1000, AppConfig.page_during_max_ms);
            PageId pageId = (PageId) EnumUtils.getEnum(PageId.class, path);
            AppPage page = AppPage.build(pageId, lastPageId, pageDuringTime);
            if (pageId == null) {
                System.out.println();
            }

            pageBuilder.page(page);
            lastPageId = page.getPage_id();
            List<AppAction> appActionList = AppAction.buildList(page, this.ts, pageDuringTime);
            if (appActionList.size() > 0) {
                pageBuilder.actions(appActionList);
            }

            List<AppDisplay> displayList = AppDisplay.buildList(page);
            if (displayList.size() > 0) {
                pageBuilder.displays(displayList);
            }

            pageBuilder.ts(this.ts);
            pageBuilder.checkError();
            logList.add(pageBuilder.build());
        }

        return logList;
    }

    public static void main(String[] args) {
        (new Mocker()).doAppMock();
    }

    public void run() {

        List<AppMain> appMainList = this.doAppMock();
        Iterator var2 = appMainList.iterator();

        while(var2.hasNext()) {
            AppMain appMain = (AppMain)var2.next();
            if (AppConfig.mock_type.equals("log")) {
                LogUtil.log(appMain.toString());
            } else if (AppConfig.mock_type.equals("http")) {
                HttpUtil.get(appMain.toString());
            } else if (AppConfig.mock_type.equals("kafka")) {
                KafkaUtil.send(AppConfig.kafka_topic, appMain.toString());
            }

            try {
                Thread.sleep((long)AppConfig.log_sleep);
            } catch (InterruptedException var5) {
                var5.printStackTrace();
            }
        }

    }
}
