package com.at.realtimelogger.controller;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 接收日志并将其发往kafka
 */
@RestController
@Slf4j
public class LoggerToKafkaController {

    @Autowired
    KafkaTemplate kafkaTemplate;


    @RequestMapping("/applog")
    public String loggerGet(@RequestParam("param") String jsonLog) {

        log.info(jsonLog);

        //将日志发送到kafka
        kafkaTemplate.send("ods_base_log", jsonLog);


        return "success";
    }


}
