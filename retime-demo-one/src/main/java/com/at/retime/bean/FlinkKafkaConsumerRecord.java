package com.at.retime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * @create 2021-10-13
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
@Accessors(chain = true)
public class FlinkKafkaConsumerRecord {

    private byte[] key;
    private byte[] msg;
    private String topic;
    private int partition;
    private long offset;
    private long timestamp;

}
