package com.study.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 传感器温度读书的数据类型
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SensorReading {
    private String id;
    private Long timestamp;
    private Double temperature;
}
