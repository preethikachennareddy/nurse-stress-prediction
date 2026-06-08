package com.nurse.stress.prediction.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

/**
 * pojo used to hold data to sink to influx db
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class IOTPing {
    public String id;
    public Float EDA;
    public Float HR;
    public Float TEMP;
    public long datetime;
    public Integer stressLevel=0;
}
