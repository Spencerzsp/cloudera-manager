package com.bigdata.cloudera.bean;

import java.io.Serializable;

/**
 * @program: Cluster-Manager
 * @description
 * @author: z p、
 * @create: 2020-04-08 11:45
 **/

public class totalBean implements Serializable {
    private String unit;
    private String value;

    public totalBean() {
    }

    public totalBean(String unit, String value) {
        this.unit = unit;
        this.value = value;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "TotalBean{" +
                "unit='" + unit + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
