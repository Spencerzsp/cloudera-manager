package com.bigdata.cloudera.bean;

import java.io.Serializable;

/**
 * @program: Cluster-Manager
 * @description
 * @author: z p、
 * @create: 2020-04-08 11:46
 **/
public class totalMapBean implements Serializable {
    private totalBean total;
    private userBaen used;

    public totalMapBean(totalBean total, userBaen used) {
        this.total = total;
        this.used = used;
    }

    public totalMapBean() {

    }

    public totalBean getTotal() {
        return total;
    }

    public void setTotal(totalBean total) {
        this.total = total;
    }

    public userBaen getUsed() {
        return used;
    }

    public void setUsed(userBaen used) {
        this.used = used;
    }
}
