package com.bigdata.cloudera.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;

@Service
public class YarnJobService {
    // @Autowired
    // JdbcTemplate jdbcTemplate;

    public static EnumSet<YarnApplicationState> appStates = null;
    public static EnumSet<YarnApplicationState> finishedStates = null;
    public static List<ApplicationReport> appsReport = null;
    public static YarnClient client = null;

    static {
        client = YarnClient.createYarnClient();
        Configuration conf = new Configuration();
        client.init(conf);
        client.start();
    }


    public JSONArray getApp() {
        appStates = EnumSet.noneOf(YarnApplicationState.class);
        if (appStates.isEmpty()) {
            appStates.add(YarnApplicationState.RUNNING);
        }
        try {
            appsReport = client.getApplications(appStates);
            JSONArray array = new JSONArray();
            int i = 0;
            for (ApplicationReport appReport : appsReport) {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("jobID", appReport.getApplicationId().toString());
                jsonObject.put("jobName", appReport.getName());
                // jsonObject.put("jobStatus", appReport.getFinalApplicationStatus());
                // jsonObject.put("jobType", appReport.getApplicationType());
                // jsonObject.put("clusterDetails", appReport.getApplicationResourceUsageReport().toString());
                // jsonObject.put("progress", appReport.getProgress());
                // jsonObject.put("startTime", appReport.getStartTime());
                // jsonObject.put("finishTime", appReport.getFinishTime());
                // jsonObject.put("count_time", "运行时长：" + (appReport.getFinishTime() - appReport.getStartTime()) / 1000 + "s");
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                jsonObject.put("now_Time", format.format(new Date()));
                array.add(jsonObject);
                // Date date = new Date();
                // Timestamp timestamp = new Timestamp(date.getTime());
                // String sql = "INSERT  INTO yarn_details (job_id,job_name,job_type,final_status,cluster_details,job_progress,start_time,finish_time,create_time) " +
                //         "values(?,?,?,?,?,?,?,?,?)";
                // jdbcTemplate.update(sql, appReport.getApplicationId().toString(), appReport.getName(), appReport.getApplicationType(),
                //         appReport.getFinalApplicationStatus().toString(), appReport.getApplicationResourceUsageReport().toString(),
                //         appReport.getProgress(), appReport.getStartTime(), appReport.getFinishTime(), timestamp);
            }
            return array;
        } catch (YarnException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 统计任务执行成功和失败的总数
     */
    public JSONArray getFinishedApp() {
        appStates = EnumSet.noneOf(YarnApplicationState.class);
        if (appStates.isEmpty()) {
            appStates.add(YarnApplicationState.FINISHED);
            appStates.add(YarnApplicationState.FAILED);
            appStates.add(YarnApplicationState.RUNNING);
            appStates.add(YarnApplicationState.KILLED);
        }
        JSONArray array = new JSONArray();
        try {
            appsReport = client.getApplications(appStates);
            int i = 0;
            int x = 0;
            int y = 0;
            int z = 0;
            JSONObject jsonObject = new JSONObject();
            for (ApplicationReport appReport : appsReport) {
                if (appReport.getFinalApplicationStatus().toString().equals("SUCCEEDED")) {
                    i++;
                }
                if (appReport.getFinalApplicationStatus().toString().equals("FAILED")) {
                    x++;
                }
                if (appReport.getFinalApplicationStatus().toString().equals("RUNNING")) {
                    y++;
                }
                if (appReport.getFinalApplicationStatus().toString().equals("KILLED")) {
                    z++;
                }
            }
            jsonObject.put("SUCCEEDED", i);
            jsonObject.put("FAILED", x);
            jsonObject.put("RUNNING", y);
            jsonObject.put("KILLED", z);

            array.add(jsonObject);
        } catch (YarnException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return array;
    }
}
