package com.bigdata.cloudera.service;


import com.alibaba.fastjson.JSONObject;
import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.DataView;
import com.cloudera.api.model.*;
import com.cloudera.api.v10.HostsResourceV10;
import com.cloudera.api.v11.TimeSeriesResourceV11;
import com.cloudera.api.v19.RootResourceV19;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;


public class ClouderMangerUtlis {

    static String url = "wbbigdata00";
    static int port = 7180;
    static String username = "admin";
    static String password = "admin";
    static RootResourceV19 apiRoot;

    static {
        apiRoot = new ClouderaManagerClientBuilder()
                .withHost(url)
                .withPort(port)
                .withUsernamePassword(username, password)
                .build()
                .getRootV19();
    }

    /**
     * 集群cpu
     */
    static String cpuQuery = "SELECT cpu_percent_across_hosts WHERE entityName = 1 AND category = CLUSTER";
    /**
     * 集群磁盘io
     */
    static String diskioQuery = "SELECT total_read_bytes_rate_across_disks+total_write_bytes_rate_across_disks WHERE entityName = 1 AND category = CLUSTER";
    /**
     * 集群网络io
     */
    static String networkQuery = "SELECT total_bytes_receive_rate_across_network_interfaces+total_bytes_transmit_rate_across_network_interfaces WHERE entityName = 1 AND category = CLUSTER";
    /**
     * 集群hdfs io
     */
    static String hdfsioQuery = "SELECT total_bytes_read_rate_across_datanodes+total_bytes_written_rate_across_datanodes WHERE entityName = hdfs AND category = SERVICE";

    /**
     * 集群重要警报
     */
    static String clusterAlertsRateQuery = "select integral(alerts_rate) where entityName=yarn";
    /**
     * 集群关键事件
     */
    static String clusterEventsCriticalRateQuery = "select integral(events_critical_rate) where entityName=yarn";
    /**
     * 集群重要事件
     */
    static String clusterEventsImportantRateQuery = "select integral(events_important_rate) where entityName=yarn";

    /**
     * 正在运行的应用程序
     */
    static String appsRunningCumulativeQuery = "SELECT apps_running_cumulative WHERE entityName = yarn:root AND category = YARN_POOL";

    // /**
    //  * 入驻单位中所有在线用户
    //  */
    // static String activeUserQuery = "SELECT hue_users_active WHERE entityName = hue-HUE_SERVER-ae57fd7a8981f1440d0aaae51abaa549 AND category = ROLE";
    //
    // /**
    //  * 在线用户
    //  */
    // public List<JSONObject> getActiveUser() {
    //     return getSeriesDataList(activeUserQuery);
    // }
    //
    // /**
    //  * 资源列队
    //  *
    //  * @return
    //  */
    // public List<JSONObject> getAppsRunningCumulative() {
    //     return getSeriesDataList(appsRunningCumulativeQuery);
    // }

    /**
     * 单台服务器重要警报
     */
    static String OneServerAlertsRateQuery = "SELECT integral(alerts_rate) WHERE entityName = \"5d814965-5601-4db7-a452-81567aafc113\" AND category = HOST";
    /**
     * 单台服务器关键事件
     */
    static String OneServerEventsCriticalRateQuery = "SELECT integral(alerts_rate) WHERE entityName = \"5d814965-5601-4db7-a452-81567aafc113\" AND category = HOST";
    /**
     * 单台服务器重要事件
     */
    static String OneServerEventsImportantRateQuery = "SELECT integral(alerts_rate) WHERE entityName = \"5d814965-5601-4db7-a452-81567aafc113\" AND category = HOST";
    /**
     * 单台服务器运行装款
     */
    static String OneServerExeState = "SELECT health_good_rate * 100 AS \"good health\" WHERE entityName = \"5d814965-5601-4db7-a452-81567aafc113\" AND category = HOST";
    /**
     * 聚合磁盘IOPS
     */
    static String OneServerAggregationDiskIOPS = "SELECT total_read_ios_rate_across_disks WHERE entityName = \"5d814965-5601-4db7-a452-81567aafc113\" AND category = HOST";

    /**
     * 聚合磁盘吞吐量
     */
    static String OneServerDiskThroughput = "SELECT total_write_bytes_rate_across_disks WHERE entityName = \"5d814965-5601-4db7-a452-81567aafc113\" AND category = HOST";
    /**
     * 磁盘延迟
     */
    static String DiskLatency = "SELECT service_time WHERE entityName = '5d814965-5601-4db7-a452-81567aafc113:vdb' AND category = DISK";
    /**
     * 磁盘吞吐量
     */
    static String DiskThroughput = "SELECT total_write_bytes_rate_across_disks WHERE entityName = '5d814965-5601-4db7-a452-81567aafc113' AND category = HOST";
    /**
     * 所有主机网络吞吐量
     */
    static String NetworkThroughput = "SELECT total_bytes_receive_rate_across_network_interfaces WHERE entityName = '5d814965-5601-4db7-a452-81567aafc113' AND category = HOST";
    /**
     * 平均负载
     */
    static String AverageLoad = "SELECT load_1 WHERE entityName = '5d814965-5601-4db7-a452-81567aafc113' AND category = HOST";
    /**
     * 平均负载
     */
    static String AverageLoad1 = "SELECT load_1 WHERE entityName = 'abdc8768-dd3e-4554-b650-a665dee3e8ee' AND category = HOST";
    /**
     * 单台内存使用情况
     */
    static String oneSeverMemory = "SELECT physical_memory_used WHERE entityName = '5d814965-5601-4db7-a452-81567aafc113' AND category = HOST";
    /**
     * 单台cpu使用情况
     */
    static String oneServerCPU = "SELECT cpu_percent / getHostFact(numCores, 1) * 100 WHERE entityName = '5d814965-5601-4db7-a452-81567aafc113' AND category = HOST";

    /**
     * HBase 读写请求
     */
    static String HBaseReadWrite = "SELECT total_read_requests_rate_across_regionservers WHERE entityName = \"hbase\" AND category = SERVICE";

    /**
     * Hive cpu
     */
    static String HiveCPU = "SELECT cpu_system_rate + cpu_user_rate WHERE entityName = 'hive-HIVEMETASTORE-4f1d950fd679ad0763f9521fa2dcbc4e' AND category = ROLE";

    /**
     * zookeeper cpu
     */
    static String zookeeperCpu = "SELECT cpu_system_rate + cpu_user_rate WHERE entityName = 'zookeeper-SERVER-4f1d950fd679ad0763f9521fa2dcbc4e' AND category = ROLE";

    /**
     * 整个Datanodes 中的磁盘刷新
     */
    static String dataNodesDiskFlush = "SELECT flush_nanos_rate_across_datanodes WHERE entityName = 'hdfs:nameservice1' AND category = SERVICE";

    /**
     * 每个节点信息
     *
     * @param nodes
     * @return
     */
    public static JSONObject getPerServerStatus(String nodes) {
        JSONObject result = new JSONObject();
        result.put("AverageLoad", getSeriesDataList(convertStr(AverageLoad, nodes)));
        result.put("NetworkThroughput", getSeriesDataList(convertStr(NetworkThroughput, nodes)));
        result.put("AggregationDiskIOPS", getSeriesDataList(convertStr(OneServerAggregationDiskIOPS, nodes)));
        result.put("ExeState", getSeriesDataList(convertStr(OneServerExeState, nodes)));
        result.put("DiskThroughput", getSeriesDataList(convertStr(DiskThroughput, nodes)));
        String str = nodes.replace(nodes.substring(nodes.length() - 2), ":vdb' ");
        result.put("DiskLatency", getSeriesDataList(convertStr(DiskLatency, str)));

        result.put("memory", getSeriesDataList(convertStr(oneSeverMemory, nodes)));
        result.put("cpu", getSeriesDataList(convertStr(oneServerCPU, nodes)));

        return result;
    }

    /**
     * 集群信息
     *
     * @return
     */
    public static JSONObject getClusterSource() {
        JSONObject result = new JSONObject();

        result.put("cpu", getHostsUsedCpu());
        result.put("memory", getClusterHostsMemory());
        result.put("disk", getClusterHostsDisk());

        result.put("diskio", getSeriesDataList(diskioQuery));
        result.put("network", getSeriesDataList(networkQuery));
        result.put("hdfsio", getSeriesDataList(hdfsioQuery));

        result.put("dataNodesDiskFlush", getSeriesDataList(dataNodesDiskFlush));

        result.put("clusterAlertsRate", getSeriesDataList(clusterAlertsRateQuery));
        result.put("clusterEventsCriticalRate", getSeriesDataList(clusterEventsCriticalRateQuery));
        result.put("clusterEventsImportantRate", getSeriesDataList(clusterEventsImportantRateQuery));
        return result;
    }

    /**
     * 获取三组件信息 HBase  Hive  zookeeper
     *
     * @return
     */
    public static JSONObject threeModule() {
        JSONObject result = new JSONObject();
        result.put("HBaseReadWrite", getSeriesDataList(HBaseReadWrite));
        result.put("HiveCPU", getSeriesDataList(HiveCPU));
        result.put("zookeeperCpu", getSeriesDataList(zookeeperCpu));

        return result;
    }

    /**
     * 平均负载1
     *
     * @return
     */
    public static JSONObject getAverageLoad1(String nodes) {
        JSONObject result = new JSONObject();
        String node = AverageLoad1.replace(AverageLoad1.substring(AverageLoad1.indexOf("=") + 1, AverageLoad1.indexOf("AND")), nodes);
        result.put("AverageLoad", getSeriesDataList(node));
        return result;
    }


    /**
     * 主机网络吞吐量
     *
     * @return
     */
    public static JSONObject getNetworkThroughput() {
        JSONObject result = new JSONObject();
        result.put("NetworkThroughput", getSeriesDataList(NetworkThroughput));
        return result;
    }

    /**
     * 磁盘延迟
     *
     * @return
     */
    public static JSONObject getDiskLatency() {
        JSONObject result = new JSONObject();
        result.put("OneServerDiskThroughput", getSeriesDataList(DiskLatency));
        return result;
    }

    /**
     * 聚合磁盘吞吐量
     */
    public static JSONObject getOneServerDiskThroughput() {
        JSONObject result = new JSONObject();
        result.put("OneServerDiskThroughput", getSeriesDataList(OneServerDiskThroughput));
        return result;
    }

    /**
     * 单台聚合磁盘IOPS
     *
     * @return
     */
    public static JSONObject getOneServerAggregationDiskIOPS() {
        JSONObject result = new JSONObject();
        result.put("OneServerAggregationDiskIOPS", getSeriesDataList(OneServerAggregationDiskIOPS));
        return result;
    }

    /**
     * 单台服务器运行状况
     *
     * @return
     */
    public static JSONObject getOneServerExeState() {
        JSONObject result = new JSONObject();
        result.put("OneServerExeState", getSeriesDataList(OneServerExeState));
        return result;
    }

    /**
     * 单台服务器重要事件与警报
     *
     * @return
     */
    public static JSONObject getOneServerEventsImportantRate(String nodes) {
        JSONObject result = new JSONObject();
        convertStr(OneServerAlertsRateQuery, nodes);
        convertStr(OneServerEventsCriticalRateQuery, nodes);
        convertStr(OneServerEventsImportantRateQuery, nodes);
        result.put("OneServerEventsAlertsRate", getSeriesDataList(convertStr(OneServerAlertsRateQuery, nodes)));
        result.put("OneServerEventsServerCriticalRate", getSeriesDataList(convertStr(OneServerEventsCriticalRateQuery, nodes)));
        result.put("OneServerEventsImportantRate", getSeriesDataList(convertStr(OneServerEventsImportantRateQuery, nodes)));
        return result;
    }


    /**
     * 集群运行警报
     *
     * @return
     */
    public static JSONObject getClusterEvent() {
        JSONObject result = new JSONObject();
        result.put("clusterAlertsRate", getSeriesDataList(clusterAlertsRateQuery));
        result.put("clusterEventsCriticalRate", getSeriesDataList(clusterEventsCriticalRateQuery));
        result.put("clusterEventsImportantRate", getSeriesDataList(clusterEventsImportantRateQuery));
        return result;
    }

    /**
     * 集群主机内存磁盘使用情况（集群管理工具会每一分钟取一次）
     *
     * @return
     */
    public static JSONObject getClusterRes() {
        JSONObject result = new JSONObject();
        result.put("memory", getClusterHostsMemory());
        result.put("disk", getClusterHostsDisk());
        return result;
    }

    /**
     * 集群各I/O情况（集群管理工具会每一分钟取一次）
     *
     * @return
     */
    public static JSONObject getClusterIo() {
        JSONObject result = new JSONObject();
        result.put("diskio", getSeriesData(diskioQuery));
        result.put("network", getSeriesData(networkQuery));
        result.put("hdfsio", getSeriesData(hdfsioQuery));
        return result;
    }

    /**
     * 查询一个区间数据
     *
     * @param query
     * @return
     */
    private static List<JSONObject> getSeriesDataList(String query) {
        List<JSONObject> listObject = new ArrayList<JSONObject>();
        TimeSeriesResourceV11 v11 = apiRoot.getTimeSeriesResource();
        ApiTimeSeriesResponseList responsesResult = v11.queryTimeSeries(query, getTime(-100), "now");
        ApiTimeSeries timeSeries = responsesResult.getResponses().get(0).getTimeSeries().get(0);
        List<ApiTimeSeriesData> list = timeSeries.getData();
        if (list.size() > 0) {
            Double value = 0.00;
            for (int i = 0; i < list.size(); i++) {
                JSONObject dateObject = new JSONObject();
                ApiTimeSeriesData data = list.get(i);
                if (data != null) {
                    value = data.getValue();
                }
                dateObject.put("date", new SimpleDateFormat("yyyy-MM-dd HH:mm").format(data.getTimestamp()));
                dateObject.put("value", new BigDecimal(value).toPlainString());
                //获取单位
                ApiTimeSeriesMetadata metadata = timeSeries.getMetadata();
                dateObject.put("unit", metadata.getUnitNumerators().get(0));
                listObject.add(dateObject);
            }
        }
        return listObject;
    }

    /**
     * 单个数据查询
     *
     * @param query
     * @return
     */
    private static JSONObject getSeriesData(String query) {
        JSONObject result = new JSONObject();
        TimeSeriesResourceV11 v11 = apiRoot.getTimeSeriesResource();
        //查询2分钟之前的数据，集群中每一分钟统计一次，总能查到数据必须是2分钟之前，去最后一条，因为可能存在当前还未统计则查不到数据
        ApiTimeSeriesResponseList responsesResult = v11.queryTimeSeries(query, getTime(-5), "now");
        ApiTimeSeries timeSeries = responsesResult.getResponses().get(0).getTimeSeries().get(0);
        List<ApiTimeSeriesData> list = timeSeries.getData();
        String value = null;
        if (list.size() > 0) {
            ApiTimeSeriesData data = list.get(0);
            if (data != null) {
                value = String.valueOf(data.getValue());
            }
        }
        //获取单位
        ApiTimeSeriesMetadata metadata = timeSeries.getMetadata();
        // getPrintSize(new BigDecimal(value).longValue());
        // new BigDecimal(value).toPlainString()
        result.put("value", new BigDecimal(value).toPlainString());
        result.put("unit", metadata.getUnitNumerators().get(0));
        return result;
    }

    /**
     * 获取集群中所有主机内存使用量
     *
     * @return
     */
    private static JSONObject getClusterHostsMemory() {
        JSONObject result = new JSONObject();
        HostsResourceV10 hostsResource = apiRoot.getHostsResource();
        List<ApiHost> hostList = hostsResource.readHosts(DataView.SUMMARY).getHosts();
        for (ApiHost apiHost : hostList) {
            JSONObject host = new JSONObject();
            String queryUsed = "SELECT physical_memory_used WHERE hostId= " + apiHost.getHostId() + "";
            host.put("used", getSeriesData(queryUsed));
            String queryTotal = "SELECT physical_memory_total WHERE hostId= " + apiHost.getHostId() + "";
            host.put("total", getSeriesData(queryTotal));
            result.put(apiHost.getHostname(), host);
            // result.put("name",apiHost.getHostname());
            // result.put("used",getSeriesData(queryUsed));
            // result.put("total",getSeriesData(queryTotal));
            // result.put("value",host);
        }
        return result;
    }

    /**
     * 获取集群所有主机磁盘使用情况
     *
     * @return
     */
    private static JSONObject getClusterHostsDisk() {
        JSONObject result = new JSONObject();
        HostsResourceV10 hostsResource = apiRoot.getHostsResource();
        List<ApiHost> hostList = hostsResource.readHosts(DataView.SUMMARY).getHosts();
        for (ApiHost apiHost : hostList) {
            JSONObject host = new JSONObject();
            String queryUsed = "SELECT total_capacity_used_across_directories  WHERE hostId= " + apiHost.getHostId() + "";
            host.put("used", getDiskList(queryUsed));
            String queryTotal = "SELECT total_capacity_across_directories WHERE hostId= " + apiHost.getHostId() + "";
            host.put("total", getDiskList(queryTotal));
            result.put(apiHost.getHostname(), host);
        }
        return result;
    }

    /**
     * 获取集群所有主机cpu使用情况
     *
     * @return
     */
    private static JSONObject getHostsUsedCpu() {
        JSONObject result = new JSONObject();
        HostsResourceV10 hostsResource = apiRoot.getHostsResource();
        List<ApiHost> hostList = hostsResource.readHosts(DataView.SUMMARY).getHosts();
        for (ApiHost apiHost : hostList) {
            JSONObject host = new JSONObject();
            String queryUsedCpu = "SELECT cpu_percent WHERE hostId= " + apiHost.getHostId() + " AND category = HOST ";
            host.put("used", getDiskList(queryUsedCpu));
            result.put(apiHost.getHostname(), host);
        }
        return result;
    }


    private static JSONObject getDiskList(String query) {
        JSONObject result = new JSONObject();
        TimeSeriesResourceV11 v11 = apiRoot.getTimeSeriesResource();
        //查询2分钟之前的数据，集群中每一分钟统计一次，总能查到数据必须是2分钟之前，去最后一条，因为可能存在当前还未统计则查不到数据
        ApiTimeSeriesResponseList responsesResult = v11.queryTimeSeries(query, getTime(-2), "now");
        List<ApiTimeSeries> timeSeriesList = responsesResult.getResponses().get(0).getTimeSeries();
        Double value = 0.00;
        String unit = "";
        if (timeSeriesList.size() > 0) {
            for (int i = 0; i < timeSeriesList.size(); i++) {
                ApiTimeSeries timeSeries = timeSeriesList.get(i);
                List<ApiTimeSeriesData> list = timeSeries.getData();
                if (list.size() > 0) {
                    ApiTimeSeriesData data = list.get(list.size() - 1);
                    if (data != null) {
                        value = +data.getValue();
                    }
                }
                ApiTimeSeriesMetadata metadata = timeSeries.getMetadata();
                unit = metadata.getUnitNumerators().get(0);
            }
            // getPrintSize(new BigDecimal(value).longValue())
            result.put("value", new BigDecimal(value).toPlainString());
            result.put("unit", unit);
        }
        return result;
    }

    private static String getTime(int minute) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MINUTE, minute);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd&HH:mm");
        return format.format(cal.getTime()).replace("&", "T");
    }

    public static String convertStr(String str, String nodes) {
        return str.replace(str.substring(str.indexOf("=") + 1, str.indexOf("AND")), nodes);
    }

    public static String getPrintSize(long size) {
        // //如果字节数少于1024，则直接以B为单位，否则先除于1024，后3位因太少无意义
        // if (size < 1024) {
        //     return String.valueOf(size) + "B";
        // } else {
        //     size = size / 1024;
        // }
        // //如果原字节数除于1024之后，少于1024，则可以直接以KB作为单位
        // //因为还没有到达要使用另一个单位的时候
        // //接下去以此类推
        // if (size < 1024) {
        //     return String.valueOf(size) + "KB";
        // } else {
        //     size = size / 1024;
        // }
        //
        size = size / 1024 / 1024;
        if (size < 1024) {
            //因为如果以MB为单位的话，要保留最后1位小数，
            //因此，把此数乘以100之后再取余
            size = size * 100;
            return String.valueOf((size / 100)) + "."
                    + String.valueOf((size % 100));
        } else {
            //否则如果要以GB为单位的，先除于1024再作同样的处理
            size = size * 100 / 1024;
            return String.valueOf((size / 100)) + "." + String.valueOf((size % 100)) + "GB";
        }
        // size = size * 100 / 1024 / 1024 / 1024;
        // return String.valueOf((size / 100)) + "." + String.valueOf((size % 1000)) + "GB";
    }

    // public static void main(String[] args) {
    //     //     String node = " '5d814965-5601-4db7-a452-81567aafc114' ";
    //     //
    //     //     String AverageLoad1 = "SELECT service_time WHERE entityName = '5d814965-5601-4db7-a452-81567aafc113:vdb' AND category = DISK";
    //     //     String str = node.replace(node.substring(node.length() - 2), ":vdb' ");
    //     //     String and = AverageLoad1.replace(AverageLoad1.substring(AverageLoad1.indexOf("=") + 1, AverageLoad1.indexOf("AND")), str);
    //     //     // System.out.println(and);
    //     //
    //     //     BigDecimal bd = new BigDecimal(839816.533333333325572311878204345703125);
    //     //     Long l = 33566347264L;
    //     // }
}

