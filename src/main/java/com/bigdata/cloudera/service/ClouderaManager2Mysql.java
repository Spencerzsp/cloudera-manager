package com.bigdata.cloudera.service;// package com.wuben.clustermanager.service;
//
// import com.alibaba.fastjson.JSON;
// import com.alibaba.fastjson.JSONArray;
// import com.alibaba.fastjson.JSONObject;
// import com.wuben.clustermanager.bean.totalMapBean;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.jdbc.core.JdbcTemplate;
// import org.springframework.stereotype.Service;
//
// import java.sql.Timestamp;
// import java.util.Date;
// import java.util.Map;
//
// /**
//  * @program: Cluster-Manager
//  * @description
//  * @author: z p、
//  * @create: 2020-04-08 11:23
//  **/
// @Service
// public class cloudermanager2mysql {
//
//     @Autowired
//     JdbcTemplate jdbcTemplate;
//
//     private final Logger logger = LoggerFactory.getLogger(cloudermanager2mysql.class);
//
//     /**
//      * io 2 mysql
//      */
//     public void clusterIO2MySQL() {
//         logger.info("开始解析集群io json字符串。");
//         Map<String, Object> map = ClouderMangerUtlis.getClusterIo();
//         for (Map.Entry<String, Object> source : map.entrySet()) {
//             String[] split = source.getValue().toString().split(",");
//             String unit = split[0].substring(split[0].indexOf(":") + 2, split[0].length() - 1);
//             String value = split[1].substring(split[1].indexOf(":") + 2, split[1].length() - 2);
//             Date date = new Date();
//             Timestamp timestamp = new Timestamp(date.getTime());
//             String sql = "INSERT INTO cluster_io(io_type,unit,value,date) values (?,?,?,?)";
//             jdbcTemplate.update(sql, source.getKey(), unit, value, timestamp);
//         }
//         logger.info("集群 io 数据写入MySQL成功。");
//     }
//
//     /**
//      * cpu 2 mysql
//      */
//     public void clusterCPU2Mysql() {
//         logger.info("开始解析集群cpu json字符串。");
//         Map<String, Object> map = ClouderMangerUtlis.getClusterCPU();
//         for (Map.Entry<String, Object> source : map.entrySet()) {
//             JSONObject nodesJSON = JSON.parseObject(source.getValue().toString());
//             for (Map.Entry<String, Object> nodes : nodesJSON.entrySet()) {
//                 String node = nodes.getKey();
//                 JSONObject usedJson = JSON.parseObject(nodes.getValue().toString());
//                 for (Map.Entry<String, Object> used : usedJson.entrySet()) {
//                     String usedValue = used.getValue().toString();
//                     String[] split = usedValue.split(",");
//                     String unit = split[0].substring(split[0].indexOf(":") + 2, split[0].length() - 1);
//                     String value = split[1].substring(split[1].indexOf(":") + 2, split[1].length() - 2);
//                     Date date = new Date();
//                     Timestamp timestamp = new Timestamp(date.getTime());
//                     String sql = "INSERT INTO cluster_cpu(nodes,unit,value,date) values (?,?,?,?)";
//                     jdbcTemplate.update(sql, node, unit, value, timestamp);
//
//                 }
//             }
//         }
//         logger.info("集群cpu写入MySQL成功。");
//     }
//
//     /**
//      * 集群主机内存磁盘 to mysql
//      */
//     public void clusterRes2Mysql() {
//         logger.info("开始解析主机内存磁盘json字符串。");
//         Map<String, Object> map = ClouderMangerUtlis.getClusterRes();
//         for (Map.Entry<String, Object> sources : map.entrySet()) {
//             String source = sources.getKey();
//             JSONObject jsonNodes = JSON.parseObject(sources.getValue().toString());
//             for (Map.Entry<String, Object> nodes : jsonNodes.entrySet()) {
//                 String node = nodes.getKey();
//                 totalMapBean totalMapBean = JSON.parseObject(nodes.getValue().toString(), totalMapBean.class);
//                 String totalUnit = totalMapBean.getTotal().getUnit();
//                 String totalValue = totalMapBean.getTotal().getValue();
//                 String usedUnit = totalMapBean.getUsed().getUnit();
//                 String usedValue = totalMapBean.getUsed().getValue();
//                 Date date = new Date();
//                 Timestamp timestamp = new Timestamp(date.getTime());
//                 String sql1 = "INSERT INTO cluster_details(source,nodeName,total_unit,total_value,used_unit,used_value,date) values(?,?,?,?,?,?,?)";
//                 jdbcTemplate.update(sql1, source, node, totalUnit, totalValue, usedUnit, usedValue, timestamp);
//             }
//         }
//         logger.info("主机内存磁盘数据写入MySQL 成功。");
//     }
//
//     /**
//      * 集群警告信息 to MySQL
//      */
//     public void clusterEventWarningData2Mysql() {
//         logger.info("开始解析集群警告信息json字符串。");
//         JSONObject jsonObj = ClouderMangerUtlis.getClusterEvent();
//         JSONObject jsonObject = JSONObject.parseObject(jsonObj.toJSONString());
//         String str = "clusterEventsImportantRate,clusterEventsCriticalRate,clusterAlertsRate";
//         String[] splits = str.split(",");
//         for (String split : splits) {
//             JSONArray jsonArray = (JSONArray) jsonObject.get(split);
//             for (int i = 0; i < jsonArray.size(); i++) {
//                 JSONObject jsonArrayJSONObject = jsonArray.getJSONObject(i);
//                 String date = (String) jsonArrayJSONObject.get("date");
//                 String unit = (String) jsonArrayJSONObject.get("unit");
//                 String value = jsonArrayJSONObject.get("value").toString();
//                 String sql1 = "INSERT INTO cluster_event_warning(cluster_event_name,date,unit,value) values(?,?,?,?)";
//                 jdbcTemplate.update(sql1, split, date, unit, value);
//             }
//         }
//         logger.info("集群警告信息写入MySQL成功。");
//     }
// }
