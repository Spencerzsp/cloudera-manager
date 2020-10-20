package com.bigdata.cloudera.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.cloudera.service.ClouderMangerUtlis;
import com.bigdata.cloudera.service.WebSocketServer;
import com.bigdata.cloudera.service.YarnJobService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

// @CrossOrigin(origins = "*")
@RestController
// @ControllerAdvice(basePackages = {"com.wuben.controller"})
public class ClouderMangerController {

    @Autowired
    WebSocketServer socketServer;

    @Autowired
    YarnJobService yarnService;

    //页面请求
//    @GetMapping("/index/{userId}")
//    public ModelAndView socket(@PathVariable String userId) {
//        ModelAndView mav = new ModelAndView("/socket1");
//        mav.addObject("userId", userId);
//        return mav;
//    }

    //每台服务器 15min 信息
    @RequestMapping(value = "/bigscreen/perServerStatus/{nodes}")
    JSONArray getPerServerStatus(@PathVariable String nodes) {
        JSONArray array = new JSONArray();

        Map<String, String> map = new HashMap<>();
        map.put("wbbigdata00", " '5d814965-5601-4db7-a452-81567aafc113' ");
        map.put("wbbigdata01", " 'abdc8768-dd3e-4554-b650-a665dee3e8ee' ");
        map.put("wbbigdata02", " 'f75c68ae-266d-49cf-ad32-17d5bf2bfaef' ");

        String node = map.get(nodes);
        JSONObject value = new JSONObject();
        value.put("code", 100);
        value.put("message", "Succeed");
        array.add(value);
        array.add(ClouderMangerUtlis.getPerServerStatus(node));
        array.add(ClouderMangerUtlis.getOneServerEventsImportantRate(node));

        return array;
    }

    //集群资源  CPU io disk 重要事件与警报 ...
    @RequestMapping("/bigscreen/clusterSource")
    JSONArray getClusterSource() {

        JSONArray array = new JSONArray();
        JSONObject mess = new JSONObject();
        mess.put("code", 100);
        mess.put("message", "Succeed");
        array.add(mess);
        JSONObject constantSource = new JSONObject();
        constantSource.put("磁盘总量", "256T");
        constantSource.put("内存总量", "96GB");
        constantSource.put("cpu核数", "12核");
        array.add(constantSource);
        array.add(ClouderMangerUtlis.getClusterSource());
        // array.add(ClouderMangerUtlis.getClusterEvent());
        return array;
    }

    //yarn app 执行信息
    // @CrossOrigin
    @RequestMapping("/running")
    JSONArray getYarnJob() {
        JSONArray array = new JSONArray();
        JSONObject mess = new JSONObject();
        mess.put("CODE", 100);
        mess.put("MESSAGE", "SUCCEEDED");
        array.add(mess);
        array.add(yarnService.getApp());
        return array;
    }

    @RequestMapping("/finished")
    JSONArray getFinished() {
        JSONArray array = new JSONArray();
        JSONObject mess = new JSONObject();
        mess.put("CODE", 100);
        mess.put("MESSAGE", "SUCCEEDED");
        array.add(mess);
        array.add(yarnService.getFinishedApp());
        return array;
    }

    @RequestMapping("/threeModule")
    JSONArray getThreeModule() {
        JSONArray array = new JSONArray();
        JSONObject mess = new JSONObject();
        mess.put("CODE", 100);
        mess.put("MESSAGE", "SUCCEEDED");
        array.add(mess);
        array.add(ClouderMangerUtlis.threeModule());
        return array;
    }

    // //推送数据接口
    // @ResponseBody
    // @RequestMapping("/socket/push/{cid}")
    // public JSONArray pushToWeb(@PathVariable String cid, String message) {
    //     JSONArray array = new JSONArray();
    //
    //     Map<String, Object> map = new HashMap<>();
    //     map.put("wbbigdata00", " '5d814965-5601-4db7-a452-81567aafc113' ");
    //     map.put("wbbigdata01", " 'abdc8768-dd3e-4554-b650-a665dee3e8ee' ");
    //     map.put("wbbigdata02", " 'f75c68ae-266d-49cf-ad32-17d5bf2bfaef' ");
    //     try {
    //         String node = String.valueOf(map.get(cid));
    //         JSONObject value = new JSONObject();
    //         value.put("code", 200);
    //         value.put("msg", "success");
    //
    //         array.add(value);
    //         array.add(ClouderMangerUtlis.getPerServerStatus(node));
    //         array.add(ClouderMangerUtlis.getOneServerEventsImportantRate(node));
    //         WebSocketServer.sendInfo(array.toJSONString(), cid);
    //     } catch (IOException e) {
    //         e.printStackTrace();
    //     }
    //     return array;
    // }
    //每台服务器重要事件和警报
    // @RequestMapping("/perServerEvent")
    // JSONObject getOneServerEvent() {
    //     return ClouderMangerUtlis.getOneServerEventsImportantRate();
    // }
    // @RequestMapping("/clusterRes")
    // JSONObject getClusterRes() {
    //     return ClouderMangerUtlis.getClusterRes();
    // }
    // @RequestMapping("/clusterIo")
    // JSONObject getClusterIo() {
    //     return ClouderMangerUtlis.getClusterIo();
    // }
    // @RequestMapping("/AverageLoad")
    // JSONObject getAverageLoad() {
    //     return ClouderMangerUtlis.getAverageLoad();
    // }
    // @RequestMapping("/NetworkThroughput")
    // JSONObject getNetworkThroughput() {
    //     return ClouderMangerUtlis.getNetworkThroughput();
    // }
    //
    //
    // @RequestMapping("/OneServerDiskThroughput")
    // JSONObject getOneServerDiskThroughput() {
    //     return ClouderMangerUtlis.getOneServerDiskThroughput();
    // }
    //
    // @RequestMapping("/OneServerIOPS")
    // JSONObject getOneServerAggregationDiskIOPS() {
    //     return ClouderMangerUtlis.getOneServerAggregationDiskIOPS();
    // }
    //
    // @RequestMapping("/OneServerExeState")
    // JSONObject getOneServerExeState() {
    //     return ClouderMangerUtlis.getOneServerExeState();
    // }
    // @RequestMapping("/DiskLatency")
    // JSONObject getDiskLatency() {
    //     return ClouderMangerUtlis.getDiskLatency();
    // }
    //集群事件
    // @RequestMapping("/clusterEvent")
    // JSONObject getClusterEvent() {
    //     return ClouderMangerUtlis.getClusterEvent();
    // }
}
