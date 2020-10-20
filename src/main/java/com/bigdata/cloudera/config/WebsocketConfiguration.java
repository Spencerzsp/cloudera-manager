package com.bigdata.cloudera.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;

/**
 * @program: Cluster-Manager
 * @description
 * @author: z p、
 * @create: 2020-06-19 11:17
 **/
@Configuration
public class WebsocketConfiguration {
    @Bean
    public ServerEndpointExporter serverEndpointExporter() {
        return new ServerEndpointExporter();
    }
}
