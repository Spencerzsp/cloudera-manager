package com.bigdata.cloudera;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ClusterApplication {

    // @Autowired
    // cloudermanager2mysql server;
    // @Autowired
    // yarnJobUtils yarn;

    public static void main(String[] args) {
        SpringApplication.run(ClusterApplication.class, args);
        // new SpringApplicationBuilder(ClusterApplication.class)
        //         .web(WebApplicationType.NONE)
        //         .bannerMode(Banner.Mode.OFF)
        //         .run(args);

    }

    // @Override
    // public void addCorsMappings(CorsRegistry registry) {
    //     registry.addMapping("/**")
    //             .allowCredentials(true)
    //             .allowedHeaders("*")
    //             .allowedOrigins("*")
    //             .allowedMethods("*");
    // }
    // @Override
    // public void run(ApplicationArguments args) {
    //     while (true) {
    //         server.clusterCPU2Mysql();
    //         server.clusterEventWarningData2Mysql();
    //         server.clusterIO2MySQL();
    //         server.clusterRes2Mysql();
    //         try {
    //             Thread.sleep(60000);
    //         } catch (InterruptedException e) {
    //             e.printStackTrace();
    //         }
    //     }
    // }
}
