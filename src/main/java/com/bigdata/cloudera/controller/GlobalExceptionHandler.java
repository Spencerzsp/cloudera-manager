package com.bigdata.cloudera.controller;// package com.wuben.clustermanager.controller;
//
// import org.springframework.web.bind.annotation.ControllerAdvice;
// import org.springframework.web.bind.annotation.ExceptionHandler;
//
// import javax.servlet.http.HttpServletResponse;
// import java.io.IOException;
// import java.io.PrintWriter;
//
// /**
//  * @program: Cluster-Manager
//  * @description
//  * @author: z p、
//  * @create: 2020-04-27 17:50
//  **/
// @ControllerAdvice
// public class GlobalExceptionHandler {
//
//     @ExceptionHandler(NullPointerException.class)
//     public void nullPointerException(NullPointerException e, HttpServletResponse response) throws IOException {
//         // response.setContentType("text/html;charset-utf-8");
//         PrintWriter writer = response.getWriter();
//         writer.write("{\"Code\":400,\"Message\":\"No such node\"}");
//         writer.flush();
//         writer.close();
//     }
//
//     @ExceptionHandler(Exception.class)
//     public void runtimeException(Exception e, HttpServletResponse response) throws IOException {
//         // response.setContentType("text/html;charset-utf-8");
//         PrintWriter out = response.getWriter();
//         out.write("{\"Code\":500,\"Message\":\"runtimeException\"}");
//         out.flush();
//         out.close();
//     }
// }
