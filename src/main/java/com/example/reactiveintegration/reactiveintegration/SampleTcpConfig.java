package com.example.reactiveintegration.reactiveintegration;

import java.util.HashMap;
import java.util.Map;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.ip.tcp.TcpInboundGateway;
import org.springframework.integration.ip.tcp.connection.TcpNioClientConnectionFactory;
import org.springframework.integration.ip.tcp.connection.TcpNioServerConnectionFactory;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.PollableChannel;

import static org.springframework.integration.dsl.context.IntegrationFlowContext.IntegrationFlowRegistration;

@Log
@Configuration
@EnableIntegration
public class SampleTcpConfig {

  private final Map<Integer, IntegrationFlowRegistration> flowRegistrations = new HashMap<>();

  @Autowired
  private IntegrationFlowContext fFlowContext;

  @Bean
  public void createTcpServer() {
    String id = "tcpServer";
    int port = 9000;

    DirectChannel replyChannel = new DirectChannel();

    TcpNioServerConnectionFactory connectionFactory = new TcpNioServerConnectionFactory(port);

    TcpInboundGateway inboundGateway = new TcpInboundGateway();
    inboundGateway.setConnectionFactory(connectionFactory);
    inboundGateway.setReplyChannel(replyChannel);

    IntegrationFlow flow = IntegrationFlows
        .from(inboundGateway)
        .handle(aMessage -> {
          String data = new String((byte[]) aMessage.getPayload());
          log.info("received: " + aMessage.getHeaders() + " \n" + data);

          replyChannel.send(MessageBuilder.withPayload("From server " + data).copyHeaders(aMessage.getHeaders()).build());
        })
        .get();

    flowRegistrations.put(port, fFlowContext.registration(flow).id(id).register());
  }

  @Bean
  public void createTcpClient() {
    String id = "tcpClient";
    String host = "localhost";
    int port = 9001;

    PollableChannel replyChannel = new QueueChannel();

    TcpNioClientConnectionFactory connectionFactory = new TcpNioClientConnectionFactory(host, port);

    TcpInboundGateway inboundGateway = new TcpInboundGateway();
    inboundGateway.setConnectionFactory(connectionFactory);
    inboundGateway.setReplyChannel(replyChannel);
    inboundGateway.setClientMode(true);

    IntegrationFlow flow = IntegrationFlows
        .from(inboundGateway)
        .handle(aMessage -> {
          String data = new String((byte[]) aMessage.getPayload());
          log.info("received: " + aMessage.getHeaders() + " \n" + data);

          replyChannel.send(MessageBuilder.withPayload("From client " + data).copyHeaders(aMessage.getHeaders()).build());
        })
        .get();

    flowRegistrations.put(port, fFlowContext.registration(flow).id(id).register());
  }
}
