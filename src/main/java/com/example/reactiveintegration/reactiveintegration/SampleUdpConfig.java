package com.example.reactiveintegration.reactiveintegration;

import java.util.HashMap;
import java.util.Map;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.dsl.context.IntegrationFlowContext.IntegrationFlowRegistration;
import org.springframework.integration.ip.udp.MulticastReceivingChannelAdapter;
import org.springframework.integration.ip.udp.MulticastSendingMessageHandler;
import org.springframework.integration.ip.udp.UnicastReceivingChannelAdapter;
import org.springframework.integration.ip.udp.UnicastSendingMessageHandler;
import org.springframework.integration.support.MessageBuilder;

@Log
@Configuration
@EnableIntegration
public class SampleUdpConfig {

  private final Map<Integer, IntegrationFlowRegistration> flowRegistrations = new HashMap<>();

  @Autowired
  private IntegrationFlowContext fFlowContext;

  @Bean
  public void createUdpIn() {
    String id = "udpIn";
    int port = 8000;

    IntegrationFlow flow = IntegrationFlows
        .from(new UnicastReceivingChannelAdapter(port))
        .handle(aMessage -> {
          String data = new String((byte[]) aMessage.getPayload());
          log.info("received: " + aMessage.getHeaders() + " \n" + data);
        })
        .get();

    flowRegistrations.put(port, fFlowContext.registration(flow).id(id).register());
  }

  @Bean
  public void createMulticastUdpIn() {
    String id = "multicastUdpIn";
    String group = "225.1.1.1";
    int port = 8001;

    IntegrationFlow flow = IntegrationFlows
        .from(new MulticastReceivingChannelAdapter(group, port))
        .handle(aMessage -> {
          String data = new String((byte[]) aMessage.getPayload());
          log.info("received multicast: " + aMessage.getHeaders() + " \n" + data);
        })
        .get();

    flowRegistrations.put(port, fFlowContext.registration(flow).id(id).register());
  }

  @Bean
  public void createUdpOut() {
    String id = "udpOut";
    String host = "localhost";
    int port = 8000;

    IntegrationFlow flow = IntegrationFlows
        .from(id + "Channel")
        .handle(new UnicastSendingMessageHandler(host, port))
        .get();

    flowRegistrations.put(port, fFlowContext.registration(flow).id(id).register());

    // sending message
    IntegrationFlowRegistration udpOut = fFlowContext.getRegistrationById(id);
    udpOut.getInputChannel().send(MessageBuilder.withPayload("Sending ->> ABCD to " + id).build());
  }

  @Bean
  public void createMulticastUdpOut() {
    String id = "multicastUdpOut";
    String host = "localhost";
    int port = 8001;

    IntegrationFlow flow = IntegrationFlows
        .from(id + "Channel")
        .handle(new MulticastSendingMessageHandler(host, port))
        .get();

    flowRegistrations.put(port, fFlowContext.registration(flow).id(id).register());

    // sending message
    IntegrationFlowRegistration udpOut = fFlowContext.getRegistrationById(id);
    udpOut.getInputChannel().send(MessageBuilder.withPayload("Sending ->> ABCD to multicast " + id).build());
  }
}
