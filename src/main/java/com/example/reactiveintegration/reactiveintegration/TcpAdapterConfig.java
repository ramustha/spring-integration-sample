package com.example.reactiveintegration.reactiveintegration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.FluxMessageChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.ip.IpHeaders;
import org.springframework.integration.ip.dsl.Tcp;
import org.springframework.integration.ip.tcp.TcpReceivingChannelAdapter;
import org.springframework.integration.ip.tcp.TcpSendingMessageHandler;
import org.springframework.integration.ip.tcp.connection.AbstractClientConnectionFactory;
import org.springframework.integration.ip.tcp.connection.AbstractServerConnectionFactory;
import org.springframework.integration.ip.tcp.connection.TcpConnectionCloseEvent;
import org.springframework.integration.ip.tcp.connection.TcpConnectionEvent;
import org.springframework.integration.ip.tcp.connection.TcpConnectionOpenEvent;
import org.springframework.integration.router.RecipientListRouter;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import reactor.core.publisher.Flux;

@Configuration
@EnableIntegration
@IntegrationComponentScan
public class TcpAdapterConfig {

  private final Map<String, TcpConnectionEvent> fServerConnectionEvent = new HashMap<>();

  @Bean
  public MessageChannel loopServerNioChannel() {
    return new DirectChannel();
  }

  @Bean
  public IntegrationFlow createTcpNioServer() {
    AbstractServerConnectionFactory connectionFactory = Tcp.nioServer(10000).get();

    connectionFactory.setApplicationEventPublisher(aEvent -> {
      System.out.println("Tcp Server Event: " + aEvent);

      if (aEvent instanceof TcpConnectionEvent) {
        TcpConnectionEvent connectionEvent = (TcpConnectionEvent) aEvent;
        String connectionId = connectionEvent.getConnectionId();

        if (aEvent instanceof TcpConnectionOpenEvent) {
          fServerConnectionEvent.put(connectionId, connectionEvent);
        } else if (aEvent instanceof TcpConnectionCloseEvent) {
          fServerConnectionEvent.remove(connectionEvent.getConnectionId());
        }

        Flux.interval(Duration.ofMillis(500))
            .flatMap(aLong -> {
              TcpConnectionEvent event = fServerConnectionEvent.get(connectionId);
              if (event != null) {
                loopServerNioChannel().send(
                    MessageBuilder
                        .withPayload("replay for " + connectionEvent.getConnectionId())
                        .setHeader(IpHeaders.CONNECTION_ID, event.getConnectionId())
                        .build());
              }
              return Flux.empty();
            })
            .subscribe();

      }
    });

    TcpReceivingChannelAdapter receivingChannelAdapter = new TcpReceivingChannelAdapter();
    receivingChannelAdapter.setConnectionFactory(connectionFactory);
    receivingChannelAdapter.setOutputChannel(loopServerNioChannel());

    TcpSendingMessageHandler sendingMessageHandler = new TcpSendingMessageHandler();
    sendingMessageHandler.setConnectionFactory(connectionFactory);

    return IntegrationFlows.from(receivingChannelAdapter).handle(sendingMessageHandler).get();
  }

  @ServiceActivator(inputChannel = "loopServerNioChannel")
  public void incomingServerMessage(Message<?> aMessage) {
    String incomingMessage = new String((byte[]) aMessage.getPayload());
    System.out.println("incoming server: " + incomingMessage);
  }

  @Bean
  public MessageChannel inboundClientNioChannel() {
    return new FluxMessageChannel();
  }

  @Bean
  public IntegrationFlow createTcpNioClient() {
    AbstractClientConnectionFactory connectionFactory = Tcp.nioClient("localhost", 8051).get();
    connectionFactory.setApplicationEventPublisher(aEvent -> {
      System.out.println("Tcp Client Event: " + aEvent);
    });

    TcpReceivingChannelAdapter receivingChannelAdapter = new TcpReceivingChannelAdapter();
    receivingChannelAdapter.setClientMode(true);
    receivingChannelAdapter.setConnectionFactory(connectionFactory);
    receivingChannelAdapter.setOutputChannel(inboundClientNioChannel());

    return IntegrationFlows.from(receivingChannelAdapter).route(routerTcpToWebSocket()).get();
  }

  @ServiceActivator(inputChannel = "inboundClientNioChannel")
  public void incomingClientMessage(Message<?> aMessage) {
    String incomingMessage = new String((byte[]) aMessage.getPayload());
    System.out.println("incoming client: " + incomingMessage);

    fServerConnectionEvent.values().forEach(aEvent -> {
      loopServerNioChannel().send(
          MessageBuilder
              .withPayload(incomingMessage)
              .setHeader(IpHeaders.CONNECTION_ID, aEvent.getConnectionId())
              .build());
    });
  }

  public RecipientListRouter routerTcpToWebSocket(){
    RecipientListRouter router = new RecipientListRouter();
    router.addRecipient("webSocketFromTcpChannel");
    return router;
  }
}
