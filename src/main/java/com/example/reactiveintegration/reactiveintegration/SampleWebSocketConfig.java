package com.example.reactiveintegration.reactiveintegration;

import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

@Configuration
public class SampleWebSocketConfig {

  @Autowired
  private WebSocketHandler webSocketHandler;

  @Bean
  public HandlerMapping webSocketHandlerMapping() {
    Map<String, WebSocketHandler> map = new HashMap<>();
    map.put("/stream", webSocketHandler);

    SimpleUrlHandlerMapping handlerMapping = new SimpleUrlHandlerMapping();
    handlerMapping.setOrder(1);
    handlerMapping.setUrlMap(map);
    return handlerMapping;
  }

  @Bean
  public WebSocketHandlerAdapter handlerAdapter() {
    return new WebSocketHandlerAdapter();
  }

  @Component
  class ReactiveWebSocketHandler implements WebSocketHandler {
    private final Flux<String> fMessage;
    private final UnicastProcessor<String> fPublisher;

    ReactiveWebSocketHandler() {
      fPublisher = UnicastProcessor.create();
      fMessage = fPublisher.publish().autoConnect();
      fMessage.subscribe();
    }

    @Bean
    public MessageChannel webSocketFromTcpChannel() {
      return new DirectChannel();
    }

    @ServiceActivator(inputChannel = "webSocketFromTcpChannel")
    public void incomingFromTcpClientMessage(Message<?> aMessage) {
      String incomingMessage = new String((byte[]) aMessage.getPayload());
      System.out.println("incoming websocket: " + incomingMessage);

      fPublisher.onNext(incomingMessage);
    }

    @Override
    public Mono<Void> handle(WebSocketSession aWebSocketSession) {
      Mono<Void> input = aWebSocketSession.receive()
          .doOnNext(aMessage -> {
            System.out.println("received websocket: " + aMessage.getPayloadAsText());
          }).then();

      Mono<Void> output = aWebSocketSession.send(fMessage.map(aWebSocketSession::textMessage));

      return Mono.zip(input, output).then();
    }
  }
}