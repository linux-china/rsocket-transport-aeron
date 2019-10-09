package io.rsocket.reactor.aeron;

import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import reactor.aeron.AeronClient;
import reactor.core.publisher.Mono;

public class AeronClientTransport implements ClientTransport {

  private final AeronClient client;

  public AeronClientTransport(AeronClient client) {
    this.client = client;
  }

  @Override
  public Mono<DuplexConnection> connect(int mtu) {
    return client.connect().map(AeronDuplexConnection::new);
  }
}
