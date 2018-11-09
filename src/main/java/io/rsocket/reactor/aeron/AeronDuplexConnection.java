package io.rsocket.reactor.aeron;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOutbound;

public class AeronDuplexConnection implements DuplexConnection {

  private final MonoProcessor<Void> onClose;
  private final AeronInbound inbound;
  private final AeronOutbound outbound;

  public AeronDuplexConnection(AeronInbound inbound, AeronOutbound outbound) {
    this.inbound = inbound;
    this.outbound = outbound;
    this.onClose = MonoProcessor.create();
    // todo: onClose.doFinally(signalType -> { doSomething() }).subscribe();
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frames) {

    //    return Flux.from(frames).collectMap(this::sendOne).then();

    return outbound
        .send(
            Flux.from(frames)
                .log("DuplexConn send -> ")
                .map(Frame::content)
                .map(ByteBuf::nioBuffer))
        .then()
        .log("DuplexConn send then -> ");
  }

  @Override
  public Mono<Void> sendOne(Frame frame) {
    return send(Mono.just(frame)
        .log("DuplexConn sendOne -> "))
        .then();
  }

  @Override
  public Flux<Frame> receive() {
    return inbound
        .receive()
        .map(Unpooled::wrappedBuffer)
        .map(Frame::from)
        .log("DuplexConn receive -> ");
  }

  @Override
  public Mono<Void> onClose() {
    return onClose.log(" DuplexConn onClose ");
  }

  @Override
  public void dispose() {
    System.err.println("DuplexConn dispose ");
    if (!onClose.isDisposed()) {
      onClose.onComplete();
    }
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }
}
