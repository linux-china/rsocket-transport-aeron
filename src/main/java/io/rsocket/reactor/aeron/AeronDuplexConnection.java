package io.rsocket.reactor.aeron;

import io.netty.buffer.ByteBuf;
import io.rsocket.DuplexConnection;
import org.reactivestreams.Publisher;
import reactor.aeron.AeronConnection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class AeronDuplexConnection implements DuplexConnection {

    private final AeronConnection connection;
    private final FrameMapper frameMapper = new FrameMapper();

    public AeronDuplexConnection(AeronConnection connection) {
        this.connection = connection;
    }

    @Override
    public Mono<Void> send(Publisher<ByteBuf> frames) {
        return connection.outbound().send(Flux.from(frames), frameMapper).then();
    }

    @Override
    public Mono<Void> sendOne(ByteBuf frame) {
        return connection.outbound().send(Mono.just(frame), frameMapper).then();
    }

    @Override
    public Flux<ByteBuf> receive() {
        return connection.inbound().receive().map(frameMapper);
    }

    @Override
    public Mono<Void> onClose() {
        return connection.onDispose();
    }

    @Override
    public void dispose() {
        connection.dispose();
    }

    @Override
    public boolean isDisposed() {
        return connection.isDisposed();
    }
}
