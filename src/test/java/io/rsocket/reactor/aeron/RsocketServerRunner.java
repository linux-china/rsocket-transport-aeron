package io.rsocket.reactor.aeron;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocketFactory;
import io.rsocket.uri.UriTransportRegistry;
import io.rsocket.util.ByteBufPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class RsocketServerRunner {

    public static void main(String[] args) throws Exception {
        // start server
        RSocketFactory.receive()
                .acceptor(
                        (setup, reactiveSocket) ->
                                Mono.just(
                                        new AbstractRSocket() {

                                            @Override
                                            public Mono<Void> fireAndForget(Payload payload) {
                                                System.err.println(
                                                        "fireAndForget(), receive request: " + payload.getDataUtf8());
                                                return super.fireAndForget(payload);
                                            }

                                            @Override
                                            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                                                System.err.println("requestChannel(), receive request: ");
                                                return super.requestChannel(payloads);
                                            }

                                            @Override
                                            public Mono<Void> metadataPush(Payload payload) {
                                                System.err.println(
                                                        "metadataPush(), receive request: " + payload.getDataUtf8());
                                                return super.metadataPush(payload);
                                            }

                                            @Override
                                            public Mono<Payload> requestResponse(Payload payload) {
                                                System.err.println(
                                                        "requestResponse(), receive request: " + payload.getDataUtf8());
                                                return Mono.just(payload);
                                            }

                                            @Override
                                            public Flux<Payload> requestStream(Payload payload) {
                                                System.err.println(
                                                        "requestStream(), receive request: " + payload.getDataUtf8());
                                                return Flux.interval(Duration.ofMillis(100))
                                                        .log("send back ")
                                                        .map(aLong -> ByteBufPayload.create("Interval: " + aLong));
                                            }
                                        }))
                .transport(UriTransportRegistry.serverForUri("aeron://localhost:12000"))
                .start()
                .subscribe();

        System.err.println("wait for the end");
        Thread.currentThread().join();
    }
}
