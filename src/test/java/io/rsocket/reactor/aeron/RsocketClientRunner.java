package io.rsocket.reactor.aeron;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.uri.UriTransportRegistry;
import io.rsocket.util.ByteBufPayload;
import reactor.core.publisher.Flux;

public class RsocketClientRunner {

    public static void main(String[] args) throws Exception {
        final RSocket rSocket = RSocketFactory.connect()
                .transport(UriTransportRegistry.clientForUri("aeron://localhost:12000"))
                .start()
                .block();
        System.err.println("start " + rSocket);
        Flux.range(0, 10)
                .flatMap(i ->
                        rSocket.requestResponse(ByteBufPayload.create("Hello_" + i))
                                .log("receive ")
                                .map(Payload::getDataUtf8)
                                .doOnNext(System.out::println)
                                .then())
                .subscribe();
        System.err.println("wait for the end");
        Thread.sleep(2000);
        rSocket.dispose();
        rSocket.onClose().block();
        System.exit(0);
    }
}
