package io.rsocket.reactor.aeron;

import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.uri.UriHandler;
import reactor.aeron.AeronClient;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronServer;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;

/**
 * Aeron uri handler, format as aeron://127.0.0.1:12000
 *
 * @author linux_china
 */
public class AeronUriHandler implements UriHandler {
    private static final String SCHEME = "aeron";

    @Override
    public Optional<ClientTransport> buildClient(URI uri) {
        Objects.requireNonNull(uri, "uri must not be null");
        if (!SCHEME.equals(uri.getScheme())) {
            return Optional.empty();
        }
        AeronResources aeronResources = new AeronResources().useTmpDir().singleWorker().start().block();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("RSocket Client Closed: " + uri);
            aeronResources.dispose();
            aeronResources.onDispose().block();
        }));
        AeronClient aeronClient = AeronClient.create(aeronResources).options(uri.getHost(), uri.getPort(), uri.getPort() + 1);
        return Optional.of(new AeronClientTransport(aeronClient));
    }

    @Override
    public Optional<ServerTransport> buildServer(URI uri) {
        Objects.requireNonNull(uri, "uri must not be null");
        if (!SCHEME.equals(uri.getScheme())) {
            return Optional.empty();
        }
        AeronResources aeronResources = new AeronResources().useTmpDir().singleWorker().start().block();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("RSocket Server Closed: " + uri);
            aeronResources.dispose();
            aeronResources.onDispose().block();
        }));
        return Optional.of(new AeronServerTransport(AeronServer.create(aeronResources).options(uri.getHost(), uri.getPort(), uri.getPort() + 2)));
    }
}
