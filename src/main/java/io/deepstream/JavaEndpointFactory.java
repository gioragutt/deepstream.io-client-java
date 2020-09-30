package io.deepstream;

import java.net.URI;

class JavaEndpointFactory implements EndpointFactory {

    @Override
    public Endpoint createEndpoint(URI uri, WebSocketConnection connection) {
        return new JavaEndpointWebsocket(uri, connection);
    }
}
