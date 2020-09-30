package io.deepstream;

import com.google.j2objc.annotations.ObjectiveCName;

import java.net.URI;

interface EndpointFactory {
    @ObjectiveCName("createEndpoint:connection:")
    Endpoint createEndpoint(URI uri, WebSocketConnection connection);
}
