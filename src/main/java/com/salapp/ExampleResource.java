package com.salapp;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.sse.Sse;
import jakarta.ws.rs.sse.SseBroadcaster;
import jakarta.ws.rs.sse.SseEventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;

@Path("/product-updates")
public class ExampleResource {

    private static final Logger log = LoggerFactory.getLogger(ExampleResource.class);
    private final ProductConsumer productConsumer;
    private SseBroadcaster broadcaster;

    @Inject
    public ExampleResource(ProductConsumer productConsumer, Sse sse) {
        this.productConsumer = productConsumer;
        this.broadcaster = sse.newBroadcaster();
        productConsumer.registerBroadcaster(broadcaster);

    }

    @GET
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public SseEmitter sendProductSSE(@Context SseEventSink eventSink) {
        SseEmitter emitter = new SseEmitter();
        broadcaster.register(eventSink);

        log.info("Emitter created and registered");

        return emitter;
    }
}
