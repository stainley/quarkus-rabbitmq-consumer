package com.salapp;

import com.salapp.services.OrderService;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMetadata;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.json.Json;
import jakarta.ws.rs.sse.SseBroadcaster;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.resteasy.reactive.server.jaxrs.OutboundSseEventImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class ProductConsumer {

    private static final Logger log = LoggerFactory.getLogger(ProductConsumer.class);

    private final OrderService orderService;
    private SseBroadcaster broadcaster;

    @Inject
    public ProductConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @Incoming("quarkus-rabbitmq")
    public CompletionStage<Void> processMessage(Message<JsonObject> messageIncoming) {
        log.info("Received message with payload: {}", messageIncoming.getPayload());

        messageIncoming.getMetadata(IncomingRabbitMQMetadata.class).ifPresentOrElse(
                this::logMetadata, () -> log.warn("No metadata found in message")
        );

        // Notify changes
        return extractOrder(messageIncoming.getPayload())
                .thenCompose(order -> {
                    emitOrder(order);
                    return saveOrder(order);
                })
                .exceptionally(ex -> {
                    log.error("Error processing message", ex);
                    return null;
                });
    }

    private CompletableFuture<Order> extractOrder(JsonObject payload) {
        try {
            int orderId = payload.getInteger("orderId", -1);
            int customerId = payload.getInteger("customerId", -1);
            int productId = payload.getInteger("productId", -1);
            int quantity = payload.getInteger("quantity", -1);

            Order order = new Order(orderId, customerId, productId, quantity);
            log.info("Extracted Order: {}", order);
            return CompletableFuture.completedFuture(order);
        } catch (Exception e) {
            log.error("Failed to extract order from payload", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    private void logMetadata(IncomingRabbitMQMetadata metadata) {
        log.info("Content Type: {}", metadata.getContentType());
        log.info("Timestamp: {}", metadata.getTimestamp(ZoneId.of("America/Toronto")));
        log.info("Author Header: {}", metadata.getHeader("author-header", String.class));
        log.info("Correlation ID: {}", metadata.getCorrelationId());
    }

    private CompletionStage<Void> saveOrder(Order order) {
        return CompletableFuture.runAsync(() -> {
            try {
                orderService.save(order);
                log.info("Order saved: {}", order);
            } catch (Exception e) {
                log.error("Error saving order", e);
                throw new RuntimeException(e);
            }
        });
    }

    private void emitOrder(Order order) {

        log.info("Will emitting Order: {}", order);

        if (broadcaster != null) {
            jakarta.json.JsonObject jsonOrder = Json.createObjectBuilder()
                    .add("orderId", order.orderId())
                    .add("customerId", order.customerId())
                    .add("productId", order.productId())
                    .add("quantity", order.quantity())
                    .build();

            log.info("JSON order: {}", jsonOrder);

            OutboundSseEventImpl event = new OutboundSseEventImpl.BuilderImpl()
                    .data(jsonOrder)
                    .build();
            broadcaster.broadcast(event);
        } else {
            log.warn("Broadcaster not set");
        }
    }

    public void registerBroadcaster(SseBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    public record Order(int orderId, int customerId, int productId, int quantity) {

    }

}
