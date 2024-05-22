package com.salapp;

import com.salapp.services.OrderService;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMetadata;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class ProductConsumer {

    private static final Logger log = LoggerFactory.getLogger(ProductConsumer.class);

    private final OrderService orderService;

    @Inject
    public ProductConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @Incoming("quarkus-rabbitmq")
    public CompletionStage<Void> processMessage(Message<JsonObject> messageIncoming) {
        log.info("Received message with payload: {}", messageIncoming.getPayload());

        messageIncoming.getMetadata(IncomingRabbitMQMetadata.class).ifPresentOrElse(
                this::logMetadata,
                () -> log.warn("No metadata found in message")
        );

        return extractOrder(messageIncoming.getPayload())
                .thenCompose(this::saveOrder)
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

    public record Order(int orderId, int customerId, int productId, int quantity) {

    }

}
