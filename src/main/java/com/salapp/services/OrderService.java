package com.salapp.services;

import com.salapp.ProductConsumer;
import jakarta.enterprise.context.ApplicationScoped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    public void save(ProductConsumer.Order order) {

        log.info("Saving order {}", order.orderId());
    }
}
