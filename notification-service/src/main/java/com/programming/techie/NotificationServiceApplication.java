package com.programming.techie;

import exception.NonRetryableException;
import exception.RetryableException;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;

@SpringBootApplication
@Slf4j
@RequiredArgsConstructor
public class NotificationServiceApplication {

    private final ObservationRegistry observationRegistry;
    private final Tracer tracer;

    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }

//    @KafkaListener(topics = "notificationTopic_Nov_V9")
//    public void handleNotification(OrderPlacedEvent orderPlacedEvent) {
//        log.info("Got message <{}>", orderPlacedEvent);
//        log.info("TraceId- {}, Received Notification for Order - {}", this.tracer.currentSpan().context().traceId(),
//                orderPlacedEvent.getOrderNumber());
//    }

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            autoCreateTopics = "false",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            exclude = { NullPointerException.class, NonRetryableException.class } // Exclude specific exceptions
    )
    @KafkaListener(topics = "notificationTopic_Nov_V9")
    public void listen(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info(in + " from " + topic);

        if (in.contains("retry")) {
            throw new RetryableException("This error should be retried.");
        } else if (in.contains("null")) {
            throw new NullPointerException("This will not be retried.");
        } else {
            throw new NonRetryableException("This error should go directly to the DLT.");
        }
    }

    @DltHandler
    public void dlt(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info(in + " from " + topic);
    }


}
