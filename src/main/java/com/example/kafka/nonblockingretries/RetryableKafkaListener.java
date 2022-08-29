package com.example.kafka.nonblockingretries;

import lombok.extern.slf4j.Slf4j;

import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetryableKafkaListener {

    private long previous = 0;

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            autoCreateTopics = "false",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
    @KafkaListener(topics = "orders")
    public void listen(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        var current = System.currentTimeMillis();
        previous = previous == 0 ? current : previous;
        var interval = current - previous;
        log.info(in + " from " + topic + " topic interval = " + interval + " milliseconds since previous message");
        throw new RuntimeException("test");
    }

    @DltHandler
    public void dlt(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info(in + " from DLT" + topic);
    }
}
