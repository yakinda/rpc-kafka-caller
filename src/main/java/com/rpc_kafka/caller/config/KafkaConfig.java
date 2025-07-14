package com.rpc_kafka.caller.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

import java.util.UUID;

@Configuration
public class KafkaConfig {
    private final String uniqueId = UUID.randomUUID().toString().substring(0, 8);
    private final String replyTopic = "replies-" + uniqueId;
    private final String replyGroupId = "replies-group-" + uniqueId;

    @Bean
    public ReplyingKafkaTemplate<String, String, String> replyingTemplate(
            ProducerFactory<String, String> pf,
            GenericMessageListenerContainer<String, String> container) {
        return new ReplyingKafkaTemplate<>(pf, container);
    }

    @Bean
    public ConcurrentMessageListenerContainer<String, String> repliesContainer(
            ConcurrentKafkaListenerContainerFactory<String, String> factory) {
        ConcurrentMessageListenerContainer<String, String> container = factory.createContainer(replyTopic);
        container.getContainerProperties().setGroupId(replyGroupId);
        return container;
    }

    @Bean
    public String replyTopic() {
        return replyTopic;
    }
}