package com.rpc_kafka.caller.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

import java.time.Duration;
import java.util.UUID;

@Configuration
public class KafkaConfig {
    private final String replyTopic = "replies";

    @Bean
    public ReplyingKafkaTemplate<String, String, String> replyingTemplate(
            ProducerFactory<String, String> pf,
            ConcurrentMessageListenerContainer<String, String> repliesContainer) {
        ReplyingKafkaTemplate<String, String, String> template = new ReplyingKafkaTemplate<>(pf, repliesContainer);
        template.setSharedReplyTopic(true);
        template.setDefaultReplyTimeout(Duration.ofSeconds(10));
        return template;
    }

    @Bean
    public ConcurrentMessageListenerContainer<String, String> repliesContainer(
            ConcurrentKafkaListenerContainerFactory<String, String> factory) {
        String groupId = "replies-group" + UUID.randomUUID().toString().substring(0, 8);
        ConcurrentMessageListenerContainer<String, String> container = factory.createContainer(replyTopic);
        container.getContainerProperties().setGroupId(groupId);
        return container;
    }
}