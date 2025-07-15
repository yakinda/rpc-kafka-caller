package com.rpc_kafka.caller.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class CallerService {
    @Autowired
    private ReplyingKafkaTemplate<String, String, String> template;

    public String callRpc(String procedure, String args) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>("requests", procedure + ":" + args);
            RequestReplyFuture<String, String, String> future = template.sendAndReceive(record);
            ConsumerRecord<String, String> response = future.get();
            return response.value();
        } catch (Exception e) {
            return "error: " + e.getMessage();
        }
    }

    @Scheduled(fixedRate = 100)
    public void testCallRPC() {
        List<Integer> payloads = List.of(randomInt(), randomInt());
        String expectedResult = String.valueOf(payloads.stream().mapToInt(it -> it).sum());
        String payloadsString = payloads.stream().map(Object::toString).collect(Collectors.joining(","));
        System.out.println("payloadsString: " + payloadsString);
        long start = System.currentTimeMillis();
        String result = callRpc("add", payloadsString);
        long end = System.currentTimeMillis();
        System.out.printf("[result:%s]-[%s]-[%s]%n", result, end - start, result.equals(expectedResult));
    }

    private Integer randomInt() {
        int min = 0;
        int max = 1000;
        return (int) (Math.random() * (max - min + 1)) + min;
    }
}