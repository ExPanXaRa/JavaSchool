package sbp.school.kafka;

import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.listener.KafkaConsumerService;

public class Main {
    public static void main(String[] args) {
        KafkaConfig kafkaConfig = new KafkaConfig();
        try (KafkaConsumerService consumer = new KafkaConsumerService(kafkaConfig.getProperties())) {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
            consumer.read();
        }
    }
}
