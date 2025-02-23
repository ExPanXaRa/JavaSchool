package sbp.school.kafka.constant;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import sbp.school.kafka.dto.ChecksumDto;

public class ChecksumConstant {


    public static ConsumerRecord<String, ChecksumDto> createConsumerRecord(String topicName,
        ChecksumDto checksumDto) {
        return new ConsumerRecord<>(topicName, 0, 0, null, checksumDto);
    }


}
