package org.example.core.messagebroker.proposal;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.example.core.api.dto.AgreementDTO;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ProposalGenerationKafkaListener {

    private final int totalRetryCount;
    private final JsonStringToAgreementDtoConverter agreementDtoConverter;
    private final ProposalGenerator proposalGenerator;
    private final KafkaTemplate<String, String> kafkaTemplate;

    private final String topicProposalGeneration;
    private final String topicProposalGenerationDlq;

    public ProposalGenerationKafkaListener(
            @Value("${kafka.total.retry.count}") int totalRetryCount,
            @Value("${kafka.topic.proposal-generation}") String topicProposalGeneration,
            @Value("${kafka.topic.proposal-generation-dlq}") String topicProposalGenerationDlq,
            JsonStringToAgreementDtoConverter agreementDtoConverter,
            ProposalGenerator proposalGenerator,
            KafkaTemplate<String, String> kafkaTemplate) {
        this.totalRetryCount = totalRetryCount;
        this.topicProposalGeneration = topicProposalGeneration;
        this.topicProposalGenerationDlq = topicProposalGenerationDlq;
        this.agreementDtoConverter = agreementDtoConverter;
        this.proposalGenerator = proposalGenerator;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "#{__listener.topicProposalGeneration}", groupId = "proposal-group")
    public void receiveMessage(ConsumerRecord<String, String> record) {
        try {
            processMessage(record.value());
        } catch (Exception e) {
            log.error("FAIL to process message: ", e);
            retryOrForwardToDeadLetterTopic(record);
        }
    }

    private void retryOrForwardToDeadLetterTopic(ConsumerRecord<String, String> record) {
        Headers headers = record.headers();
        Header retryHeader = headers.lastHeader("x-retry-count");

        int retryCount = 0;
        if (retryHeader != null) {
            retryCount = Integer.parseInt(new String(retryHeader.value()));
        }
        retryCount++;
        log.info("Message retry count = {}", retryCount);

        if (retryCount <= totalRetryCount) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                    topicProposalGeneration,
                    null,
                    record.key(),
                    record.value()
            );

            producerRecord.headers().remove("x-retry-count");
            producerRecord.headers().add("x-retry-count", Integer.toString(retryCount).getBytes());

            kafkaTemplate.send(producerRecord);
            log.info("Message sent back to topic {} for retry", topicProposalGeneration);
        } else {
            kafkaTemplate.send(topicProposalGenerationDlq, record.key(), record.value());
            log.info("Message sent to DLQ topic {}", topicProposalGenerationDlq);
        }
    }

    private void processMessage(String messageBody) throws Exception {
        log.info("Processing message: {}", messageBody);
        AgreementDTO agreementDTO = agreementDtoConverter.convert(messageBody);
        proposalGenerator.generateProposalAndStoreToFile(agreementDTO);
    }
}
