package org.example.core.messagebroker.proposal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.example.core.api.dto.AgreementDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ProposalGenerationKafkaListener {

    private static final Logger logger = LoggerFactory.getLogger(ProposalGenerationKafkaListener.class);

    private final int totalRetryCount;
    private final JsonStringToAgreementDtoConverter agreementDtoConverter;
    private final ProposalGenerator proposalGenerator;
    private final KafkaTemplate<String, String> kafkaTemplate;

    private static final String TOPIC_PROPOSAL_GENERATION = "proposal-generation";
    private static final String TOPIC_PROPOSAL_GENERATION_DLQ = "proposal-generation-dlq";

    public ProposalGenerationKafkaListener(
            @Value("${kafka.total.retry.count}") int totalRetryCount,
            JsonStringToAgreementDtoConverter agreementDtoConverter,
            ProposalGenerator proposalGenerator,
            KafkaTemplate<String, String> kafkaTemplate) {
        this.totalRetryCount = totalRetryCount;
        this.agreementDtoConverter = agreementDtoConverter;
        this.proposalGenerator = proposalGenerator;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = TOPIC_PROPOSAL_GENERATION, groupId = "proposal-group")
    public void receiveMessage(ConsumerRecord<String, String> record) {
        try {
            processMessage(record.value());
        } catch (Exception e) {
            logger.error("FAIL to process message: ", e);
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
        logger.info("Message retry count = {}", retryCount);

        if (retryCount <= totalRetryCount) {

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                    TOPIC_PROPOSAL_GENERATION,
                    null,
                    record.key(),
                    record.value()
            );

            producerRecord.headers().remove("x-retry-count");
            producerRecord.headers().add("x-retry-count", Integer.toString(retryCount).getBytes());

            kafkaTemplate.send(producerRecord);
            logger.info("Message sent back to topic {} for retry", TOPIC_PROPOSAL_GENERATION);
        } else {
            kafkaTemplate.send(TOPIC_PROPOSAL_GENERATION_DLQ, record.key(), record.value());
            logger.info("Message sent to DLQ topic {}", TOPIC_PROPOSAL_GENERATION_DLQ);
        }
    }

    private void processMessage(String messageBody) throws Exception {
        logger.info("Processing message: {}", messageBody);
        AgreementDTO agreementDTO = agreementDtoConverter.convert(messageBody);
        proposalGenerator.generateProposalAndStoreToFile(agreementDTO);
    }

}
