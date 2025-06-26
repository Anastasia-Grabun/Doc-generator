package org.example.core.messagebroker.proposalack;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.example.core.api.dto.AgreementDTO;
import org.example.core.api.dto.ProposalGenerationAcknowledgment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
class ProposalGenerationAckQueueSenderImpl implements ProposalGenerationAckQueueSender {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topicProposalGenerationAck;

    public ProposalGenerationAckQueueSenderImpl(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${kafka.topic.proposal-generation-ack}") String topicProposalGenerationAck) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicProposalGenerationAck = topicProposalGenerationAck;
    }

    @Override
    public void send(AgreementDTO agreement, String proposalFilePath) {
        ProposalGenerationAcknowledgment ackMessage = new ProposalGenerationAcknowledgment();
        ackMessage.setAgreementUuid(agreement.getUuid());
        ackMessage.setProposalFilePath(proposalFilePath);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String json = objectMapper.writeValueAsString(ackMessage);
            log.info("PROPOSAL GENERATION ACK message content: " + json);
            kafkaTemplate.send(topicProposalGenerationAck, json);
        } catch (JsonProcessingException e) {
            log.error("Error converting proposal generation ack to JSON", e);
        } catch (Exception e) {
            log.error("Error sending proposal generation ack message to Kafka", e);
        }
    }

}
