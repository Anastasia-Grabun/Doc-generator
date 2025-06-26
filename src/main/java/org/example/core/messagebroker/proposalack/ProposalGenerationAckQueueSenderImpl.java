package org.example.core.messagebroker.proposalack;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.example.core.api.dto.AgreementDTO;
import org.example.core.api.dto.ProposalGenerationAcknowledgment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class ProposalGenerationAckQueueSenderImpl implements ProposalGenerationAckQueueSender {

    private static final Logger logger = LoggerFactory.getLogger(ProposalGenerationAckQueueSenderImpl.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    private static final String TOPIC_PROPOSAL_GENERATION_ACK = "proposal-generation-ack";

    @Override
    public void send(AgreementDTO agreement, String proposalFilePath) {
        ProposalGenerationAcknowledgment ackMessage = new ProposalGenerationAcknowledgment();
        ackMessage.setAgreementUuid(agreement.getUuid());
        ackMessage.setProposalFilePath(proposalFilePath);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String json = objectMapper.writeValueAsString(ackMessage);
            logger.info("PROPOSAL GENERATION ACK message content: " + json);
            kafkaTemplate.send(TOPIC_PROPOSAL_GENERATION_ACK, json);
        } catch (JsonProcessingException e) {
            logger.error("Error converting proposal generation ack to JSON", e);
        } catch (Exception e) {
            logger.error("Error sending proposal generation ack message to Kafka", e);
        }
    }

}
