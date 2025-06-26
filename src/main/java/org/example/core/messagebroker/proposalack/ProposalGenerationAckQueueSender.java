package org.example.core.messagebroker.proposalack;

import org.example.core.api.dto.AgreementDTO;

public interface ProposalGenerationAckQueueSender {

    void send(AgreementDTO agreement, String proposalFilePath);

}
