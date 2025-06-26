package org.example.core.messagebroker.proposal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.core.api.dto.AgreementDTO;
import org.springframework.stereotype.Component;

@Component
class JsonStringToAgreementDtoConverter {

    public AgreementDTO convert(String json) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(json, AgreementDTO.class);
    }

}
