package org.example.core.messagebroker;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;

@EnableKafka
@Configuration
public class KafkaConfig {

    public static final String TOPIC_PROPOSAL_GENERATION = "proposal-generation";

    @Bean
    public NewTopic proposalGenerationTopic() {
        return TopicBuilder.name("proposal-generation")
                .partitions(1)
                .replicas(1)
                .build();
    }

}