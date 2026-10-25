package io.chatpulse.metadata;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("pipeline")
@ConditionalOnProperty(name="pipeline.role", havingValue="analyzer")
class MetadataConfiguration {
    @Bean(destroyMethod = "close")
    MetadataService metadataService(@Value("${metadata.enabled:true}") boolean enabled) { return new MetadataService(enabled); }
}
