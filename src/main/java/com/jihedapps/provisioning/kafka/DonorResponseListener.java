package com.jihedapps.provisioning.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihedapps.provisioning.idempotency.ProcessedEventRepository;
import org.camunda.bpm.engine.MismatchingMessageCorrelationException;
import org.camunda.bpm.engine.RuntimeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class DonorResponseListener {

    private static final Logger LOG = LoggerFactory.getLogger(DonorResponseListener.class);

    private final RuntimeService runtimeService;
    private final ProcessedEventRepository processedEventRepository;
    private final ObjectMapper objectMapper;

    public DonorResponseListener(RuntimeService runtimeService,
                                 ProcessedEventRepository processedEventRepository,
                                 ObjectMapper objectMapper) {
        this.runtimeService = runtimeService;
        this.processedEventRepository = processedEventRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(id = "donor-response-listener", topics = "${provisioning.kafka.donor-response-topic}", groupId = "bpmn-provisioning-patterns")
    @Transactional
    public void onDonorResponse(String payload) throws Exception {
        DonorResponseEvent event = objectMapper.readValue(payload, DonorResponseEvent.class);
        
        LOG.debug("Received donor response event: {}", event.eventId());

        boolean isNew = processedEventRepository.markProcessed(event.eventId(), "donor-response-events", event.requestId());
        if (!isNew) {
            LOG.debug("Event {} already processed, skipping to guarantee idempotency", event.eventId());
            return;
        }

        try {
            runtimeService.createMessageCorrelation("DonorResponseMessage")
                    .processInstanceBusinessKey(event.requestId())
                    .setVariable("donorDecision", event.decision())
                    .correlateWithResult();
            
            LOG.info("Successfully correlated donor response for request {}", event.requestId());
        } catch (MismatchingMessageCorrelationException e) {
            LOG.warn("No waiting process instance found for request {}. Event will be rolled back and sent to DLT.", event.requestId());
            throw e; // Let it bubble up to trigger rollback and DLT recovery
        }
    }
}
