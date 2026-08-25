package com.jihedapps.provisioning.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihedapps.provisioning.idempotency.ProcessedEventRepository;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.MismatchingMessageCorrelationException;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.history.HistoricProcessInstance;
import org.camunda.bpm.engine.runtime.MessageCorrelationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
public class DonorResponseListener {

    private static final Logger LOG = LoggerFactory.getLogger(DonorResponseListener.class);

    private final RuntimeService runtimeService;
    private final HistoryService historyService;
    private final ProcessedEventRepository processedEventRepository;
    private final ObjectMapper objectMapper;

    public DonorResponseListener(RuntimeService runtimeService,
                                 HistoryService historyService,
                                 ProcessedEventRepository processedEventRepository,
                                 ObjectMapper objectMapper) {
        this.runtimeService = runtimeService;
        this.historyService = historyService;
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

        // correlateAllWithResult returns an empty list when nothing is waiting, where
        // correlateWithResult throws. That distinction matters here: Camunda marks the
        // surrounding transaction rollback-only as it raises
        // MismatchingMessageCorrelationException, and catching the exception does not
        // clear that flag. The benign-race branch below would then return normally and
        // still blow up at commit with UnexpectedRollbackException. Asking instead of
        // catching keeps the transaction clean. The business key is unique per request,
        // so this correlates at most one instance either way.
        List<MessageCorrelationResult> correlated = runtimeService.createMessageCorrelation("DonorResponseMessage")
                .processInstanceBusinessKey(event.requestId())
                .setVariable("donorDecision", event.decision())
                .correlateAllWithResult();

        if (!correlated.isEmpty()) {
            LOG.info("Successfully correlated donor response for request {}", event.requestId());
            return;
        }

        HistoricProcessInstance historicProcessInstance = historyService.createHistoricProcessInstanceQuery()
                .processInstanceBusinessKey(event.requestId())
                .singleResult();

        boolean tookTimeoutPath = false;
        if (historicProcessInstance != null) {
            tookTimeoutPath = historyService.createHistoricActivityInstanceQuery()
                    .processInstanceId(historicProcessInstance.getId())
                    .activityId("slaTimeout")
                    .finished()
                    .count() > 0;
        }

        if (tookTimeoutPath) {
            LOG.warn("Correlation found nothing waiting for request {}. The SLA timer expired and the process moved on. Ignoring message.", event.requestId());
            return; // Business race condition, not a technical failure. Ignore.
        }

        LOG.warn("No waiting process instance found for request {}. Event will be rolled back and sent to DLT.", event.requestId());
        // Thrown by us rather than by the engine, so the rollback is Spring's own and
        // the error handler can route the record to the DLT.
        throw new MismatchingMessageCorrelationException("DonorResponseMessage",
                "no process instance waiting for business key " + event.requestId());
    }
}
