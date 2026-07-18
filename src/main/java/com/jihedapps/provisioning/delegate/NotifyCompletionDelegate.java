package com.jihedapps.provisioning.delegate;

import com.jihedapps.provisioning.kafka.PortabilityEventPublisher;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component("notifyCompletionDelegate")
public class NotifyCompletionDelegate implements JavaDelegate {

    private final PortabilityEventPublisher publisher;

    public NotifyCompletionDelegate(PortabilityEventPublisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public void execute(DelegateExecution execution) {
        String requestId = execution.getBusinessKey();
        publisher.publish("portability.completed", requestId, Map.of(
                "msisdn", execution.getVariable("msisdn")
        ));
    }
}
