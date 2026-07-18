package com.jihedapps.provisioning.delegate;

import com.jihedapps.provisioning.kafka.PortabilityEventPublisher;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component("activateOnRecipientDelegate")
public class ActivateOnRecipientDelegate implements JavaDelegate {

    private final PortabilityEventPublisher publisher;

    public ActivateOnRecipientDelegate(PortabilityEventPublisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public void execute(DelegateExecution execution) {
        String requestId = execution.getBusinessKey();
        publisher.publish("portability.activated", requestId, Map.of(
                "msisdn", execution.getVariable("msisdn"),
                "recipientOperator", execution.getVariable("recipientOperator")
        ));
    }
}
