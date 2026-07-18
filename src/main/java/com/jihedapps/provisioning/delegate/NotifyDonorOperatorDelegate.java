package com.jihedapps.provisioning.delegate;

import com.jihedapps.provisioning.kafka.PortabilityEventPublisher;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component("notifyDonorOperatorDelegate")
public class NotifyDonorOperatorDelegate implements JavaDelegate {

    private final PortabilityEventPublisher publisher;

    public NotifyDonorOperatorDelegate(PortabilityEventPublisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public void execute(DelegateExecution execution) {
        String requestId = execution.getBusinessKey();
        publisher.publish("donor.notification.requested", requestId, Map.of(
                "msisdn", execution.getVariable("msisdn"),
                "donorOperator", execution.getVariable("donorOperator"),
                "recipientOperator", execution.getVariable("recipientOperator")
        ));
    }
}
