package com.jihedapps.provisioning.delegate;

import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * The one gate before anything touches Kafka or a "network". Throws a plain unchecked exception
 * rather than a {@link org.camunda.bpm.engine.delegate.BpmnError} — a {@code BpmnError} with no
 * boundary catch event is silently swallowed by the engine (the process just ends early with
 * "none end event semantics", no error surfaces to the caller), which is the wrong behavior for a
 * synchronous REST call that should fail loudly on bad input.
 */
@Component("validateRequestDelegate")
public class ValidateRequestDelegate implements JavaDelegate {

    @Override
    public void execute(DelegateExecution execution) {
        String msisdn = (String) execution.getVariable("msisdn");
        String donorOperator = (String) execution.getVariable("donorOperator");
        String recipientOperator = (String) execution.getVariable("recipientOperator");

        if (isBlank(msisdn) || isBlank(donorOperator) || isBlank(recipientOperator)) {
            throw new IllegalArgumentException("msisdn, donorOperator, and recipientOperator are all required");
        }
        if (donorOperator.equalsIgnoreCase(recipientOperator)) {
            throw new IllegalArgumentException("donorOperator and recipientOperator must differ");
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
