package com.jihedapps.provisioning.simprovisioning;

import com.jihedapps.provisioning.kafka.PortabilityEventPublisher;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@SpringBootTest
class BulkSimProvisioningTest {

    private static final String PROCESS_KEY = "bulk-sim-provisioning";

    @Autowired
    private RuntimeService runtimeService;
    @Autowired
    private HistoryService historyService;

    @MockBean
    private SimProvisioningGateway gateway;
    @MockBean
    private PortabilityEventPublisher publisher;

    private List<Map<String, String>> simRequests(String... iccids) {
        List<Map<String, String>> list = new java.util.ArrayList<>();
        for (String iccid : iccids) {
            Map<String, String> m = new HashMap<>();
            m.put("iccid", iccid);
            m.put("msisdn", "+2162000" + iccid);
            list.add(m);
        }
        return list;
    }

    private String endActivityId(String batchId) {
        var historicInstance = historyService.createHistoricProcessInstanceQuery()
                .processInstanceBusinessKey(batchId)
                .singleResult();
        return historicInstance.getEndActivityId();
    }

    @Test
    void allSuccessfulProvisioningCompletesTheBatchWithoutCompensation() {
        when(gateway.provision(any(), any())).thenReturn(true);
        String batchId = UUID.randomUUID().toString();

        runtimeService.startProcessInstanceByKey(PROCESS_KEY, batchId, Map.of(
                "simRequests", simRequests("sim-1", "sim-2", "sim-3"),
                "rollbackThreshold", 0.2
        ));

        assertThat(endActivityId(batchId)).isEqualTo("completed");
        verify(gateway, never()).deprovision(any());
    }

    @Test
    void failureRateAboveThresholdRollsBackOnlyTheSuccessfulOnes() {
        when(gateway.provision(eq("sim-1"), any())).thenReturn(true);
        when(gateway.provision(eq("sim-2"), any())).thenReturn(false);
        when(gateway.provision(eq("sim-3"), any())).thenReturn(false);
        String batchId = UUID.randomUUID().toString();

        // 2 of 3 fail = 0.667 failure rate, well above a 0.2 threshold
        runtimeService.startProcessInstanceByKey(PROCESS_KEY, batchId, Map.of(
                "simRequests", simRequests("sim-1", "sim-2", "sim-3"),
                "rollbackThreshold", 0.2
        ));

        assertThat(endActivityId(batchId)).isEqualTo("rolledBack");
        verify(gateway, times(1)).deprovision("sim-1");
        verify(gateway, never()).deprovision("sim-2");
        verify(gateway, never()).deprovision("sim-3");
    }

    @Test
    void failureRateBelowThresholdAcceptsPartialSuccessWithoutRollback() {
        when(gateway.provision(eq("sim-1"), any())).thenReturn(true);
        when(gateway.provision(eq("sim-2"), any())).thenReturn(true);
        when(gateway.provision(eq("sim-3"), any())).thenReturn(true);
        when(gateway.provision(eq("sim-4"), any())).thenReturn(false);
        String batchId = UUID.randomUUID().toString();

        // 1 of 4 fails = 0.25... let's use a threshold that keeps this under, e.g. 0.3
        runtimeService.startProcessInstanceByKey(PROCESS_KEY, batchId, Map.of(
                "simRequests", simRequests("sim-1", "sim-2", "sim-3", "sim-4"),
                "rollbackThreshold", 0.3
        ));

        assertThat(endActivityId(batchId)).isEqualTo("completed");
        verify(gateway, never()).deprovision(any());
        verify(publisher).publish(eq("sim.provisioning_failed"), eq(batchId), any());
    }
}
