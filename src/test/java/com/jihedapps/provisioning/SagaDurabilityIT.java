package com.jihedapps.provisioning;

import org.camunda.bpm.engine.ManagementService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.TaskService;
import org.camunda.bpm.engine.impl.util.ClockUtil;
import org.camunda.bpm.engine.runtime.Job;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.task.Task;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class SagaDurabilityIT {

    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1")).withKraft();

    @BeforeAll
    static void startContainers() {
        postgres.start();
        kafka.start();
    }

    @AfterAll
    static void stopContainers() {
        postgres.stop();
        kafka.stop();
    }

    @Test
    void testSagaSurvivesRestart() {
        String requestId = UUID.randomUUID().toString();
        String processInstanceId;

        Date start = new Date();
        ClockUtil.setCurrentTime(start);

        // --- PHASE A: Start first context ---
        ConfigurableApplicationContext ctx1 = startContext();
        try {
            RuntimeService runtimeService = ctx1.getBean(RuntimeService.class);
            ManagementService managementService = ctx1.getBean(ManagementService.class);

            ProcessInstance instance = runtimeService.startProcessInstanceByKey("number-portability-saga", requestId, Map.of(
                    "msisdn", "+21620000000",
                    "donorOperator", "Ooredoo",
                    "recipientOperator", "Orange",
                    "donorResponseTimeout", "PT30M"
            ));
            processInstanceId = instance.getId();

            boolean isWaiting = runtimeService.getActiveActivityIds(processInstanceId).contains("donorResponseReceived");
            assertThat(isWaiting).as("Saga should be waiting for donor response").isTrue();

            Job timerJob = managementService.createJobQuery().processInstanceId(processInstanceId).timers().singleResult();
            assertThat(timerJob).as("SLA timer should be scheduled in Postgres").isNotNull();

        } finally {
            ctx1.close();
        }

        // --- PHASE B: Start second context ---
        ConfigurableApplicationContext ctx2 = startContext();
        try {
            RuntimeService runtimeService = ctx2.getBean(RuntimeService.class);
            ManagementService managementService = ctx2.getBean(ManagementService.class);
            TaskService taskService = ctx2.getBean(TaskService.class);

            ProcessInstance instance = runtimeService.createProcessInstanceQuery().processInstanceId(processInstanceId).singleResult();
            assertThat(instance).as("Process instance must survive the restart").isNotNull();

            boolean isWaiting = runtimeService.getActiveActivityIds(processInstanceId).contains("donorResponseReceived");
            assertThat(isWaiting).as("Saga should still be waiting for donor response after restart").isTrue();

            Job timerJob = managementService.createJobQuery().processInstanceId(processInstanceId).timers().singleResult();
            assertThat(timerJob).as("SLA timer must survive the restart").isNotNull();

            // Advance clock to trigger SLA timeout
            ClockUtil.setCurrentTime(new Date(start.getTime() + 35 * 60 * 1000));
            managementService.executeJob(timerJob.getId());

            Task review = taskService.createTaskQuery().processInstanceBusinessKey(requestId).taskDefinitionKey("manualReview").singleResult();
            assertThat(review).as("Saga should compensate and reach manual review after timeout").isNotNull();

        } finally {
            ctx2.close();
        }

        ClockUtil.reset();
    }

    private ConfigurableApplicationContext startContext() {
        return new SpringApplicationBuilder(ProvisioningApplication.class)
                .profiles("postgres")
                .properties(
                        "camunda.bpm.job-execution.enabled=false",
                        "server.port=0", // Use random port to avoid conflicts across restarts
                        "spring.datasource.url=" + postgres.getJdbcUrl(),
                        "spring.datasource.username=" + postgres.getUsername(),
                        "spring.datasource.password=" + postgres.getPassword(),
                        "spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers()
                )
                .run();
    }
}
