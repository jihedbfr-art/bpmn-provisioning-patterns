package com.jihedapps.provisioning.outbox;

import com.jihedapps.provisioning.idempotency.ProcessedEventRepository;
import com.jihedapps.provisioning.outbox.OutboxRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

@SpringBootTest(properties = {"provisioning.outbox.relay.enabled=false"})
class SqlDialectPortabilityTest {

    @Autowired
    private OutboxRepository outboxRepository;
    
    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Test
    void skipLockedQueryIsValidOnH2() {
        assertThatNoException().isThrownBy(() -> outboxRepository.lockUnpublishedBatch(1));
    }

    @Test
    void processedEventInsertIsIdempotentOnDefaultProfile() {
        String id = UUID.randomUUID().toString();
        assertThat(processedEventRepository.markProcessed(id, "topic", "aggregate")).isTrue();
        assertThat(processedEventRepository.markProcessed(id, "topic", "aggregate")).isFalse();
    }
}
