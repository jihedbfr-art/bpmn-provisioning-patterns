package com.jihedapps.provisioning.config;

import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TracingFallbackConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public Tracer noopTracer() {
        return Tracer.NOOP;
    }

    @Bean
    @ConditionalOnMissingBean
    public Propagator noopPropagator() {
        return Propagator.NOOP;
    }
}
