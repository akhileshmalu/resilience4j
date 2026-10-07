/*
 * Copyright 2026 Resilience4j contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.resilience4j.reactor.ratelimiter.operator;

import io.github.resilience4j.ratelimiter.RateLimiter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * Simulates what Reactor's automatic context propagation (3.5.3+) or an agent such as the
 * OpenTelemetry Reactor instrumentation does: a lift hook that wraps every subscriber and
 * restores ThreadLocals from that subscriber's Context, clearing missing keys, while signals
 * are delivered. The caller sets the ThreadLocal, and the source must still see it when the
 * rate limiter delays the subscription to another thread.
 */
public class RateLimiterOperatorContextTest {

    private static final String HOOK_KEY = "threadLocalRestoring";
    private static final ThreadLocal<String> TENANT = new ThreadLocal<>();

    @AfterEach
    public void tearDown() {
        Hooks.resetOnEachOperator(HOOK_KEY);
        TENANT.remove();
    }

    @Test
    public void shouldRestoreThreadLocalForMonoWhenNotDelayed() {
        assertThat(tenantSeenByMonoSource(rateLimiter(false))).isEqualTo("A");
    }

    @Test
    public void shouldRestoreThreadLocalForMonoWhenDelayed() {
        assertThat(tenantSeenByMonoSource(rateLimiter(true))).isEqualTo("A");
    }

    @Test
    public void shouldRestoreThreadLocalForFluxWhenDelayed() {
        Hooks.onEachOperator(HOOK_KEY, Operators.lift((scannable, subscriber) ->
            new ThreadLocalRestoringSubscriber<>(subscriber)));
        AtomicReference<String> seen = new AtomicReference<>("not-run");
        Flux<String> source = Flux.defer(() -> {
            seen.set(TENANT.get());
            return Flux.just("a", "b");
        });

        TENANT.set("A");
        source.transformDeferred(RateLimiterOperator.of(rateLimiter(true)))
            .contextWrite(Context.of("tenant", "A"))
            .blockLast(Duration.ofSeconds(5));

        assertThat(seen.get()).isEqualTo("A");
    }

    private String tenantSeenByMonoSource(RateLimiter rateLimiter) {
        Hooks.onEachOperator(HOOK_KEY, Operators.lift((scannable, subscriber) ->
            new ThreadLocalRestoringSubscriber<>(subscriber)));
        AtomicReference<String> seen = new AtomicReference<>("not-run");
        Mono<String> source = Mono.defer(() -> {
            seen.set(TENANT.get());
            return Mono.just("ok");
        });

        TENANT.set("A");
        source.transformDeferred(RateLimiterOperator.of(rateLimiter))
            .contextWrite(Context.of("tenant", "A"))
            .block(Duration.ofSeconds(5));
        return seen.get();
    }

    private static RateLimiter rateLimiter(boolean delayed) {
        RateLimiter rateLimiter = mock(RateLimiter.class);
        given(rateLimiter.reservePermission(1))
            .willReturn(delayed ? Duration.ofMillis(50).toNanos() : 0L);
        return rateLimiter;
    }

    private static final class ThreadLocalRestoringSubscriber<T> implements CoreSubscriber<T> {

        private final CoreSubscriber<? super T> actual;

        ThreadLocalRestoringSubscriber(CoreSubscriber<? super T> actual) {
            this.actual = actual;
        }

        @Override
        public Context currentContext() {
            return actual.currentContext();
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            restoring(() -> actual.onSubscribe(subscription));
        }

        @Override
        public void onNext(T value) {
            restoring(() -> actual.onNext(value));
        }

        @Override
        public void onError(Throwable throwable) {
            restoring(() -> actual.onError(throwable));
        }

        @Override
        public void onComplete() {
            restoring(actual::onComplete);
        }

        private void restoring(Runnable signal) {
            String previous = TENANT.get();
            String fromContext = actual.currentContext().getOrDefault("tenant", null);
            if (fromContext == null) {
                TENANT.remove();
            } else {
                TENANT.set(fromContext);
            }
            try {
                signal.run();
            } finally {
                if (previous == null) {
                    TENANT.remove();
                } else {
                    TENANT.set(previous);
                }
            }
        }
    }
}
