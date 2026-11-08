/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shenyu.plugin.httpclient;

import io.netty.channel.ConnectTimeoutException;
import io.netty.handler.timeout.ReadTimeoutException;
import org.apache.shenyu.common.constant.Constants;
import org.apache.shenyu.common.exception.ShenyuException;
import org.apache.shenyu.plugin.api.ShenyuPluginChain;
import org.apache.shenyu.plugin.api.context.ShenyuContext;
import org.apache.shenyu.plugin.api.result.ShenyuResultEnum;
import org.apache.shenyu.plugin.base.circuitbreaker.UpstreamCircuitBreaker;
import org.apache.shenyu.plugin.httpclient.exception.ShenyuTimeoutException;
import org.apache.shenyu.plugin.httpclient.exception.ShenyuUpstreamStatusException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferLimitException;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.ConnectException;
import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZeroRetryHttpClientTest {

    private static final String EXHAUSTED = "Request timeout, the maximum number of retry times has been exceeded";

    static Stream<Arguments> configurations() {
        return Stream.of(Arguments.of("default", "current"), Arguments.of("default", "failover"),
                Arguments.of("fixed", "current"), Arguments.of("fixed", "failover"),
                Arguments.of("exponential", "current"), Arguments.of("exponential", "failover"),
                Arguments.of("unknown", "current"), Arguments.of(null, null));
    }

    @ParameterizedTest
    @MethodSource("configurations")
    void preservesSourceErrorsWithoutResubscribing(final String spec, final String strategy) {
        Throwable[] failures = {new TimeoutException("upstream timeout"), new ConnectTimeoutException("connect timeout"),
            ReadTimeoutException.INSTANCE, new IllegalStateException("closed"), new ConnectException("refused"),
            new RuntimeException(new TimeoutException("wrapped")), new ShenyuTimeoutException("existing timeout"),
            new DataBufferLimitException("too large"), new ShenyuException("no upstream"), new ShenyuUpstreamStatusException(503)};
        for (Throwable failure : failures) {
            RecordingPlugin plugin = new RecordingPlugin(body -> Mono.error(failure));
            ServerWebExchange exchange = exchange(spec, strategy);
            AtomicInteger chainCalls = new AtomicInteger();
            StepVerifier.create(plugin.execute(exchange, chain(chainCalls)))
                    .expectErrorSatisfies(error -> assertSourceError(spec, strategy, failure, error))
                    .verify(Duration.ofSeconds(5));
            assertEquals(1, plugin.attempts.get());
            assertEquals(0, chainCalls.get());
        }
    }

    @ParameterizedTest
    @MethodSource("configurations")
    void deadlineCancelsTheRequestAndMapsToGatewayTimeout(final String spec, final String strategy) {
        AtomicBoolean cancelled = new AtomicBoolean();
        RecordingPlugin plugin = new RecordingPlugin(body -> Mono.<String>never().doOnCancel(() -> cancelled.set(true)));
        AtomicInteger chainCalls = new AtomicInteger();
        StepVerifier.withVirtualTime(() -> plugin.execute(exchange(spec, strategy), chain(chainCalls)))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(99))
                .thenAwait(Duration.ofMillis(1))
                .expectErrorSatisfies(error -> {
                    ResponseStatusException status = assertInstanceOf(ResponseStatusException.class, error);
                    assertEquals(HttpStatus.GATEWAY_TIMEOUT, status.getStatusCode());
                    assertInstanceOf(TimeoutException.class, status.getCause());
                    assertEquals("default".equals(spec) && "failover".equals(strategy)
                            ? "Response took longer than timeout: PT0.1S" : "Retry sequence took longer than timeout: PT0.1S", status.getReason());
                }).verify(Duration.ofSeconds(5));
        assertTrue(cancelled.get());
        assertEquals(1, plugin.attempts.get());
        assertEquals(0, chainCalls.get());
    }

    @ParameterizedTest
    @MethodSource("configurations")
    void streamsBodyWithoutReplayCacheAndDoesNotTimeOutTheChain(final String spec, final String strategy) {
        AtomicInteger bytes = new AtomicInteger();
        RecordingPlugin plugin = new RecordingPlugin(body -> body.doOnNext(buffer -> {
            bytes.addAndGet(buffer.readableByteCount());
            DataBufferUtils.release(buffer);
        }).then(Mono.just("success")));
        ServerWebExchange exchange = exchange(spec, strategy);
        AtomicInteger chainCalls = new AtomicInteger();
        StepVerifier.withVirtualTime(() -> plugin.execute(exchange, current -> {
            chainCalls.incrementAndGet();
            return Mono.delay(Duration.ofMillis(200)).then();
        })).expectSubscription().thenAwait(Duration.ofMillis(200)).verifyComplete();
        assertEquals(1, plugin.attempts.get());
        assertEquals(1, chainCalls.get());
        assertEquals(12, bytes.get());
        assertNull(exchange.getAttribute(Constants.CACHED_REQUEST_BODY));
    }

    @ParameterizedTest
    @MethodSource("configurations")
    void cancellationReachesTheSource(final String spec, final String strategy) {
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicInteger chainCalls = new AtomicInteger();
        RecordingPlugin plugin = new RecordingPlugin(body -> Mono.<String>never().doOnCancel(() -> cancelled.set(true)));
        StepVerifier.withVirtualTime(() -> plugin.execute(exchange(spec, strategy), chain(chainCalls)))
                .expectSubscription().thenAwait(Duration.ofMillis(50)).thenCancel().verify(Duration.ofSeconds(5));
        assertTrue(cancelled.get());
        assertEquals(1, plugin.attempts.get());
        assertEquals(0, chainCalls.get());
    }

    @ParameterizedTest
    @MethodSource("configurations")
    void halfOpenProbeSuppressesConfiguredRetries(final String spec, final String strategy) {
        RecordingPlugin plugin = new RecordingPlugin(body -> Mono.error(new TimeoutException("probe failure")));
        ServerWebExchange exchange = exchange(spec, strategy);
        exchange.getAttributes().put(Constants.HTTP_RETRY, 3);
        exchange.getAttributes().put(UpstreamCircuitBreaker.HALF_OPEN_PROBE, true);
        AtomicInteger chainCalls = new AtomicInteger();
        StepVerifier.create(plugin.execute(exchange, chain(chainCalls))).expectError().verify(Duration.ofSeconds(5));
        assertEquals(1, plugin.attempts.get());
        assertEquals(0, chainCalls.get());
        assertNull(exchange.getAttribute(Constants.CACHED_REQUEST_BODY));
    }

    private void assertSourceError(final String spec, final String strategy, final Throwable failure, final Throwable error) {
        boolean defaultSpec = !"fixed".equals(spec) && !"exponential".equals(spec);
        boolean current = Objects.isNull(strategy) || "current".equals(strategy);
        if (defaultSpec && current && (failure instanceof TimeoutException || failure instanceof ConnectTimeoutException
                || failure instanceof ReadTimeoutException || failure instanceof IllegalStateException && !(failure instanceof DataBufferLimitException)
                || failure instanceof ShenyuTimeoutException)) {
            ResponseStatusException status = assertInstanceOf(ResponseStatusException.class, error);
            assertEquals(HttpStatus.REQUEST_TIMEOUT, status.getStatusCode());
            assertInstanceOf(ShenyuTimeoutException.class, status.getCause());
            assertEquals(failure instanceof ShenyuTimeoutException ? failure.getMessage() : EXHAUSTED, status.getReason());
        } else if (failure instanceof TimeoutException || failure instanceof ShenyuException || failure instanceof DataBufferLimitException) {
            ResponseStatusException status = assertInstanceOf(ResponseStatusException.class, error);
            assertSame(failure, status.getCause());
            if (failure instanceof TimeoutException) {
                assertEquals(HttpStatus.GATEWAY_TIMEOUT, status.getStatusCode());
                assertEquals(failure.getMessage(), status.getReason());
            } else if (failure instanceof ShenyuException) {
                assertEquals(HttpStatus.SERVICE_UNAVAILABLE, status.getStatusCode());
                assertEquals("failover".equals(strategy) && !"exponential".equals(spec)
                        ? "CANNOT_FIND_HEALTHY_UPSTREAM_URL_AFTER_FAILOVER" : ShenyuResultEnum.CANNOT_FIND_HEALTHY_UPSTREAM_URL_AFTER_FAILOVER.getMsg(), status.getReason());
            } else {
                assertEquals(HttpStatus.PAYLOAD_TOO_LARGE, status.getStatusCode());
            }
        } else {
            assertSame(failure, error);
        }
    }

    private ServerWebExchange exchange(final String spec, final String strategy) {
        ServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.post("/test").body("request-body"));
        exchange.getAttributes().put(Constants.CONTEXT, new ShenyuContext());
        exchange.getAttributes().put(Constants.HTTP_URI, URI.create("http://localhost/test"));
        exchange.getAttributes().put(Constants.HTTP_TIME_OUT, 100L);
        if (Objects.nonNull(spec)) {
            exchange.getAttributes().put(Constants.HTTP_RETRY_BACK_OFF_SPEC, spec);
            exchange.getAttributes().put(Constants.HTTP_RETRY, 0);
        }
        if (Objects.nonNull(strategy)) {
            exchange.getAttributes().put(Constants.RETRY_STRATEGY, strategy);
        }
        return exchange;
    }

    private ShenyuPluginChain chain(final AtomicInteger calls) {
        return exchange -> {
            calls.incrementAndGet();
            return Mono.empty();
        };
    }

    private static final class RecordingPlugin extends AbstractHttpClientPlugin<String> {

        private final AtomicInteger attempts = new AtomicInteger();

        private final Function<Flux<DataBuffer>, Mono<String>> response;

        private RecordingPlugin(final Function<Flux<DataBuffer>, Mono<String>> response) {
            super(4);
            this.response = response;
        }

        @Override
        protected Mono<String> doRequest(final ServerWebExchange exchange, final String method, final URI uri, final Flux<DataBuffer> body) {
            return Mono.defer(() -> {
                attempts.incrementAndGet();
                return response.apply(body);
            });
        }

        @Override
        public int getOrder() {
            return 0;
        }
    }
}
