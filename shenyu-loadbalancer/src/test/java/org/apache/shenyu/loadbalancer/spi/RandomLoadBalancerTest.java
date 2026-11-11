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

package org.apache.shenyu.loadbalancer.spi;

import org.apache.shenyu.loadbalancer.entity.LoadBalanceData;
import org.apache.shenyu.loadbalancer.entity.Upstream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The type random balance test.
 */
public class RandomLoadBalancerTest {

    @Test
    public void randomLoadBalancesWeightEqualTest() {
        final List<Upstream> targets = Stream.of(10, 10, 10)
                .map(weight -> Upstream.builder()
                        .url("upstream-" + weight)
                        .weight(weight)
                        .build())
                .collect(Collectors.toList());
        final RandomLoadBalancer balancer = new RandomLoadBalancer();
        final LoadBalanceData data = new LoadBalanceData();
        for (int index = 0; index < 1000; index++) {
            assertTrue(targets.contains(balancer.doSelect(targets, data)));
        }
    }

    @Test
    public void randomLoadBalancesWeightZeroTest() {
        final List<Upstream> targets = Stream.of(0, 0, 0)
                .map(weight -> Upstream.builder()
                        .url("upstream-" + weight)
                        .weight(weight)
                        .build())
                .collect(Collectors.toList());
        final RandomLoadBalancer balancer = new RandomLoadBalancer();
        final LoadBalanceData data = new LoadBalanceData();
        for (int index = 0; index < 1000; index++) {
            assertTrue(targets.contains(balancer.doSelect(targets, data)));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 3, 5})
    public void selectsOnlyPositiveWeightAtDifferentPositions(final int heavyIndex) {
        final List<Upstream> targets = IntStream.range(0, 6)
                .mapToObj(index -> Upstream.builder().url("upstream-" + index).weight(index == heavyIndex ? 100 : 0).build())
                .collect(Collectors.toList());
        final RandomLoadBalancer balancer = new RandomLoadBalancer();
        final LoadBalanceData data = new LoadBalanceData();
        for (int index = 0; index < 1000; index++) {
            assertSame(targets.get(heavyIndex), balancer.doSelect(targets, data));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 100})
    public void selectsSingleUpstream(final int weight) {
        final Upstream target = Upstream.builder().url("single").weight(weight).build();
        final RandomLoadBalancer balancer = new RandomLoadBalancer();
        final LoadBalanceData data = new LoadBalanceData();
        for (int index = 0; index < 100; index++) {
            assertSame(target, balancer.doSelect(List.of(target), data));
        }
    }

    @Test
    public void excludesLeadingZeroWeightWhenRemainingWeightsAreEqualAndPositive() {
        final Upstream zero = Upstream.builder().url("zero").weight(0).build();
        final Upstream first = Upstream.builder().url("first").weight(10).build();
        final Upstream second = Upstream.builder().url("second").weight(10).build();
        final List<Upstream> targets = List.of(zero, first, second);
        final List<Upstream> positiveTargets = List.of(first, second);
        final RandomLoadBalancer balancer = new RandomLoadBalancer();
        final LoadBalanceData data = new LoadBalanceData();
        for (int index = 0; index < 1000; index++) {
            assertTrue(positiveTargets.contains(balancer.doSelect(targets, data)));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 1, 3, 5})
    public void readsEachWeightOnceInListOrder(final int heavyIndex) {
        final List<Upstream> targets = IntStream.range(0, 6)
                .mapToObj(index -> Upstream.builder().url("upstream-" + index).weight(index == heavyIndex ? 100 : 0).build())
                .collect(Collectors.toList());
        final List<Upstream> observed = new ArrayList<>();
        final RandomLoadBalancer balancer = new RandomLoadBalancer() {
            @Override
            protected int getWeight(final Upstream upstream) {
                assertTrue(observed.size() < targets.size());
                assertSame(targets.get(observed.size()), upstream);
                observed.add(upstream);
                return super.getWeight(upstream);
            }
        };
        assertTrue(targets.contains(balancer.doSelect(targets, new LoadBalanceData())));
        assertEquals(targets, observed);
    }

    @Test
    public void rejectsEmptyAndNullListsInDoSelect() {
        final RandomLoadBalancer balancer = new RandomLoadBalancer();
        final LoadBalanceData data = new LoadBalanceData();
        assertThrows(IndexOutOfBoundsException.class, () -> balancer.doSelect(List.of(), data));
        assertThrows(NullPointerException.class, () -> balancer.doSelect(null, data));
    }

    /**
     * random load balance test.
     */
    @Test
    public void randomLoadBalanceOrderedWeightTest() {
        final Upstream upstreamOrdered = new RandomLoadBalancer().select(Stream.of(10, 40, 50)
                .map(weight -> Upstream.builder()
                        .url("upstream-" + weight)
                        .weight(weight)
                        .build())
                .collect(Collectors.toList()), new LoadBalanceData());
        assertNotNull(upstreamOrdered);
    }

    @Test
    public void randomLoadBalanceDisOrderedWeightTest() {
        final Upstream upstreamDisordered = new RandomLoadBalancer().select(Stream.of(10, 50, 40)
                .map(weight -> Upstream.builder()
                        .url("upstream-" + weight)
                        .weight(weight)
                        .build())
                .collect(Collectors.toList()), new LoadBalanceData());
        assertNotNull(upstreamDisordered);
    }

    @Test
    public void randomLoadBalanceReversedWeightTest() {
        final Upstream upstreamReversed = new RandomLoadBalancer().select(Stream.of(50, 40, 10)
                .map(weight -> Upstream.builder()
                        .url("upstream-" + weight)
                        .weight(weight)
                        .build())
                .collect(Collectors.toList()), new LoadBalanceData());
        assertNotNull(upstreamReversed);
    }

    @Test
    public void zeroWeightAndDisabledTargetsAreExcludedWhenPositiveWeightExists() {
        Upstream zero = Upstream.builder().url("zero").weight(0).build();
        Upstream disabled = Upstream.builder().url("disabled").weight(100).status(false).build();
        Upstream active = Upstream.builder().url("active").weight(10).build();
        List<Upstream> targets = List.of(zero, active, disabled);
        RandomLoadBalancer balancer = new RandomLoadBalancer();
        for (int index = 0; index < 1000; index++) {
            assertSame(active, balancer.select(targets, new LoadBalanceData()));
        }
    }

    @Test
    public void concurrentSelectionsRespectRelativeWeights() throws Exception {
        List<Upstream> targets = List.of(Upstream.builder().url("first").weight(10).build(),
                Upstream.builder().url("second").weight(30).build(), Upstream.builder().url("third").weight(60).build());
        RandomLoadBalancer balancer = new RandomLoadBalancer();
        AtomicIntegerArray counts = new AtomicIntegerArray(3);
        CompletableFuture<?>[] requests = new CompletableFuture<?>[4];
        for (int worker = 0; worker < requests.length; worker++) {
            requests[worker] = CompletableFuture.runAsync(() -> {
                LoadBalanceData data = new LoadBalanceData();
                for (int request = 0; request < 10000; request++) {
                    Upstream selected = balancer.select(targets, data);
                    counts.incrementAndGet(targets.indexOf(selected));
                }
            });
        }
        CompletableFuture.allOf(requests).get(10, TimeUnit.SECONDS);
        // Broad bounds detect broken weighting without depending on an exact random sequence.
        assertTrue(counts.get(0) > 3000 && counts.get(0) < 5000);
        assertTrue(counts.get(1) > 10000 && counts.get(1) < 14000);
        assertTrue(counts.get(2) > 22000 && counts.get(2) < 26000);
    }


    @Test
    public void preservesCustomWeightOverrides() {
        Upstream preferred = Upstream.builder().url("preferred").weight(0).build();
        Upstream ordinary = Upstream.builder().url("ordinary").weight(100).build();
        RandomLoadBalancer customized = new RandomLoadBalancer() {
            @Override
            protected int getWeight(final Upstream upstream) {
                return upstream == preferred ? 100 : 0;
            }
        };
        for (int index = 0; index < 100; index++) {
            assertSame(preferred, customized.select(List.of(ordinary, preferred), new LoadBalanceData()));
        }
    }

    @Test
    public void warmupWeightStillControlsDistribution() {
        long now = System.currentTimeMillis();
        Upstream warming = Upstream.builder().url("warming").weight(100).warmup(600000).timestamp(now - 60000).build();
        Upstream mature = Upstream.builder().url("mature").weight(10).warmup(600000).timestamp(now - 1200000).build();
        RandomLoadBalancer balancer = new RandomLoadBalancer();
        List<Upstream> targets = List.of(warming, mature);
        LoadBalanceData data = new LoadBalanceData();
        int warmingSelections = 0;
        for (int index = 0; index < 20000; index++) {
            if (balancer.select(targets, data) == warming) {
                warmingSelections++;
            }
        }
        assertTrue(warmingSelections > 8000 && warmingSelections < 12000);
    }

}
