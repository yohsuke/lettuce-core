/*
 * Copyright 2011-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lettuce.core.cluster.topology;

import io.lettuce.core.RedisURI;
import io.lettuce.core.TestSupport;
import io.lettuce.core.cluster.ClusterTestSettings;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.test.Wait;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterTopologyChangedEventIntegrationTests2 extends TestSupport {

    private RedisClusterClient clusterClient;
    private AtomicLong topologyChangedEventCounter;
    private int initialPartitionSize;

    @BeforeEach
    void before() {
        clusterClient = RedisClusterClient
                .create(RedisURI.Builder.redis(host, ClusterTestSettings.port1).build());

        topologyChangedEventCounter = new AtomicLong(0);
        clusterClient.getResources().eventBus().get()
                .filter(event -> event instanceof ClusterTopologyChangedEvent)
                .cast(ClusterTopologyChangedEvent.class)
                .subscribe(event -> topologyChangedEventCounter.incrementAndGet());

        initialPartitionSize = clusterClient.getPartitions().size();
        assertThat(initialPartitionSize).isNotZero();
    }


    @AfterEach
    void after() {
        clusterClient.shutdown();
    }


    @Test
    void eventWillOccurWhenUpdatePartition() {

        // Check the event count should be the same as the initial value (=0).
        assertThat(topologyChangedEventCounter.get()).isEqualTo(0);

        // Force update the partition cache.

        // Call #clear to empty the partition manually.
        clusterClient.getPartitions().clear();
        assertThat(clusterClient.getPartitions().size()).isZero();

        // This #reloadPartitions will force the partition count to be updated/restored.
        clusterClient.reloadPartitions();
        assertThat(clusterClient.getPartitions().size()).isEqualTo(initialPartitionSize);

        // Wait ClusterTopologyChangedEvent happens.
        // The #reloadPartitions will publish a ClusterTopologyChangedEvent (possibly within 1 sec).
        Wait.untilNotEquals(0L, topologyChangedEventCounter::get)
                .during(Duration.ofSeconds(1)).waitOrTimeout();
        assertThat(topologyChangedEventCounter.get()).isEqualTo(1);
    }

    private static List<String> getCurrentNodeIdList(RedisClusterClient clusterClient) {
        return clusterClient
                .getPartitions()
                .stream()
                .map(RedisClusterNode::getNodeId)
                .collect(Collectors.toList());
    }

    @Test
    void eventWillNotOccurWhenReloadSamePartitions() {

        // Check the event count should be the same as the initial value (=0).
        assertThat(topologyChangedEventCounter.get()).isEqualTo(0);

        // This #reloadPartitions will not change the partition.
        // (simply reloading the same value, so NodeId set will have same values.)
        List<String> nodeIdListBeforeReload = getCurrentNodeIdList(clusterClient);
        clusterClient.reloadPartitions();
        List<String> nodeIdListAfterReload = getCurrentNodeIdList(clusterClient);

        assertThat(nodeIdListAfterReload).containsExactlyInAnyOrderElementsOf(nodeIdListBeforeReload);

        // The #reloadPartitions will not publish ClusterTopologyChangedEvent.
        // So this change waiting will time out.
        // And the event count will be the same as the initial value (=0).
        assertThatThrownBy(
                () -> Wait.untilNotEquals(0, topologyChangedEventCounter::get)
                        .during(Duration.ofSeconds(1)).waitOrTimeout()
        ).hasCauseInstanceOf(TimeoutException.class);
        assertThat(topologyChangedEventCounter.get()).isEqualTo(0);
    }
}
