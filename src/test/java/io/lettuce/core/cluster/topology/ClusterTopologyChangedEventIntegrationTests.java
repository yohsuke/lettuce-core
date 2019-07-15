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
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterTopologyChangedEventIntegrationTests extends TestSupport {

    private static List<String> getCurrentNodeIdList(RedisClusterClient clusterClient) {
        return clusterClient
                .getPartitions()
                .stream()
                .map(RedisClusterNode::getNodeId)
                .collect(Collectors.toList());
    }

    @Test
    void test() {

        int initialCount = 0;
        AtomicLong topologyChangedEventCounter = new AtomicLong(initialCount);

        RedisClusterClient clusterClient = RedisClusterClient
                .create(RedisURI.Builder.redis(host, ClusterTestSettings.port1).build());

        clusterClient.getResources().eventBus().get()
                .filter(event -> event instanceof ClusterTopologyChangedEvent)
                .cast(ClusterTopologyChangedEvent.class)
                .subscribe(event -> topologyChangedEventCounter.incrementAndGet());


        // Check the event count should be the same as the initial value (=0).
        assertThat(topologyChangedEventCounter.get()).isEqualTo(initialCount);

        // This #reloadPartitions will not change the partition.
        // (simply reloading the same value, so NodeIds will have same values.)
        List<String> nodeIdListBeforeReload = getCurrentNodeIdList(clusterClient);
        clusterClient.reloadPartitions();
        List<String> nodeIdListAfterReload = getCurrentNodeIdList(clusterClient);

        assertThat(nodeIdListAfterReload).containsExactlyInAnyOrderElementsOf(nodeIdListBeforeReload);

        // If the #reloadPartitions does not publish ClusterTopologyChangedEvent,
        // this count change wait will time out.
        try {
            Wait.untilNotEquals(initialCount, topologyChangedEventCounter::get)
                    .during(Duration.ofSeconds(1)).waitOrTimeout();
            System.out.println("ClusterTopologyChangedEvent occurs.");
        } catch (IllegalStateException ignored) {
            System.out.println("Timed out. It seems that ClusterTopologyChangedEvent did not occur.");
        }

        clusterClient.shutdown();
    }
}
