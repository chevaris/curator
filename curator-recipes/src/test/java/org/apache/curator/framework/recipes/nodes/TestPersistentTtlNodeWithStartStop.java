/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.curator.framework.recipes.nodes;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.Timing;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestPersistentTtlNodeWithStartStop extends CuratorTestBase {
  private final Timing timing = new Timing();

  @BeforeAll
  public static void setUpClass() {
    System.setProperty("zookeeper.extendedTypesEnabled", "true");
  }

  @BeforeEach
  @Override
  public void setup() throws Exception {
    System.setProperty("znode.container.checkIntervalMs", "1");
    super.setup();
  }

  @AfterEach
  @Override
  public void teardown() throws Exception {
    System.clearProperty("znode.container.checkIntervalMs");
    super.teardown();
  }

  @Test
  public void testWithPersistentTtlNode() throws Exception {
    try (CuratorFramework client =
        CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
      client.start();
      final long ttlMs = 1_000L;
      try (PersistentTtlNode node = new PersistentTtlNode(client, "/test", ttlMs, new byte[0])) {
        node.start();
        assertTrue(node.waitForInitialCreate(timing.session(), TimeUnit.MILLISECONDS));
        // Give some minor time for touch node to be created. Will worked after patch
        for (int i = 1; i <= 5; i++) {
          if (client.checkExists().forPath("/test") != null) {
            break;
          }
          Thread.sleep(10L);
        }
      }
    }
    try (CuratorFramework client1 =
        CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
      client1.start();
      assertTrue(client1.blockUntilConnected(2, TimeUnit.SECONDS));
      Thread.sleep(3_000L);
      assertNull(client1.checkExists().forPath("/test/touch"));
      assertNull(
          client1.checkExists().forPath("/test"),
          "Persistent TTL node NOT removed. The reason is that '/test/touch' was NOT create on time to make PerssistentTTLNode recipe to work");
    }
  }

  @Test
  public void testWithPersistentTtlNodeWithWatcher() throws Exception {
    try (CuratorFramework client =
        CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
      client.start();
      final long ttlMs = 1_000L;
      try (PersistentTtlNodeWithWatcher node =
          new PersistentTtlNodeWithWatcher(client, "/test", ttlMs, new byte[0])) {
        node.start();
        assertTrue(node.waitForInitialCreate(timing.session(), TimeUnit.MILLISECONDS));
        // Give some minor time for touch node to be created. Will worked after patch
        for (int i = 1; i <= 5; i++) {
          if (client.checkExists().forPath("/test") == null) {
            break;
          }
          Thread.sleep(10L);
        }
      }
    }
    try (CuratorFramework client1 =
        CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
      client1.start();
      assertTrue(client1.blockUntilConnected(2, TimeUnit.SECONDS));
      Thread.sleep(3_000L);
      assertNull(client1.checkExists().forPath("/test/touch"));
      assertNull(
          client1.checkExists().forPath("/test"),
          "Persistent TTL node NOT removed. The reason is that '/test/touch' was NOT create on time to make PerssistentTTLNode recipe to work");
    }
  }
}
