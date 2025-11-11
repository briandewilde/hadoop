/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ipc.metrics;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.ExternalCall;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.MetricsAsserts;
import org.junit.Test;

public class TestRpcMetrics {

  @Test
  public void metricsAreUnregistered() throws Exception {

    Configuration conf = new Configuration();
    Server server = new Server("0.0.0.0", 0, LongWritable.class, 1, conf) {
      @Override
      public Writable call(
          RPC.RpcKind rpcKind, String protocol, Writable param,
          long receiveTime) throws Exception {
        return null;
      }
    };
    MetricsSystem metricsSystem = DefaultMetricsSystem.instance();
    RpcMetrics rpcMetrics = server.getRpcMetrics();
    RpcDetailedMetrics rpcDetailedMetrics = server.getRpcDetailedMetrics();

    assertNotNull(metricsSystem.getSource(rpcMetrics.name()));
    assertNotNull(metricsSystem.getSource(rpcDetailedMetrics.name()));

    server.stop();

    assertNull(metricsSystem.getSource(rpcMetrics.name()));
    assertNull(metricsSystem.getSource(rpcDetailedMetrics.name()));

  }

  @Test
  public void metricsSupportsAliases() throws Exception {
    // Instantiate a no-op server
    Configuration conf = new Configuration();
    Server server = new Server("0.0.0.0", 0, LongWritable.class, 1, conf) {
      @Override
      public Writable call(RPC.RpcKind rpcKind, String protocol, Writable param,
          long receiveTime) {
        return null;
      }
    };
    server.start();

    // Create a simple action that creates a metrics alias
    PrivilegedExceptionAction<Void> action = () -> {
      Server.Call call = RPC.Server.getCurCall().get();
      call.addDetailedMetricsAlias("Alias");
      return null; };

    // Create a simple call to run the action
    ExternalCall<Void> call = new ExternalCall<Void>(action) {
      @Override
      public UserGroupInformation getRemoteUser() {
        return null;
      }
    };

    try {
      // Enqueue the call and wait for it to complete
      server.queueCall(call);
      call.get();

      // Grab a reference to the MetricsRecordBuilder for inspection later
      MetricsRecordBuilder mrb =
          MetricsAsserts.getMetrics(server.getRpcDetailedMetrics().name());
      // Ensure metrics snapshot has occurred by stopping the server
      server.stop();
      // Ensure the metrics contains the alias
      MetricsAsserts.assertCounter("AliasNumOps", 1L, mrb);
    } finally {
      server.stop();
    }
  }
}
