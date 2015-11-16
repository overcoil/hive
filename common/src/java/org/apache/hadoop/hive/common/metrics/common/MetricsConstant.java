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
package org.apache.hadoop.hive.common.metrics.common;

/**
 * This class defines some metrics generated by Hive processes.
 */
public class MetricsConstant {

  public static final String JVM_PAUSE_INFO = "jvm.pause.info-threshold";
  public static final String JVM_PAUSE_WARN = "jvm.pause.warn-threshold";
  public static final String JVM_EXTRA_SLEEP = "jvm.pause.extraSleepTime";

  public static final String OPEN_CONNECTIONS = "open_connections";
  public static final String OPEN_OPERATIONS = "open_operations";

  public static final String JDO_ACTIVE_TRANSACTIONS = "active_jdo_transactions";
  public static final String JDO_ROLLBACK_TRANSACTIONS = "rollbacked_jdo_transactions";
  public static final String JDO_COMMIT_TRANSACTIONS = "committed_jdo_transactions";
  public static final String JDO_OPEN_TRANSACTIONS = "opened_jdo_transactions";

  public static final String METASTORE_HIVE_LOCKS = "metastore_hive_locks";
  public static final String ZOOKEEPER_HIVE_SHAREDLOCKS = "zookeeper_hive_sharedlocks";
  public static final String ZOOKEEPER_HIVE_EXCLUSIVELOCKS = "zookeeper_hive_exclusivelocks";
  public static final String ZOOKEEPER_HIVE_SEMISHAREDLOCKS = "zookeeper_hive_semisharedlocks";
}
