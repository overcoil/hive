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

package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.split.TezMapReduceSplitsGrouper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.mapreduce.hadoop.InputSplitInfoMem;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.InputSpecUpdate;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

/**
 * This class is used to generate splits inside the AM on the cluster. It
 * optionally groups together splits based on available head room as well as
 * making sure that splits from different partitions are only grouped if they
 * are of the same schema, format and serde
 */
public class HiveSplitGenerator extends InputInitializer {

  private static final Logger LOG = LoggerFactory.getLogger(HiveSplitGenerator.class);

  private final DynamicPartitionPruner pruner;
  private final Configuration conf;
  private final JobConf jobConf;
  private final MRInputUserPayloadProto userPayloadProto;
  private final MapWork work;
  private final SplitGrouper splitGrouper = new SplitGrouper();

  private static final String MIN_SPLIT_SIZE;
  @SuppressWarnings("unused")
  private static final String MAX_SPLIT_SIZE;

  static {
    final HadoopShims SHIMS = ShimLoader.getHadoopShims();
    MIN_SPLIT_SIZE = SHIMS.getHadoopConfNames().get("MAPREDMINSPLITSIZE");
    MAX_SPLIT_SIZE = SHIMS.getHadoopConfNames().get("MAPREDMAXSPLITSIZE");
  }

  public HiveSplitGenerator(InputInitializerContext initializerContext) throws IOException,
      SerDeException {
    super(initializerContext);
    Preconditions.checkNotNull(initializerContext);
    userPayloadProto =
        MRInputHelpers.parseMRInputPayload(initializerContext.getInputUserPayload());

    this.conf = TezUtils.createConfFromByteString(userPayloadProto.getConfigurationBytes());

    this.jobConf = new JobConf(conf);

    // Read all credentials into the credentials instance stored in JobConf.
    ShimLoader.getHadoopShims().getMergedCredentials(jobConf);

    this.work = Utilities.getMapWork(jobConf);

    // Events can start coming in the moment the InputInitializer is created. The pruner
    // must be setup and initialized here so that it sets up it's structures to start accepting events.
    // Setting it up in initialize leads to a window where events may come in before the pruner is
    // initialized, which may cause it to drop events.
    pruner = new DynamicPartitionPruner(initializerContext, work, jobConf);

  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Event> initialize() throws Exception {
    // Setup the map work for this thread. Pruning modified the work instance to potentially remove
    // partitions. The same work instance must be used when generating splits.
    Utilities.setMapWork(jobConf, work);
    try {
      boolean sendSerializedEvents =
          conf.getBoolean("mapreduce.tez.input.initializer.serialize.event.payload", true);

      // perform dynamic partition pruning
      pruner.prune();

      InputSplitInfoMem inputSplitInfo = null;
      boolean generateConsistentSplits = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_TEZ_GENERATE_CONSISTENT_SPLITS);
      LOG.info("GenerateConsistentSplitsInHive=" + generateConsistentSplits);
      String realInputFormatName = conf.get("mapred.input.format.class");
      boolean groupingEnabled = userPayloadProto.getGroupingEnabled();
      if (groupingEnabled) {
        // Need to instantiate the realInputFormat
        InputFormat<?, ?> inputFormat =
          (InputFormat<?, ?>) ReflectionUtils.newInstance(JavaUtils.loadClass(realInputFormatName),
              jobConf);

        int totalResource = getContext().getTotalAvailableResource().getMemory();
        int taskResource = getContext().getVertexTaskResource().getMemory();
        int availableSlots = totalResource / taskResource;

        if (conf.getLong(MIN_SPLIT_SIZE, 1) <= 1) {
          // broken configuration from mapred-default.xml
          final long blockSize = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
              DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
          final long minGrouping = conf.getLong(
              TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_MIN_SIZE,
              TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_MIN_SIZE_DEFAULT);
          final long preferredSplitSize = Math.min(blockSize / 2, minGrouping);
          jobConf.setLong(MIN_SPLIT_SIZE, preferredSplitSize);
          LOG.info("The preferred split size is " + preferredSplitSize);
        }

        // Create the un-grouped splits
        float waves =
            conf.getFloat(TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_WAVES,
                TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_WAVES_DEFAULT);

        InputSplit[] splits = inputFormat.getSplits(jobConf, (int) (availableSlots * waves));
        // Sort the splits, so that subsequent grouping is consistent.
        Arrays.sort(splits, new InputSplitComparator());
        LOG.info("Number of input splits: " + splits.length + ". " + availableSlots
            + " available slots, " + waves + " waves. Input format is: " + realInputFormatName);

        Multimap<Integer, InputSplit> groupedSplits =
            splitGrouper.generateGroupedSplits(jobConf, conf, splits, waves, availableSlots);
        // And finally return them in a flat array
        InputSplit[] flatSplits = groupedSplits.values().toArray(new InputSplit[0]);
        LOG.info("Number of grouped splits: " + flatSplits.length);

        List<TaskLocationHint> locationHints = splitGrouper.createTaskLocationHints(flatSplits, generateConsistentSplits);

        inputSplitInfo =
            new InputSplitInfoMem(flatSplits, locationHints, flatSplits.length, null, jobConf);
      } else {
        // no need for grouping and the target #of tasks.
        // This code path should never be triggered at the moment. If grouping is disabled,
        // DAGUtils uses MRInputAMSplitGenerator.
        // If this is used in the future - make sure to disable grouping in the payload, if it isn't already disabled
        throw new RuntimeException(
            "HiveInputFormat does not support non-grouped splits, InputFormatName is: "
                + realInputFormatName);
        // inputSplitInfo = MRInputHelpers.generateInputSplitsToMem(jobConf, false, 0);
      }

      return createEventList(sendSerializedEvents, inputSplitInfo);
    } finally {
      Utilities.clearWork(jobConf);
    }
  }




  private List<Event> createEventList(boolean sendSerializedEvents, InputSplitInfoMem inputSplitInfo) {

    List<Event> events = Lists.newArrayListWithCapacity(inputSplitInfo.getNumTasks() + 1);

    InputConfigureVertexTasksEvent configureVertexEvent =
        InputConfigureVertexTasksEvent.create(inputSplitInfo.getNumTasks(),
        VertexLocationHint.create(inputSplitInfo.getTaskLocationHints()),
        InputSpecUpdate.getDefaultSinglePhysicalInputSpecUpdate());
    events.add(configureVertexEvent);

    if (sendSerializedEvents) {
      MRSplitsProto splitsProto = inputSplitInfo.getSplitsProto();
      int count = 0;
      for (MRSplitProto mrSplit : splitsProto.getSplitsList()) {
        InputDataInformationEvent diEvent = InputDataInformationEvent.createWithSerializedPayload(
            count++, mrSplit.toByteString().asReadOnlyByteBuffer());
        events.add(diEvent);
      }
    } else {
      int count = 0;
      for (org.apache.hadoop.mapred.InputSplit split : inputSplitInfo.getOldFormatSplits()) {
        InputDataInformationEvent diEvent = InputDataInformationEvent.createWithObjectPayload(
            count++, split);
        events.add(diEvent);
      }
    }
    return events;
  }

  @Override
  public void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
    pruner.processVertex(stateUpdate.getVertexName());
  }

  @Override
  public void handleInputInitializerEvent(List<InputInitializerEvent> events) throws Exception {
    for (InputInitializerEvent e : events) {
      pruner.addEvent(e);
    }
  }

  // Descending sort based on split size| Followed by file name. Followed by startPosition.
  private static class InputSplitComparator implements Comparator<InputSplit> {
    @Override
    public int compare(InputSplit o1, InputSplit o2) {
      try {
        long len1 = o1.getLength();
        long len2 = o2.getLength();
        if (len1 < len2) {
          return 1;
        } else if (len1 == len2) {
          // If the same size. Sort on file name followed by startPosition.
          if (o1 instanceof FileSplit && o2 instanceof FileSplit) {
            FileSplit fs1 = (FileSplit) o1;
            FileSplit fs2 = (FileSplit) o2;
            if (fs1.getPath() != null && fs2.getPath() != null) {
              int pathComp = (fs1.getPath().compareTo(fs2.getPath()));
              if (pathComp == 0) {
                // Compare start Position
                long startPos1 = fs1.getStart();
                long startPos2 = fs2.getStart();
                if (startPos1 > startPos1) {
                  return 1;
                } else if (startPos1 < startPos2) {
                  return -1;
                } else {
                  return 0;
                }
              } else {
                return pathComp;
              }
            }
          }
          // No further checks if not a file split. Return equality.
          return 0;
        } else {
          return -1;
        }
      } catch (IOException e) {
        throw new RuntimeException("Problem getting input split size", e);
      }
    }
  }
}
