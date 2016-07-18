/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.base;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.Container._Fields;
import org.apache.aurora.gen.DockerContainer;
import org.apache.aurora.gen.DockerParameter;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.MesosFetcherURI;
import org.apache.aurora.gen.Metadata;
import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.TierManager.TierManagerImpl.TierConfig;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.ConfigurationManagerSettings;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.log.ThriftBackfill;

/**
 * Convenience methods for working with tasks.
 * <p>
 * TODO(wfarner): This lives in under src/main only so benchmarks can access it.  Reconsider the
 * project layout so this is not necessary.
 */
public final class TaskTestUtil {

  public static final IJobKey JOB = JobKeys.from("role", "dev", "job");
  public static final TierInfo REVOCABLE_TIER =
      new TierInfo(true /* preemptible */, true /* revocable */);
  public static final TierInfo DEV_TIER =
      new TierInfo(true /* preemptible */, false /* revocable */);
  public static final TierInfo PREFERRED_TIER =
      new TierInfo(false /* preemptible */, false /* revocable */);
  public static final String PROD_TIER_NAME = "tier-prod";
  public static final String DEV_TIER_NAME = "tier-dev";
  public static final TierConfig TIER_CONFIG =
      new TierConfig(DEV_TIER_NAME, ImmutableMap.of(
          PROD_TIER_NAME, PREFERRED_TIER,
          DEV_TIER_NAME, DEV_TIER
      ));
  public static final TierManager TIER_MANAGER = new TierManager.TierManagerImpl(TIER_CONFIG);
  public static final ThriftBackfill THRIFT_BACKFILL = new ThriftBackfill(TIER_MANAGER);
  public static final ConfigurationManagerSettings CONFIGURATION_MANAGER_SETTINGS =
      new ConfigurationManagerSettings(
          ImmutableSet.of(_Fields.MESOS),
          false,
          ImmutableMultimap.of(),
          true,
          true,
          true);
  public static final String EXECUTOR_NAME = "name";
  public static final ExecutorSettings EXECUTOR_SETTINGS = new ExecutorSettings(
      ImmutableMap.<String, org.apache.aurora.scheduler.configuration.executor.ExecutorConfig>builder().put(EXECUTOR_NAME, null).build(), false);
  public static final ConfigurationManager CONFIGURATION_MANAGER =
      new ConfigurationManager(CONFIGURATION_MANAGER_SETTINGS,
          TIER_MANAGER,
          THRIFT_BACKFILL,
          EXECUTOR_SETTINGS);

  private TaskTestUtil() {
    // Utility class.
  }

  public static ITaskConfig makeConfig(IJobKey job) {
    return ITaskConfig.build(new TaskConfig()
        .setJob(job.newBuilder())
        .setOwner(new Identity().setUser(job.getRole() + "-user"))
        .setIsService(true)
        .setNumCpus(1.0)
        .setRamMb(1024)
        .setDiskMb(1024)
        .setPriority(1)
        .setMaxTaskFailures(-1)
        .setProduction(true)
        .setTier(PROD_TIER_NAME)
        .setConstraints(ImmutableSet.of(
            new Constraint(
                "valueConstraint",
                TaskConstraint.value(
                    new ValueConstraint(true, ImmutableSet.of("value1", "value2")))),
            new Constraint(
                "limitConstraint",
                TaskConstraint.limit(new LimitConstraint(5)))))
        .setRequestedPorts(ImmutableSet.of("http"))
        .setTaskLinks(ImmutableMap.of("http", "link", "admin", "otherLink"))
        .setContactEmail("foo@bar.com")
        .setMetadata(ImmutableSet.of(new Metadata("key", "value")))
        .setMesosFetcherUris(ImmutableSet.of(
            new MesosFetcherURI("pathA").setExtract(true).setCache(true),
            new MesosFetcherURI("pathB").setExtract(true).setCache(true)))
        .setExecutorConfig(new ExecutorConfig(EXECUTOR_NAME, "config"))
        .setContainer(Container.docker(
            new DockerContainer("imagename")
                .setParameters(ImmutableList.of(
                    new DockerParameter("a", "b"),
                    new DockerParameter("c", "d")))))
        .setResources(ImmutableSet.of(
            Resource.numCpus(1.0),
            Resource.ramMb(1024),
            Resource.diskMb(1024),
            Resource.namedPort("http"))));
  }

  public static IScheduledTask makeTask(String id, IJobKey job) {
    return makeTask(id, makeConfig(job));
  }

  public static IScheduledTask makeTask(String id, ITaskConfig config) {
    return IScheduledTask.build(new ScheduledTask()
        .setStatus(ScheduleStatus.ASSIGNED)
        .setTaskEvents(ImmutableList.of(
            new TaskEvent(100L, ScheduleStatus.PENDING)
                .setMessage("message")
                .setScheduler("scheduler"),
            new TaskEvent(101L, ScheduleStatus.ASSIGNED)
                .setMessage("message")
                .setScheduler("scheduler2")))
        .setAncestorId("ancestor")
        .setFailureCount(3)
        .setAssignedTask(new AssignedTask()
            .setInstanceId(2)
            .setTaskId(id)
            .setAssignedPorts(ImmutableMap.of("http", 1000))
            .setTask(config.newBuilder())));
  }

  public static IScheduledTask addStateTransition(
      IScheduledTask task,
      ScheduleStatus status,
      long timestamp) {

    ScheduledTask builder = task.newBuilder();
    builder.setStatus(status);
    builder.addToTaskEvents(new TaskEvent()
        .setTimestamp(timestamp)
        .setStatus(status)
        .setScheduler("scheduler"));
    return IScheduledTask.build(builder);
  }

  public static String tierConfigFile() {
    return "{\"default\": \"preemptible\","
        + "\"tiers\":{"
        + "\"preferred\": {\"revocable\": false, \"preemptible\": false},"
        + "\"preemptible\": {\"revocable\": false, \"preemptible\": true},"
        + "\"revocable\": {\"revocable\": true, \"preemptible\": true}"
        + "}}";
  }

  public static Map<String, TierInfo> tierInfos() {
    return ImmutableMap.of(
        "preferred", PREFERRED_TIER,
        "preemptible", DEV_TIER,
        "revocable", REVOCABLE_TIER);
  }

  public static Set<org.apache.aurora.gen.TierConfig> tierConfigs() {
    return ImmutableSet.of(
        new org.apache.aurora.gen.TierConfig("preferred", PREFERRED_TIER.toMap()),
        new org.apache.aurora.gen.TierConfig("preemptible", DEV_TIER.toMap()),
        new org.apache.aurora.gen.TierConfig("revocable", REVOCABLE_TIER.toMap())
    );
  }
}
