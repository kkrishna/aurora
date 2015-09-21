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
package org.apache.aurora.scheduler.mesos;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.Volume;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

/**
 * Configuration for the executor to run, and resource overhead required for it.
 */
public final class ExecutorSettings {
  private final String executorName;
  private final CommandInfo.Builder commandInfo;
  private final ResourceSlot executorOverhead;
  private final List<Volume> globalContainerMounts;
  private final String thermosObserverRoot;
  private final Map<String, String> config;

  private static final String THERMOS_NAME = "AuroraExecutor";

  ExecutorSettings(
      String executorName,
      CommandInfo.Builder commandInfo,
      String thermosObserverRoot,
      ResourceSlot executorOverhead,
      List<Volume> globalContainerMounts,
      Map<String, String> config) {

    this.executorName = executorName;
    this.commandInfo = requireNonNull(commandInfo);
    this.executorOverhead = requireNonNull(executorOverhead);
    this.globalContainerMounts = requireNonNull(globalContainerMounts);
    this.config = config;

    if (THERMOS_NAME.equals(executorName)) {
      this.thermosObserverRoot = requireNonNull(thermosObserverRoot);
    } else {
      this.thermosObserverRoot = "";
    }
  }

  public String getExecutorName() {
    return executorName;
  }

  public CommandInfo.Builder getCommandInfo() {
    return commandInfo;
  }

  public String getThermosObserverRoot() {
    return thermosObserverRoot;
  }

  public ResourceSlot getExecutorOverhead() {
    return executorOverhead;
  }

  public List<Volume> getGlobalContainerMounts() {
    return globalContainerMounts;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        executorName,
        commandInfo,
        thermosObserverRoot,
        executorOverhead,
        globalContainerMounts,
        config);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (getClass() != obj.getClass()) {
      return false;
    }

    final ExecutorSettings that = (ExecutorSettings) obj;

    return Objects.equals(executorName, that.executorName)
        && Objects.equals(commandInfo.build(), that.commandInfo.build())
        && Objects.equals(thermosObserverRoot, that.thermosObserverRoot)
        && Objects.equals(executorOverhead, that.executorOverhead)
        && Objects.equals(globalContainerMounts, that.globalContainerMounts)
        && Objects.equals(config, that.config);
  }

  @Override
  public String toString() {
    return com.google.common.base.MoreObjects.toStringHelper(this)
        .add("executorName", executorName)
        .add("commandInfo", commandInfo)
        .add("executorOverhead", executorOverhead)
        .add("globalContainerMounts", globalContainerMounts)
        .add("thermosObserverRoot", thermosObserverRoot)
        .add("config", config)
        .toString();
  }

  public static final class Builder {
    private String executorName;
    private CommandInfo.Builder commandInfo;
    private String thermosObserverRoot;
    private ResourceSlot executorOverhead;
    private List<Volume> globalContainerMounts;
    private Map<String, String> config;

    Builder() {
      executorOverhead = ResourceSlot.NONE;
      globalContainerMounts = ImmutableList.of();
      config = ImmutableMap.of();
    }

    public Builder setExecutorName(String executorName) {
      this.executorName = executorName;
      return this;
    }

    public Builder setCommandInfo(CommandInfo.Builder commandInfo) {
      this.commandInfo = commandInfo;
      return this;
    }

    public Builder setThermosObserverRoot(String thermosObserverRoot) {
      this.thermosObserverRoot = thermosObserverRoot;
      return this;
    }

    public Builder setExecutorOverhead(ResourceSlot executorOverhead) {
      this.executorOverhead = executorOverhead;
      return this;
    }

    public Builder setGlobalContainerMounts(List<Volume> globalContainerMounts) {
      if (nonNull(globalContainerMounts)) {
        this.globalContainerMounts = globalContainerMounts;
      }
      return this;
    }

    public Builder setConfig(Map<String, String> config) {
      this.config = config;
      return this;
    }

    public ExecutorSettings build() {
      return new ExecutorSettings(
          executorName,
          commandInfo,
          thermosObserverRoot,
          executorOverhead,
          globalContainerMounts,
          config);
    }
  }
}
