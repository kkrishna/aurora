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
import java.util.Objects;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import org.apache.aurora.gen.Volume;
import org.apache.aurora.scheduler.Resources;

import static java.util.Objects.requireNonNull;

/**
 * Configuration for the executor to run, and resource overhead required for it.
 */
public final class ExecutorSettings {
  private final String executorName;
  private final String executorPath;
  private final List<String> executorResources;
  private final String thermosObserverRoot;
  private final Optional<String> executorFlags;
  private final Resources executorOverhead;
  private final List<Volume> globalContainerMounts;
  //private final JsonObject dataBlob;

  ExecutorSettings(
      String executorName,
      String executorPath,
      List<String> executorResources,
      String thermosObserverRoot,
      Optional<String> executorFlags,
      Resources executorOverhead,
      List<Volume> globalContainerMounts) {

    //TODO: Remove check when observer is axed
    if ("thermos".equals(executorName)) {
      requireNonNull(thermosObserverRoot);
    }

    /**
     * Path is ignored for Mesos-Command, set it to an empty string to avoid breakage
     *  for now in case its not in the config file
     */
    if ("mesos-command".equals(executorName)) {
      this.executorPath = "";
    } else {
      this.executorPath = requireNonNull(executorPath);
    }

    this.executorName = requireNonNull(executorName);
    this.executorResources = requireNonNull(executorResources);
    this.thermosObserverRoot = thermosObserverRoot;
    this.executorFlags = executorFlags;
    this.executorOverhead = requireNonNull(executorOverhead);
    this.globalContainerMounts = requireNonNull(globalContainerMounts);

  }

  public String getExecutorName() {
    return executorName;
  }

  public String getExecutorPath() {
    return executorPath;
  }

  public List<String> getExecutorResources() {
    return executorResources;
  }

  public String getThermosObserverRoot() {
    return thermosObserverRoot;
  }

  public Optional<String> getExecutorFlags() {
    return executorFlags;
  }

  public Resources getExecutorOverhead() {
    return executorOverhead;
  }

  public List<Volume> getGlobalContainerMounts() {
    return globalContainerMounts;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        executorName,
        executorPath,
        executorResources,
        thermosObserverRoot,
        executorPath,
        executorOverhead,
        globalContainerMounts);
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

    return Objects.equals(this.executorName, that.executorName)
        && Objects.equals(this.executorPath, that.executorPath)
        && Objects.equals(this.executorResources, that.executorResources)
        && Objects.equals(this.thermosObserverRoot, that.thermosObserverRoot)
        && Objects.equals(this.executorOverhead, that.executorOverhead)
        && Objects.equals(this.globalContainerMounts, that.globalContainerMounts);
  }

  public static final class Builder {
    private String executorName;
    private String executorPath;
    private List<String> executorResources;
    private String thermosObserverRoot;
    private Optional<String> executorFlags;
    private Resources executorOverhead;
    private List<Volume> globalContainerMounts;

    Builder() {
      executorResources = ImmutableList.of();
      executorFlags = Optional.absent();
      executorOverhead = Resources.NONE;
      globalContainerMounts = ImmutableList.of();
    }

    public Builder setExecutorName(String executorName) {
      this.executorName = executorName;
      return this;
    }

    public Builder setExecutorPath(String executorPath) {
      this.executorPath = executorPath;
      return this;
    }

    public Builder setExecutorResources(List<String> executorResources) {
      this.executorResources = executorResources;
      return this;
    }

    public Builder setThermosObserverRoot(String thermosObserverRoot) {
      this.thermosObserverRoot = thermosObserverRoot;
      return this;
    }

    public Builder setExecutorFlags(Optional<String> executorFlags) {
      this.executorFlags = executorFlags;
      return this;
    }

    public Builder setExecutorOverhead(Resources executorOverhead) {
      this.executorOverhead = executorOverhead;
      return this;
    }

    public Builder setGlobalContainerMounts(List<Volume> globalContainerMounts) {
      this.globalContainerMounts = globalContainerMounts;
      return this;
    }

    public ExecutorSettings build() {
      return new ExecutorSettings(
          executorName,
          executorPath,
          executorResources,
          thermosObserverRoot,
          executorFlags,
          executorOverhead,
          globalContainerMounts);
    }
  }
}
