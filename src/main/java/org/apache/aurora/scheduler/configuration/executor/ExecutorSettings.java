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
package org.apache.aurora.scheduler.configuration.executor;

import com.google.common.collect.ImmutableMap;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceManager;

import static java.util.Objects.requireNonNull;

/**
 * Configuration for the executor to run, and resource overhead required for it.
 */
public class ExecutorSettings {
  private final ImmutableMap<String, ExecutorConfig> config;
  private final boolean populateDiscoveryInfo;

  public ExecutorSettings(ImmutableMap<String,ExecutorConfig> config, boolean populateDiscoveryInfo) {
    this.config = requireNonNull(config);
    this.populateDiscoveryInfo = populateDiscoveryInfo;
  }

  public ExecutorConfig getExecutorConfig(String name) {
    return config.get(name);
  }

  public boolean executorConfigExists(String name) {
    return config.containsKey(name);
  }

  public boolean shouldPopulateDiscoverInfo() {
    return populateDiscoveryInfo;
  }

  public Optional<ResourceBag> getExecutorOverhead(String name) {
      return Optional.ofNullable(ResourceManager.bagFromMesosResources(
          config.get(name).getExecutor().getResourcesList()));
  }

  @Override
  public int hashCode() {
    return Objects.hash(config);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ExecutorSettings)) {
      return false;
    }

    ExecutorSettings other = (ExecutorSettings) obj;
    return Objects.equals(config, other.config);
  }
}
