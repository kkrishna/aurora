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
package org.apache.aurora.scheduler.preemptor;

import java.util.Objects;

import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

/**
 * A victim to be considered as a candidate for preemption.
 */
public final class PreemptionVictim {
  private final String slaveHost;
  private final boolean production;
  private final String role;
  private final int priority;
  private final ResourceSlot resourceSlot;
  private final String taskId;
  private final String executorName;

  private PreemptionVictim(
      String slaveHost,
      boolean production,
      String role,
      int priority,
      ResourceSlot resourceSlot,
      String taskId,
      String executorName) {

    this.slaveHost = slaveHost;
    this.production = production;
    this.role = role;
    this.priority = priority;
    this.resourceSlot = resourceSlot;
    this.taskId = taskId;
    this.executorName = executorName;
  }

  public static PreemptionVictim fromTask(IAssignedTask task) {
    ITaskConfig config = task.getTask();
    return new PreemptionVictim(
        task.getSlaveHost(),
        config.isProduction(),
        config.getJob().getRole(),
        config.getPriority(),
        ResourceSlot.from(task.getTask()),
        task.getTaskId(),
        config.getExecutorConfig().getName());
  }

  public String getSlaveHost() {
    return slaveHost;
  }

  public boolean isProduction() {
    return production;
  }

  public String getRole() {
    return role;
  }

  public int getPriority() {
    return priority;
  }

  public ResourceSlot getResourceSlot() {
    return resourceSlot;
  }

  public String getTaskId() {
    return taskId;
  }

  public String getExecutorName() {
    return executorName;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PreemptionVictim)) {
      return false;
    }

    PreemptionVictim other = (PreemptionVictim) o;
    return Objects.equals(getSlaveHost(), other.getSlaveHost())
        && Objects.equals(isProduction(), other.isProduction())
        && Objects.equals(getRole(), other.getRole())
        && Objects.equals(getPriority(), other.getPriority())
        && Objects.equals(getResourceSlot(), other.getResourceSlot())
        && Objects.equals(getTaskId(), other.getTaskId())
        && Objects.equals(getExecutorName(), other.getExecutorName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(slaveHost, production, role, priority, resourceSlot, taskId, executorName);
  }

  @Override
  public String toString() {
    return com.google.common.base.Objects.toStringHelper(this)
        .add("slaveHost", getSlaveHost())
        .add("production", isProduction())
        .add("role", getRole())
        .add("priority", getPriority())
        .add("resourceSlot", getResourceSlot())
        .add("taskId", getTaskId())
        .add("executorName", getExecutorName())
        .toString();
  }
}
