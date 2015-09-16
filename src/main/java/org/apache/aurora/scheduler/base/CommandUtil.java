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

import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.aurora.common.base.MorePreconditions;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;

/**
 * Utility class for constructing {@link CommandInfo} objects given an executor URI.
 */
public final class CommandUtil {

  private CommandUtil() {
    // Utility class.
  }

  /**
   * Creates a description of a command that will fetch and execute the given URI to an executor
   * binary.
   *
   * @param executorUri A URI to the executor
   * @return A populated CommandInfo with correct resources set and command set.
   */
  public static CommandInfo create(CommandInfo.Builder executorUri) {
    return create(
        executorUri,
        "./").build();
  }

  /**
   * Creates a description of a command that will fetch and execute the given URI to an executor
   * binary.
   *
   * @param executorCommand CommandInfo builder from Json config file
   * @param commandBasePath The relative base path of the executor.
   * @return A CommandInfo.Builder populated with resources and a command.
   */
  public static CommandInfo.Builder create(
      CommandInfo.Builder executorCommand,
      String commandBasePath) {

    CommandInfo.Builder builder = CommandInfo.newBuilder();

    return builder.setShell(false);
  }

  public static CommandInfo.Builder dockerCreate(
      CommandInfo.Builder executorCommand,
      String commandBasePath) {


    //TODO(rdelvalle): Determine if there is a way to collapse this to a single create
    MorePreconditions.checkNotBlank(commandBasePath);
    CommandInfo.Builder builder = CommandInfo.newBuilder();

    String cmd = commandBasePath + executorCommand.getValue();
    return builder.setValue(cmd.trim());
  }
}
