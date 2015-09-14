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
   * @param executorResources A list of URIs to be fetched into the sandbox with the executor.
   * @return A populated CommandInfo with correct resources set and command set.
   */
  public static CommandInfo create(List<String> executorUri, List<URI> executorResources) {
    return create(
        executorUri,
        executorResources,
        "./").build();
  }

  /**
   * Creates a description of a command that will fetch and execute the given URI to an executor
   * binary.
   *
   * @param executorCommand A list of strings that form the command to be executed and it's
   *                        arguments.
   * @param executorResources A list of URIs to be fetched into the sandbox with the executor.
   * @param commandBasePath The relative base path of the executor.
   * @return A CommandInfo.Builder populated with resources and a command.
   */
  public static CommandInfo.Builder create(
      List<String> executorCommand,
      List<URI> executorResources,
      String commandBasePath) {

    Preconditions.checkNotNull(executorResources);
    MorePreconditions.checkNotBlank(commandBasePath);
    CommandInfo.Builder builder = CommandInfo.newBuilder();



    //TODO(rdelvalle): Determine if commandBasePath is needed
    builder.setShell(false)
        .addAllUris(executorResources)
        .setValue(commandBasePath + executorCommand.get(0))
        .addAllArguments(executorCommand);

    return builder;
  }

  public static CommandInfo.Builder dockerCreate(
      List<String> executorCommand,
      List<URI> executorResources,
      String commandBasePath) {


    //TODO(rdelvalle): Determine if there is a way to collapse this to a single create
    String cmd = String.join(" ", executorCommand);
    Preconditions.checkNotNull(executorResources);
    MorePreconditions.checkNotBlank(commandBasePath);
    MorePreconditions.checkNotBlank(cmd);
    CommandInfo.Builder builder = CommandInfo.newBuilder();

    cmd = commandBasePath + cmd;
    return builder.setValue(cmd.trim());
  }
}
