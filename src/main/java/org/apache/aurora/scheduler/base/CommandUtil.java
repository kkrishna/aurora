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
   * Gets the last part of the path of a URI.
   *
   * @param uri URI to parse
   * @return The last segment of the URI.
   */
  public static String uriBasename(String uri) {
    int lastSlash = uri.lastIndexOf('/');
    if (lastSlash == -1) {
      return uri;
    } else {
      String basename = uri.substring(lastSlash + 1);
      MorePreconditions.checkNotBlank(basename, "URI must not end with a slash.");

      return basename;
    }
  }

  public static void backSlashTest(String uri) {
    int lastSlash = uri.lastIndexOf('/');
    if (lastSlash == -1) {
      return;
    } else {
      String basename = uri.substring(lastSlash + 1);
      MorePreconditions.checkNotBlank(basename, "URI must not end with a slash.");
    }
  }

  /**
   *
   */

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

    // TODO(rdelvalle): Determine if this is really necessary or if it's worth getting rid of
    // along with the test for it
    backSlashTest(executorCommand.get(0));

    for (URI uri : executorResources) {
      backSlashTest(uri.getValue());
    }

    String cmdLine = String.join(" ", executorCommand);
    Preconditions.checkNotNull(executorResources);
    MorePreconditions.checkNotBlank(cmdLine);
    MorePreconditions.checkNotBlank(commandBasePath);
    CommandInfo.Builder builder = CommandInfo.newBuilder();

    builder.addAllUris(executorResources);

    cmdLine = commandBasePath + cmdLine;
    System.out.println("MARKERMAKER: " + cmdLine.trim());
    return builder.setValue(cmdLine.trim());
  }
}
