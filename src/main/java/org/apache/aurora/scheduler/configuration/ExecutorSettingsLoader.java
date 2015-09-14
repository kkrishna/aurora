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

package org.apache.aurora.scheduler.configuration;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.collect.Maps.EntryTransformer;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.gen.Volume;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.app.VolumeParser;
import org.apache.aurora.scheduler.mesos.ExecutorSettings;
import org.apache.mesos.Protos;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * Loads configuration file for executors from a JSON file
 * returns a map that can be used to dynamically choose executors
 */
public final class ExecutorSettingsLoader {
  private static final String AURORA_EXECUTOR = "AuroraExecutor";

  private ExecutorSettingsLoader()  {
    // Utility class
  }

  public static class ExecutorSettingsConfigException extends Exception {
    public ExecutorSettingsConfigException(String message, Throwable cause) {
      super(message, cause);
    }
  }


  public static ImmutableMap<String, ExecutorSettings> load(File configFile)
      throws ExecutorSettingsConfigException {

    Map<String, ExecutorSettings> executorSettings = ImmutableMap.of();

    try {
      String reader = Files.toString(configFile, StandardCharsets.UTF_8);

      Map<String, ExecutorConfiguration> configMap = new ObjectMapper().readValue(reader,
          new TypeReference<Map<String, ExecutorConfiguration>>() {
          });

      executorSettings = Maps.transformEntries(configMap,FROM_CONFIG);


    } catch (JsonParseException e) {
      throw Throwables.propagate(e);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    return ImmutableMap.<String, ExecutorSettings>copyOf(executorSettings);
  }
    /**
     * Helper method to convert information from config file into a Volume list. Uses
     * VolumeParser class to verify correctness.
     */
    private static ImmutableList<Volume> globalContainerMountParser(
            List<String> globalContainerMounts) {

        List<Volume> globalMountsList = new ArrayList<Volume>();
        VolumeParser volParser = new VolumeParser();

        for (String mount : globalContainerMounts) {
            try {
                globalMountsList.add(volParser.doParse(mount));
            } catch (IllegalArgumentException e) {
            }
        }

        return ImmutableList.<Volume>copyOf(globalMountsList);
    }


   private static ImmutableList<Protos.CommandInfo.URI> resourceParser(
           List<ExecutorConfiguration.Resource> resources) {
        List<Protos.CommandInfo.URI> URIList = new ArrayList<Protos.CommandInfo.URI>();

        for (ExecutorConfiguration.Resource res: resources) {

            Protos.CommandInfo.URI.Builder builder = Protos.CommandInfo.URI.newBuilder();
            builder.setValue(res.getValue());

            //TODO(rdelvalle): Improve on this logic
            if(res.getExecutable().isPresent()) {
                builder.setExecutable(res.getExecutable().get());
            }

            if(res.getExtract().isPresent()) {
                builder.setExecutable(res.getExtract().get());
            }

            if(res.getCache().isPresent()) {
                builder.setExecutable(res.getCache().get());
            }

            URIList.add(builder.build());
        }

        return ImmutableList.<Protos.CommandInfo.URI>copyOf(URIList);
    }


  private static final EntryTransformer<String, ExecutorConfiguration, ExecutorSettings> FROM_CONFIG =
          new EntryTransformer<String, ExecutorConfiguration, ExecutorSettings>() {
            @Override
            public ExecutorSettings transformEntry(String key, ExecutorConfiguration config) {

                return ExecutorSettings.newBuilder()
                        .setExecutorName(key)
                        .setExecutorCommand(config.getCommand())
                        .setGlobalContainerMounts(globalContainerMountParser(
                                        config.getGlobalContainerMounts()))
                        .setExecutorResources(resourceParser(config.getResources()))
                        .setThermosObserverRoot(config.getConfig().get("thermosObserverRoot"))
                        .setExecutorOverhead(
                                new ResourceSlot(config.getOverhead().getNumCpus(),
                                        Amount.of(config.getOverhead().getRamMB(), Data.MB),
                                        Amount.of(config.getOverhead().getDiskMB(), Data.MB),
                                        config.getOverhead().getNumPorts()))
                        .setConfig(config.getConfig())
                        .build();
            }
          };

}
