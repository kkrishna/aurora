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
package org.apache.aurora.scheduler.app;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;

import com.google.gson.reflect.TypeToken;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.aurora.gen.Volume;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.mesos.ExecutorSettings;

import static java.util.Objects.requireNonNull;

/**
 * Executor configuration file loader.
 */
public final class ExecutorSettingsLoader {
  private static final Logger LOG = Logger.getLogger(ExecutorSettingsLoader.class.getName());
  /**
   * No instances of this class should exist.
   */
  private ExecutorSettingsLoader()  {
  }

  /**
   * Exception class to cluster all errors that reading the config file
   * could generate into a general config file parsing error.
   */
  public static class ExecutorSettingsConfigException extends Exception {
    public ExecutorSettingsConfigException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Helper method to convert information from config file into a Resource object.
   */
  private static Resources parseOverhead(ExecutorConfiguration.ExecutorOverhead overhead) {

    return new Resources(overhead.getNumCpus(),
        Amount.of(overhead.getRamMB(), Data.MB),
        Amount.of(overhead.getDiskMB(), Data.MB),
        overhead.getNumPorts());
  }

  /**
   * Helper method to convert information from config file into a Volume list.
   */
  private static ImmutableList<Volume> globalContainerMountParser(
      Set<String> globalContainerMounts) {

    List<Volume> globalMountsList = new ArrayList<Volume>();
    VolumeParser volParser = new VolumeParser();

    for (String mount : globalContainerMounts) {
      try {
        globalMountsList.add(volParser.doParse(mount));
      } catch (IllegalArgumentException e) {
        LOG.warning("Illegal global container mount setting \"" + mount + "\" is being ignored");
      }
    }

    return ImmutableList.<Volume>copyOf(globalMountsList);
  }

  /**
   * Executor settings map loader to be called whenever Map needs to be generated from
   * the JSON config file.
   */
  public static ImmutableMap<String, ExecutorSettings> load(String configFilePath)
      throws ExecutorSettingsConfigException {

    Map<String, ExecutorSettings> executorSettings = new HashMap<String, ExecutorSettings>();
    try {
      Reader fileReader = new InputStreamReader(new FileInputStream(configFilePath), "UTF8");
      Gson gson = new GsonBuilder().setPrettyPrinting().create();
      Type type = new TypeToken<ArrayList<ExecutorConfiguration>>() { } .getType();
      List<ExecutorConfiguration> lst = gson.fromJson(fileReader, type);

      for (ExecutorConfiguration executorConfig : lst) {
        //TODO: Remove check when observer is axed
        if ("thermos".equals(executorConfig.getName())) {
          requireNonNull(executorConfig.getThermosObserverRoot());
        }

        executorSettings.put(executorConfig.getName(),
            ExecutorSettings.newBuilder()
                .setExecutorName(executorConfig.getName())
                .setExecutorPath(executorConfig.getPath())
                .setExecutorFlags(Optional.<String>fromNullable(executorConfig.getExecutorFlags()))
                .setGlobalContainerMounts(
                    globalContainerMountParser(executorConfig.getGlobalContainerMounts()))
                .setExecutorResources(ImmutableList.<String>copyOf(executorConfig.getResources()))
                .setThermosObserverRoot(executorConfig.getThermosObserverRoot())
                .setExecutorOverhead(parseOverhead(executorConfig.getOverhead()))
                .build());
      }

    } catch (FileNotFoundException e) {
      throw new ExecutorSettingsConfigException("Config file could not be found", e);
    } catch (UnsupportedEncodingException e) {
      throw new ExecutorSettingsConfigException("Config file needs to be in UTF8 format", e);
    } catch (JsonParseException e) {
      throw new ExecutorSettingsConfigException("Error parsing JSON", e);
    } catch (NullPointerException e) {
      throw new ExecutorSettingsConfigException("A required parameter is missing", e);
    }

    return ImmutableMap.<String, ExecutorSettings>copyOf(executorSettings);
  }
}
