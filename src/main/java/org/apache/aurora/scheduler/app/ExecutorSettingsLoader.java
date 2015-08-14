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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;

import com.google.gson.reflect.TypeToken;

import org.apache.aurora.scheduler.mesos.ExecutorSettings;

/**
 * Executor configuration file loader.
 */
public final class ExecutorSettingsLoader {
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
   * Executor settings map loader to be called whenever Map needs to be generated from
   * the JSON config file.
   */
  public static ImmutableSet<ExecutorSettings> load(String configFilePath)
      throws ExecutorSettingsConfigException {

    Set<ExecutorSettings> executors = new HashSet<ExecutorSettings>();

    try {
      Reader fileReader = new InputStreamReader(new FileInputStream(configFilePath), "UTF8");
      Gson gson = new GsonBuilder().setPrettyPrinting().create();
      Type type = new TypeToken<ArrayList<ExecutorConfiguration>>() { } .getType();
      List<ExecutorConfiguration> executorConfigs = gson.fromJson(fileReader, type);

      for (ExecutorConfiguration config: executorConfigs) {
        executors.add(config.toExecutorSettings());
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

    return ImmutableSet.<ExecutorSettings>copyOf(executors);
  }
}
