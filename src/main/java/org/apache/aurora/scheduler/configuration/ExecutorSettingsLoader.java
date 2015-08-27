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
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

import com.google.common.base.Optional;
import com.google.common.io.CharSource;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.gen.Volume;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.app.VolumeParser;
import org.apache.aurora.scheduler.mesos.ExecutorSettings;

public final class ExecutorSettingsLoader {
  private ExecutorSettingsLoader()  {
    // Utility class
  }

  public static class ExecutorSettingsConfigException extends Exception {
    public ExecutorSettingsConfigException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  public static ExecutorSettings load(File configFile)
      throws ExecutorSettingsConfigException {

    CharSource configFileSource = Files.asCharSource(configFile, StandardCharsets.UTF_8);
    ExecutorSettings executorSettings;

    try (Reader reader = configFileSource.openBufferedStream()) {
      Gson gson = new GsonBuilder()
          .registerTypeAdapter(ResourceSlot.class, new ResourceSlotDeserializer())
          .registerTypeAdapter(Optional.class, new FlagsDeserializer())
          .registerTypeAdapter(Volume.class, new VolumeDeserializer()).create();

      executorSettings = gson.fromJson(reader, ExecutorSettings.class);

    } catch (JsonParseException e) {
      throw new ExecutorSettingsConfigException("Error parsing JSON config\n" + e, e);
    } catch (IOException e) {
      throw new ExecutorSettingsConfigException("IO Error\n" + e, e);
    }

    //GSON bypasses constraint checks by using reflection, build new object to enforce constraints
    return ExecutorSettings.newBuilder()
        .setExecutorName(executorSettings.getExecutorName())
        .setExecutorCommand(executorSettings.getExecutorCommand())
        .setExecutorResources(executorSettings.getExecutorResources())
        .setThermosObserverRoot(executorSettings.getThermosObserverRoot())
        .setExecutorOverhead(executorSettings.getExecutorOverhead())
        .setGlobalContainerMounts(executorSettings.getGlobalContainerMounts()).build();
  }

  static class ResourceSlotDeserializer implements JsonDeserializer<ResourceSlot> {

    @Override
    public ResourceSlot deserialize(
        JsonElement json,
        Type typeOfT,
        JsonDeserializationContext context)
        throws JsonParseException {

      JsonObject jsonObj = (JsonObject) json;

      return new ResourceSlot(jsonObj.get("numCpus").getAsDouble(),
          Amount.of(jsonObj.get("ramMB").getAsLong(), Data.MB),
          Amount.of(jsonObj.get("diskMB").getAsLong(), Data.MB),
          jsonObj.get("numPorts").getAsInt());
    }
  }

  static class FlagsDeserializer implements JsonDeserializer<Optional<String>> {

    @Override
    public Optional<String> deserialize(
        JsonElement json,
        Type typeOfT,
        JsonDeserializationContext context)
        throws JsonParseException {

      return Optional.<String>of(json.getAsString());
    }
  }

  static class VolumeDeserializer implements JsonDeserializer<Volume> {

    @Override
    public Volume deserialize(
        JsonElement json,
        Type typeOfT,
        JsonDeserializationContext context)
        throws JsonParseException {
      VolumeParser volParser = new VolumeParser();
      try {
        return volParser.doParse(json.getAsString());
      } catch (IllegalArgumentException e) {
        throw new JsonParseException("Failed to parse mount \"" + json.getAsString() + "\"", e);
      }
    }
  }
}
