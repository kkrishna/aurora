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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharSource;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.gen.Volume;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.app.VolumeParser;
import org.apache.aurora.scheduler.mesos.ExecutorSettings;
import org.apache.mesos.Protos.CommandInfo.URI;

public final class ExecutorSettingsLoader {
  private static String AURORA_EXECUTOR = "AuroraExecutor";

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

    CharSource configFileSource = Files.asCharSource(configFile, StandardCharsets.UTF_8);
    ImmutableMap<String, ExecutorSettings> executorSettings;

    try (Reader reader = configFileSource.openBufferedStream()) {
      Gson gson = new GsonBuilder()
          .registerTypeAdapter(Map.class, new ExecutorsMapDeserializer())
          .registerTypeAdapter(ResourceSlot.class, new ResourceSlotDeserializer())
          .registerTypeAdapter(Optional.class, new FlagsDeserializer())
          .registerTypeAdapter(Volume.class, new VolumeDeserializer())
          .registerTypeAdapter(URI.class, new URIDeserializer()).create();
      Type type = new TypeToken<Map<String, ExecutorSettings>>(){}.getType();

      executorSettings = gson.fromJson(reader, type);

    } catch (JsonParseException e) {
      throw new ExecutorSettingsConfigException("Error parsing JSON config\n" + e, e);
    } catch (IOException e) {
      throw new ExecutorSettingsConfigException("IO Error\n" + e, e);
    }


    //GSON bypasses constraint checks by using reflection, thus we build new object
    // to enforce constraints. Builder will also provide default values for empty fields
    return executorSettings;
  }

  static class ResourceSlotDeserializer implements JsonDeserializer<ResourceSlot> {

    @Override
    public ResourceSlot deserialize(
        JsonElement json,
        Type typeOfT,
        JsonDeserializationContext context)
        throws JsonParseException {

      JsonObject jsonObj = json.getAsJsonObject();

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

  static class URIDeserializer implements JsonDeserializer<URI> {

    @Override
    public URI deserialize(
        JsonElement json,
        Type typeOfT,
        JsonDeserializationContext context)
        throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();

      URI.Builder builder = URI.newBuilder().setValue(jsonObj.get("value").getAsString());

      //TODO(rdelvalle): Figure out if there's a better pattern for doing this
      if (jsonObj.has("executable")) {
        builder.setExecutable(jsonObj.get("executable").getAsBoolean());
      }

      if (jsonObj.has("extract")) {
        builder.setExtract(jsonObj.get("extract").getAsBoolean());
      }

      if (jsonObj.has("cache")) {
        builder.setCache(jsonObj.get("cache").getAsBoolean());
      }

      return builder.build();
    }
  }

  static class ExecutorsMapDeserializer implements JsonDeserializer<Map<String,ExecutorSettings>> {

    @Override
    public Map<String,ExecutorSettings> deserialize(
        JsonElement json,
        Type typeOfT,
        JsonDeserializationContext context)
        throws JsonParseException {


      JsonObject jsonObject = json.getAsJsonObject();

      Map<String, ExecutorSettings> settingsMap = new HashMap<String, ExecutorSettings>();

      for(Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {

        ExecutorSettings executorSettings = context.deserialize(entry.getValue(),
            ExecutorSettings.class);

        //TODO(rdelvalle): Change this in custom executors patch
        String thermosRootObserver = "";

        if(AURORA_EXECUTOR.equals(entry.getKey())) {
          thermosRootObserver = executorSettings.getConfig().getAsJsonObject()
              .get("thermosObserverRoot").getAsString();
        }

        settingsMap.put(entry.getKey(),
            ExecutorSettings.newBuilder()
                .setExecutorName(entry.getKey())
                .setExecutorCommand(executorSettings.getExecutorCommand())
                .setExecutorResources(executorSettings.getExecutorResources())
                .setExecutorOverhead(executorSettings.getExecutorOverhead())
                .setThermosObserverRoot(thermosRootObserver)
                .setGlobalContainerMounts(executorSettings.getGlobalContainerMounts())
                .setConfig(executorSettings.getConfig()).build());

      }

      return ImmutableMap.<String,ExecutorSettings>copyOf(settingsMap);
    }
  }

}
