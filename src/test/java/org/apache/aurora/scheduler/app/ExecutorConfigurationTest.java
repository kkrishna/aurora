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
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import org.apache.aurora.scheduler.mesos.ExecutorSettings;
import org.junit.Test;

import static org.apache.aurora.scheduler.mesos.TaskExecutors.FAKE_MESOS_COMMAND_EXECUTOR;
import static org.junit.Assert.assertEquals;

public class ExecutorConfigurationTest {
  private static final String MESOS_COMMAND_EXAMPLE_RESOURCE
      = "executor-settings-mesos-command-example.json";

  @Test
  public void toExecutorSettingsTest()
      throws FileNotFoundException, UnsupportedEncodingException {

    Reader fileReader = new InputStreamReader(
        new FileInputStream(getClass().getResource(MESOS_COMMAND_EXAMPLE_RESOURCE).getFile())
        , "UTF8");
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    Type type = new TypeToken<ArrayList<ExecutorConfiguration>>() { } .getType();
    List<ExecutorConfiguration> lst = gson.fromJson(fileReader, type);

    ExecutorSettings executorSettings = lst.get(0).toExecutorSettings();

    assertEquals(executorSettings, FAKE_MESOS_COMMAND_EXECUTOR);

  }
}
