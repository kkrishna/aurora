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

import java.util.Map;

import org.apache.aurora.scheduler.mesos.ExecutorSettings;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class ExecutorSettingsLoaderTest {
  private static final String EXAMPLE_RESOURCE = "executor-settings-example.json";
  private static final String NONEXISTENT_RESOURCE = "executor-settings-nonexistent.json";

  @Test
  public void parse() throws ExecutorSettingsLoader.ExecutorSettingsConfigException {
    Map<String, ExecutorSettings> test = ExecutorSettingsLoader.load(
        getClass().getResource(EXAMPLE_RESOURCE).getFile());

    assertFalse(test.isEmpty());
  }

  @Test(expected = ExecutorSettingsLoader.ExecutorSettingsConfigException.class)
  public void testNonExistentFile() throws ExecutorSettingsLoader.ExecutorSettingsConfigException {
    ExecutorSettingsLoader.load(NONEXISTENT_RESOURCE);
  }
}