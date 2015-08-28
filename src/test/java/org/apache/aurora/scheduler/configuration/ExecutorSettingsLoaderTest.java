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

import org.apache.aurora.scheduler.mesos.ExecutorSettings;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class ExecutorSettingsLoaderTest {
  private static final String THERMOS_EXAMPLE_RESOURCE = "thermos-settings-example.json";
  private static final String  THERMOS_NO_OBSERVER_RESOURCE
      = "executor-settings-thermos-no-observer.json";
  private static final String NO_VALUE_URI = "no-value-URI.json";
  private static final String NONEXISTENT_RESOURCE = "executor-settings-nonexistent.json";

  @Test
  public void parse() throws ExecutorSettingsLoader.ExecutorSettingsConfigException {
    ExecutorSettings test = ExecutorSettingsLoader.load(
        new File(getClass().getResource(THERMOS_EXAMPLE_RESOURCE).getFile()));

    assertNotNull(test);
  }

  @Test(expected = NullPointerException.class)
  public void testThermosNoObserver()
      throws ExecutorSettingsLoader.ExecutorSettingsConfigException {
    ExecutorSettingsLoader.load(
        new File(getClass().getResource(THERMOS_NO_OBSERVER_RESOURCE).getFile()));
  }

  @Test(expected = NullPointerException.class)
  public void testThermosNoValueURI()
      throws ExecutorSettingsLoader.ExecutorSettingsConfigException {
    ExecutorSettingsLoader.load(
        new File(getClass().getResource(NO_VALUE_URI).getFile()));
  }

  @Test(expected = ExecutorSettingsLoader.ExecutorSettingsConfigException.class)
  public void testNonExistentFile() throws ExecutorSettingsLoader.ExecutorSettingsConfigException {
    ExecutorSettingsLoader.load(new File(NONEXISTENT_RESOURCE));
  }
}
