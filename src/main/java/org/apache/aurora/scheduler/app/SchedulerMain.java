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

import java.io.File;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;

import org.apache.aurora.GuavaUtils.ServiceManagerIface;
import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.ArgScanner;
import org.apache.aurora.common.args.ArgScanner.ArgScanException;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.args.constraints.CanRead;
import org.apache.aurora.common.args.constraints.NotEmpty;
import org.apache.aurora.common.args.constraints.NotNull;
import org.apache.aurora.common.inject.Bindings;
import org.apache.aurora.common.logging.RootLogConfig;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.common.zookeeper.Group;
import org.apache.aurora.common.zookeeper.SingletonService;
import org.apache.aurora.common.zookeeper.SingletonService.LeadershipListener;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.gen.Volume;
import org.apache.aurora.scheduler.AppStartup;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.SchedulerLifecycle;
import org.apache.aurora.scheduler.configuration.ExecutorSettingsLoader;
import org.apache.aurora.scheduler.cron.quartz.CronModule;
import org.apache.aurora.scheduler.http.HttpService;
import org.apache.aurora.scheduler.log.mesos.MesosLogStreamModule;
import org.apache.aurora.scheduler.mesos.CommandLineDriverSettingsModule;
import org.apache.aurora.scheduler.mesos.ExecutorSettings;
import org.apache.aurora.scheduler.mesos.LibMesosLoadingModule;
import org.apache.aurora.scheduler.stats.StatsModule;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.backup.BackupModule;
import org.apache.aurora.scheduler.storage.db.DbModule;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.log.LogStorageModule;
import org.apache.aurora.scheduler.storage.log.SnapshotStoreImpl;
import org.apache.aurora.scheduler.zookeeper.guice.client.ZooKeeperClientModule;
import org.apache.aurora.scheduler.zookeeper.guice.client.ZooKeeperClientModule.ClientConfig;
import org.apache.aurora.scheduler.zookeeper.guice.client.flagged.FlaggedClientConfig;

import static org.apache.aurora.common.logging.RootLogConfig.Configuration;
import static org.apache.aurora.gen.apiConstants.THRIFT_API_VERSION;

/**
 * Launcher for the aurora scheduler.
 */
public class SchedulerMain {
  private static final Logger LOG = Logger.getLogger(SchedulerMain.class.getName());

  @NotNull
  @CmdLine(name = "cluster_name", help = "Name to identify the cluster being served.")
  private static final Arg<String> CLUSTER_NAME = Arg.create();

  @NotNull
  @NotEmpty
  @CmdLine(name = "serverset_path", help = "ZooKeeper ServerSet path to register at.")
  private static final Arg<String> SERVERSET_PATH = Arg.create();

  @CmdLine(name = "extra_modules",
      help = "A list of modules that provide additional functionality.")
  private static final Arg<List<Class<? extends Module>>> EXTRA_MODULES =
      Arg.create(ImmutableList.of());

  // TODO(Suman Karumuri): Rename viz_job_url_prefix to stats_job_url_prefix for consistency.
  @CmdLine(name = "viz_job_url_prefix", help = "URL prefix for job container stats.")
  private static final Arg<String> STATS_URL_PREFIX = Arg.create("");

  @CanRead
  @CmdLine(name = "executors_config_path", help = "Path to executor config JSON file")
  private static final Arg<File> EXECUTORS_CONFIG_PATH = Arg.create();

  @Inject private SingletonService schedulerService;
  @Inject private HttpService httpService;
  @Inject private SchedulerLifecycle schedulerLifecycle;
  @Inject private Lifecycle appLifecycle;
  @Inject
  @AppStartup
  private ServiceManagerIface startupServices;

  private void stop() {
    LOG.info("Stopping scheduler services.");
    try {
      startupServices.stopAsync().awaitStopped(5L, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      LOG.info("Shutdown did not complete in time: " + e);
    }
    appLifecycle.shutdown();
  }

  void run() {
    startupServices.startAsync();
    Runtime.getRuntime().addShutdownHook(new Thread(SchedulerMain.this::stop, "ShutdownHook"));
    startupServices.awaitHealthy();

    // Setup log4j to match our jul glog config in order to pick up zookeeper logging.
    Configuration logConfiguration = RootLogConfig.configurationFromFlags();
    logConfiguration.apply();
    Log4jConfigurator.configureConsole(logConfiguration);

    LeadershipListener leaderListener = schedulerLifecycle.prepare();

    HostAndPort httpAddress = httpService.getAddress();
    InetSocketAddress httpSocketAddress =
        InetSocketAddress.createUnresolved(httpAddress.getHostText(), httpAddress.getPort());
    try {
      schedulerService.lead(
          httpSocketAddress,
          ImmutableMap.of("http", httpSocketAddress),
          leaderListener);
    } catch (Group.WatchException e) {
      throw new IllegalStateException("Failed to watch group and lead service.", e);
    } catch (Group.JoinException e) {
      throw new IllegalStateException("Failed to join scheduler service group.", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted while joining scheduler service group.", e);
    }

    appLifecycle.awaitShutdown();
    stop();
  }

  @VisibleForTesting
  static Module getUniversalModule() {
    return Modules.combine(
        new LifecycleModule(),
        new StatsModule(),
        new AppModule(),
        new CronModule(),
        DbModule.productionModule(Bindings.annotatedKeyFactory(Storage.Volatile.class)),
        new DbModule.GarbageCollectorModule());
  }

  /**
   * Runs the scheduler by including modules configured from command line arguments in
   * addition to the provided environment-specific module.
   *
   * @param appEnvironmentModule Additional modules based on the execution environment.
   */
  @VisibleForTesting
  public static void flagConfiguredMain(Module appEnvironmentModule) {
    AtomicLong uncaughtExceptions = Stats.exportLong("uncaught_exceptions");
    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        uncaughtExceptions.incrementAndGet();
        LOG.log(Level.SEVERE, "Uncaught exception from " + t + ":" + e, e);
      }
    });

    ClientConfig zkClientConfig = FlaggedClientConfig.create();
    Module module = Modules.combine(
        appEnvironmentModule,
        getUniversalModule(),
        new ZooKeeperClientModule(zkClientConfig),
        new ServiceDiscoveryModule(SERVERSET_PATH.get(), zkClientConfig.credentials),
        new BackupModule(SnapshotStoreImpl.class),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(ExecutorSettings.class)
                .toInstance(ExecutorSettingsLoader.load(
                    EXECUTORS_CONFIG_PATH.get()).get("AuroraExecutor"));


            bind(IServerInfo.class).toInstance(
                IServerInfo.build(
                    new ServerInfo()
                        .setClusterName(CLUSTER_NAME.get())
                        .setThriftAPIVersion(THRIFT_API_VERSION)
                        .setStatsUrlPrefix(STATS_URL_PREFIX.get())));
          }
        });

    Lifecycle lifecycle = null;
    try {
      Injector injector = Guice.createInjector(module);
      lifecycle = injector.getInstance(Lifecycle.class);
      SchedulerMain scheduler = new SchedulerMain();
      injector.injectMembers(scheduler);
      try {
        scheduler.run();
      } finally {
        LOG.info("Application run() exited.");
      }
    } finally {
      if (lifecycle != null) {
        lifecycle.shutdown();
      }
    }
  }

  public static void main(String... args) {
    applyStaticArgumentValues(args);

    List<Module> modules = ImmutableList.<Module>builder()
        .add(
            new CommandLineDriverSettingsModule(),
            new LibMesosLoadingModule(),
            new MesosLogStreamModule(FlaggedClientConfig.create()),
            new LogStorageModule())
        .addAll(Iterables.transform(EXTRA_MODULES.get(), MoreModules::getModule))
        .build();
    flagConfiguredMain(Modules.combine(modules));
  }

  private static void exit(String message, Exception error) {
    LOG.log(Level.SEVERE, message + "\n" + error, error);
    System.exit(1);
  }

  /**
   * Applies {@link CmdLine} arg values throughout the classpath.  This must be invoked before
   * attempting to read any argument values in the system.
   *
   * @param args Command line arguments.
   */
  @VisibleForTesting
  public static void applyStaticArgumentValues(String... args) {
    try {
      if (!new ArgScanner().parse(Arrays.asList(args))) {
        System.exit(0);
      }
    } catch (ArgScanException e) {
      exit("Failed to scan arguments", e);
    } catch (IllegalArgumentException e) {
      exit("Failed to apply arguments", e);
    }
  }
}
