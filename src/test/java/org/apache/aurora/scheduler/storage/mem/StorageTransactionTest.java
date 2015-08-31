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
package org.apache.aurora.scheduler.storage.mem;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.TearDownTestCase;
import org.apache.aurora.common.util.concurrent.ExecutorServiceShutdown;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.db.DbUtil;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * TODO(William Farner): Wire a mechanism to allow verification of synchronized writers.
 * TODO(wfarner): Merge this with DbStorageTest.
 */
public class StorageTransactionTest extends TearDownTestCase {

  private ExecutorService executor;
  private Storage storage;

  @Before
  public void setUp() {
    executor = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat("SlowRead-%d").setDaemon(true).build());
    addTearDown(new TearDown() {
      @Override
      public void tearDown() {
        new ExecutorServiceShutdown(executor, Amount.of(1L, Time.SECONDS)).execute();
      }
    });
    storage = DbUtil.createStorage();
  }

  @Test
  public void testConcurrentReaders() throws Exception {
    // Validate that a slow read does not block another read.

    final CountDownLatch slowReadStarted = new CountDownLatch(1);
    final CountDownLatch slowReadFinished = new CountDownLatch(1);

    Future<String> future = executor.submit(new Callable<String>() {
      @Override
      public String call() throws Exception {
        return storage.read(new Work.Quiet<String>() {
          @Override
          public String apply(StoreProvider storeProvider) {
            slowReadStarted.countDown();
            try {
              slowReadFinished.await();
            } catch (InterruptedException e) {
              fail(e.getMessage());
            }
            return "slowResult";
          }
        });
      }
    });

    slowReadStarted.await();

    String fastResult = storage.read(new Work.Quiet<String>() {
      @Override
      public String apply(StoreProvider storeProvider) {
        return "fastResult";
      }
    });
    assertEquals("fastResult", fastResult);
    slowReadFinished.countDown();
    assertEquals("slowResult", future.get());
  }

  private IScheduledTask makeTask(String taskId) {
    return TaskTestUtil.makeTask(taskId, TaskTestUtil.JOB);
  }

  private static class CustomException extends RuntimeException {
  }

  private <T, E extends RuntimeException> void expectWriteFail(MutateWork<T, E> work) {
    try {
      storage.write(work);
      fail("Expected a CustomException.");
    } catch (CustomException e) {
      // Expected.
    }
  }

  private void expectTasks(final String... taskIds) {
    storage.read(new Work.Quiet<Void>() {
      @Override
      public Void apply(StoreProvider storeProvider) {
        Query.Builder query = Query.unscoped();
        Set<String> ids = FluentIterable.from(storeProvider.getTaskStore().fetchTasks(query))
            .transform(Tasks::id)
            .toSet();
        assertEquals(ImmutableSet.<String>builder().add(taskIds).build(), ids);
        return null;
      }
    });
  }

  @Test
  public void testWritesUnderTransaction() {
    final IResourceAggregate quota = IResourceAggregate
            .build(new ResourceAggregate().setDiskMb(100).setNumCpus(2.0).setRamMb(512));

    try {
      storage.write(new MutateWork.NoResult.Quiet() {
        @Override
        public void execute(MutableStoreProvider storeProvider) {
          storeProvider.getQuotaStore().saveQuota("a", quota);
          throw new CustomException();
        }
      });
      fail("Expected CustomException to be thrown.");
    } catch (CustomException e) {
      // Expected
    }

    storage.read(new Work.Quiet<Void>() {
      @Override
      public Void apply(StoreProvider storeProvider) {
        // If the previous write was under a transaction then there would be no quota records.
        assertEquals(ImmutableMap.of(),
            storeProvider.getQuotaStore().fetchQuotas());
        return null;
      }
    });
  }

  @Test
  public void testOperations() {
    expectWriteFail(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("a"), makeTask("b")));
        throw new CustomException();
      }
    });
    expectTasks();

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("a"), makeTask("b")));
      }
    });
    expectTasks("a", "b");

    expectWriteFail(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().deleteAllTasks();
        throw new CustomException();
      }
    });
    expectTasks("a", "b");

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().deleteAllTasks();
      }
    });

    expectWriteFail(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("a")));
        throw new CustomException();
      }
    });
    expectTasks();

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("a")));
      }
    });

    // Nested transaction where inner transaction fails.
    expectWriteFail(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("c")));
        storage.write(new MutateWork.NoResult.Quiet() {
          @Override
          public void execute(MutableStoreProvider storeProvider) {
            storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("d")));
            throw new CustomException();
          }
        });
      }
    });
    expectTasks("a");

    // Nested transaction where outer transaction fails.
    expectWriteFail(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("c")));
        storage.write(new MutateWork.NoResult.Quiet() {
          @Override
          public void execute(MutableStoreProvider storeProvider) {
            storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("d")));
          }
        });
        throw new CustomException();
      }
    });
    expectTasks("a");
  }
}
