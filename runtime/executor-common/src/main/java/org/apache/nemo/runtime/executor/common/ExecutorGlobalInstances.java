package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class ExecutorGlobalInstances implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorGlobalInstances.class.getName());

  private final ScheduledExecutorService watermarkTriggerService;
  private static final long WATERMARK_PERIOD = 250; // ms
  private final List<Pair<SourceVertex, Runnable>> watermarkServices;


  public ExecutorGlobalInstances() {
    this.watermarkTriggerService = Executors.newScheduledThreadPool(3);
    this.watermarkServices = new ArrayList<>();

    this.watermarkTriggerService.scheduleAtFixedRate(() -> {
      synchronized (watermarkServices) {
        //LOG.info("Trigger watermarks:{}", watermarkServices.size());
        watermarkServices.forEach(pair -> pair.right().run());
      }
    }, WATERMARK_PERIOD, WATERMARK_PERIOD, TimeUnit.MILLISECONDS);
  }

  public void registerWatermarkService(final SourceVertex sv, final Runnable runnable) {
    synchronized (watermarkServices) {
      //LOG.info("Register {}: ", sv);
      watermarkServices.add(Pair.of(sv, runnable));
    }
  }

  public void deregisterWatermarkService(final SourceVertex taskId) {
    synchronized (watermarkServices) {
      final Iterator<Pair<SourceVertex, Runnable>> iterator = watermarkServices.iterator();
      while (iterator.hasNext()) {
        final Pair<SourceVertex, Runnable> pair = iterator.next();
        if (pair.left().equals(taskId)) {
          iterator.remove();
          return;
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    watermarkTriggerService.shutdown();
  }
}
