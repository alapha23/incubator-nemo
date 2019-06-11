package org.apache.nemo.runtime.executor.burstypolicy;

import org.apache.nemo.runtime.executor.common.TaskExecutor;

import java.util.List;

public final class StatelessTaskStatInfo {
  public final int running;
  public final int offload_pending;
  public final int offloaded;
  public final int deoffloaded;
  public final int totalStateless;
  public final List<TaskExecutor> runningTasks;
  public final List<TaskExecutor> statelessRunningTasks;
  public final List<TaskExecutor> statefulRunningTasks;


  public StatelessTaskStatInfo(
    final int running, final int offload_pending, final int offloaded, final int deoffloaded,
    final int totalStateless,
    final List<TaskExecutor> runningTasks,
    final List<TaskExecutor> statelessRunningTasks,
    final List<TaskExecutor> statefulRunningTasks) {
    this.running = running;
    this.offload_pending = offload_pending;
    this.offloaded = offloaded;
    this.deoffloaded = deoffloaded;
    this.totalStateless = totalStateless;
    this.runningTasks = runningTasks;
    this.statelessRunningTasks = statelessRunningTasks;
    this.statefulRunningTasks = statefulRunningTasks;
  }

  public List<TaskExecutor> getRunningStatelessTasks() {
    return runningTasks;
  }
}