package com.flipkart.fdp.superbi.execution;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultRetryTaskHandler implements RetryTaskHandler {

  private final PriorityQueue<BackgroundRefreshTask> priorityTaskQueue;
  private static int MAX_QUEUE_SIZE = 10000;

  public DefaultRetryTaskHandler() {
    this.priorityTaskQueue = new PriorityQueue<>(new Comparator<BackgroundRefreshTask>() {
      //Compare based on executionAfterTimestamp, least will be retried first.
      @Override
      public int compare(BackgroundRefreshTask o1, BackgroundRefreshTask o2) {
        return Long.compare(o1.getExecutionAfterTimestamp(), o2.getExecutionAfterTimestamp());
      }
    });
  }

  @Override
  public void submitForRetry(BackgroundRefreshTask backgroundRefreshTask) {
    if (priorityTaskQueue.size() < MAX_QUEUE_SIZE) {
      this.priorityTaskQueue.add(backgroundRefreshTask);
    } else {
      log.warn("Priority queue is at its full capacity {}, not queuing for retry: {}",
          MAX_QUEUE_SIZE, backgroundRefreshTask);
    }
  }

  public void init() {
    ExecutorService retryTaskExecutor = Executors.newSingleThreadExecutor();
    retryTaskExecutor.submit(new RetryTaskHandlerThread());
  }

  private class RetryTaskHandlerThread implements Runnable {

    private Boolean isInterrupted = false;

    @Override
    public void run() {
      while (!isInterrupted) {
        try {
          //Sleep for 5 seconds
          Thread.sleep(5000);

          log.debug("RetryTaskHandlerThread woke up. Peeking into priorityQueue...");
          while (priorityTaskQueue.peek() != null &&
              priorityTaskQueue.peek().getExecutionAfterTimestamp() <= System.currentTimeMillis()) {
            BackgroundRefreshTask retryTask = priorityTaskQueue.poll();

            //TODO:: This will try to give it to Hystrix and error callback will be called with RejectedExecutionException.
            // Which is not the right thing, as the queue will be drained immediately.
            // Not worrying to fix this right now, as BR V2 will anyways solve this.
            retryTask.executeAsync();
          }
        } catch (InterruptedException i) {
          log.debug("RetryTaskHandlerThread Interrupted! Exiting loop.");
          isInterrupted = true;
        } catch (Exception ignore) {
          log.error(
              "NOT CRITICAL: DefaultRetryTaskHandler Thread received an exception, ignoring it and trying next.",
              ignore);
        }
      }
    }
  }
}