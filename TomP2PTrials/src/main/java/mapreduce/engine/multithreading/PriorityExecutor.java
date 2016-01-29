package mapreduce.engine.multithreading;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import mapreduce.engine.broadcasting.messages.BCMessageStatus;
import mapreduce.execution.jobs.PriorityLevel;
import mapreduce.execution.tasks.Task;

/**
 * Taken from http://stackoverflow.com/questions/16833951/testing-priorityblockingqueue-in-threadpoolexecutor
 * 
 * @author Oliver
 *
 */
public class PriorityExecutor extends ThreadPoolExecutor {

	public PriorityExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
	}

	public static PriorityExecutor newFixedThreadPool(int nThreads) {
		return new PriorityExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
				new PriorityBlockingQueue<Runnable>());
	}

	public Future<?> submit(Runnable runnable, PriorityLevel jobPriority, Long jobCreationTime,
			Integer procedureIndex, BCMessageStatus messageStatus, Long messageCreationTime) {
		return super.submit(new ComparableBCMessageTask<>(runnable, null, jobPriority, jobCreationTime,
				procedureIndex, messageStatus, messageCreationTime));
	}

	public Future<?> submit(Runnable runnable, Task task) {
		return super.submit(new ComparableTaskExecutionTask<>(runnable, null, task));
	}

	@Override
	protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
		return (RunnableFuture<T>) callable;
	}

	@Override
	protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
		return (RunnableFuture<T>) runnable;
	}

}