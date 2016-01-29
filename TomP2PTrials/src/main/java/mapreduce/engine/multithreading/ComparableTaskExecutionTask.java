package mapreduce.engine.multithreading;

import java.util.concurrent.FutureTask;

import mapreduce.execution.tasks.Task;

public class ComparableTaskExecutionTask<T> extends FutureTask<T> implements Comparable<ComparableTaskExecutionTask<T>> {
	private volatile Task task;

	public ComparableTaskExecutionTask(Runnable runnable, T result, Task task) {
		super(runnable, result);
		this.task = task;
	}

	@Override
	public int compareTo(ComparableTaskExecutionTask<T> o) {
		int result = 0;

		if (!task.isFinished() && !o.task.isFinished()) {
			if (task.canBeExecuted() && o.task.canBeExecuted()) {
				if (task.currentMaxNrOfSameResultHash() == o.task.currentMaxNrOfSameResultHash()) {
					if (task.activeCount() > o.task.activeCount()) {
						return 1;
					} else if (task.activeCount() < o.task.activeCount()) {
						return -1;
					} else {
						return 0;
					}
				} else if (task.currentMaxNrOfSameResultHash() < o.task.currentMaxNrOfSameResultHash()) {
					return -1;
				} else {// if (task.currentMaxNrOfSameResultHash() > o.task.currentMaxNrOfSameResultHash()) {
					return 1;
				}

			} else if (!task.canBeExecuted() && o.task.canBeExecuted()) {
				return 1;
			} else if (task.canBeExecuted() && !o.task.canBeExecuted()) {
				return -1;
			} else {
				return 0;
			}
		} else if (task.isFinished() && !o.task.isFinished()) {
			result = 1;
		} else if (!task.isFinished() && o.task.isFinished()) {
			result = -1;
		} else {
			result = 0;
		}
		return result;
	}
}

// public ComparableFutureTask(Callable<T> callable, int priority) {
// super(callable);
// }