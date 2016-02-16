package mapreduce.engine.multithreading;

import java.util.concurrent.FutureTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.executors.JobCalculationExecutor;

public class AbortableJobExecutorTask<T> extends FutureTask<T> {
	private static Logger logger = LoggerFactory.getLogger(AbortableJobExecutorTask.class);

	private JobCalculationExecutor executor;

	public AbortableJobExecutorTask(Runnable runnable, T result) {
		super(runnable, result);
		logger.info("runnable: " + runnable);
		this.executor = (JobCalculationExecutor) runnable;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		logger.info("cancel called");
		boolean cancel = super.cancel(mayInterruptIfRunning); 
		executor.abortExecution();
		return cancel;
	}
}
