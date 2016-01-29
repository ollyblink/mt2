package obsolete;

import java.util.List;

import mapreduce.execution.tasks.Task;

public interface ITaskScheduler {

	/**
	 * Schedules the next task to execute according to a defined scheduling algorithm
	 * 
	 * @param tasksToSchedule
	 * @return
	 */
	public Task schedule(List<Task> tasksToSchedule);

}
