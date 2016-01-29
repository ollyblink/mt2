package obsolete.taskexecutionscheduling;

import java.util.List;

import mapreduce.execution.tasks.Task;
import obsolete.ITaskScheduler;

public abstract class AbstractTaskExecutionScheduler implements ITaskScheduler {

	@Override
	public Task schedule(List<Task> tasksToSchedule) {
		if (tasksToSchedule != null) {
			return scheduleNonNull(tasksToSchedule);
		}
		return null;
	}

	protected abstract Task scheduleNonNull(List<Task> tasksToSchedule);

	protected boolean allTasksAreFinished(List<Task> tasksToSchedule) {
		synchronized (tasksToSchedule) {
			for (Task task : tasksToSchedule) {
				if (!task.isFinished()) {
					return false;
				}
			}
		}
		return true;
	}

	protected boolean noTaskAssignedYet(List<Task> tasksToSchedule) {
		synchronized (tasksToSchedule) {
			for (Task task : tasksToSchedule) {
				if (task != null && task.nrOfOutputDomains() > 0) {
					return false;
				}
			}
		}
		return true;
	}
}