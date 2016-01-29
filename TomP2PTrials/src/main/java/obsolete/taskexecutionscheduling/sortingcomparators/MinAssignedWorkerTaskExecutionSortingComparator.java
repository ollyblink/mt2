package obsolete.taskexecutionscheduling.sortingcomparators;

import java.util.Comparator;

import mapreduce.execution.tasks.Task;

public class MinAssignedWorkerTaskExecutionSortingComparator implements Comparator<Task> {

	@Override
	public int compare(Task t1, Task t2) {
		if (!t1.isFinished() && t2.isFinished()) {
			return -1;
		} else if (t1.isFinished() && !t2.isFinished()) {
			return 1;
		} else {// if (!(t1.isFinished() && !t2.isFinished() || (t1.isFinished() && t2.isFinished())) {
			if (t1.nrOfOutputDomains() > t2.nrOfOutputDomains()) {
				return -1;
			} else if (t1.nrOfOutputDomains() < t2.nrOfOutputDomains()) {
				return 1;
			} else {
				if (t1.activeCount() > t2.activeCount()) {
					return -1;
				} else if (t1.activeCount() < t2.activeCount()) {
					return 1;
				} else {
					return 0;
				}
			}
		}
	}

	public static MinAssignedWorkerTaskExecutionSortingComparator create() {
		return new MinAssignedWorkerTaskExecutionSortingComparator();
	}
}
