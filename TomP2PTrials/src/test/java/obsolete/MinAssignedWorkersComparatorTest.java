package obsolete;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.tasks.Task;
import obsolete.taskexecutionscheduling.sortingcomparators.MinAssignedWorkerTaskExecutionSortingComparator;

public class MinAssignedWorkersComparatorTest {

//	@Test
//	public void testScheduledTasks() {
//
//		List<Task> tasks = createTestTasks();
//
//		Collections.shuffle(tasks);
//		Collections.sort(tasks, MinAssignedWorkerTaskExecutionSortingComparator.create());
//
//		assertEquals("test", tasks.get(0).key());
//		assertEquals("a", tasks.get(1).key());
//		assertEquals("is", tasks.get(2).key());
//		assertEquals("this", tasks.get(3).key());
//		assertEquals("hallo", tasks.get(4).key());
//		assertEquals("hello", tasks.get(5).key());
//
//	}
//
//	public static List<Task> createTestTasks() {
//		List<Task> tasks = new ArrayList<>();
//
//		// Finished, active, 3 executors
//		tasks.add(
//				Task.create("hello").nrOfSameResultHash(3)
//						.addOutputDomain(
//								ExecutorTaskDomain.create("hello", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0)))
//				.addOutputDomain(ExecutorTaskDomain.create("hello", "Executor1", 1, JobProcedureDomain.create("test", "Executor1", "Null", 0)))
//				.addOutputDomain(ExecutorTaskDomain.create("hello", "Executor1", 1, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//
//		// Finished, not active, 3 executors
//		tasks.add(
//				Task.create("hallo").nrOfSameResultHash(3)
//						.addOutputDomain(
//								ExecutorTaskDomain.create("hallo", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0)))
//				.addOutputDomain(ExecutorTaskDomain.create("hallo", "Executor1", 1, JobProcedureDomain.create("test", "Executor1", "Null", 0)))
//				.addOutputDomain(ExecutorTaskDomain.create("hallo", "Executor1", 1, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//
//		// Not finished, active, 2 executors
//		tasks.add(Task.create("this").nrOfSameResultHash(3).incrementActiveCount()
//				.addOutputDomain(ExecutorTaskDomain.create("this", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0)))
//				.addOutputDomain(ExecutorTaskDomain.create("this", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//
//		// Not finished, not active, 2 executors
//		tasks.add(Task.create("is").nrOfSameResultHash(3)
//				.addOutputDomain(ExecutorTaskDomain.create("is", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0)))
//				.addOutputDomain(ExecutorTaskDomain.create("is", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//
//		// Not finished, active, 1 executors
//		tasks.add(Task.create("a").nrOfSameResultHash(3).incrementActiveCount()
//				.addOutputDomain(ExecutorTaskDomain.create("a", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//
//		// Not finished, not active, 1 executors
//		tasks.add(Task.create("test").nrOfSameResultHash(3) 
//				.addOutputDomain(ExecutorTaskDomain.create("test", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//		return tasks;
//	}

}
