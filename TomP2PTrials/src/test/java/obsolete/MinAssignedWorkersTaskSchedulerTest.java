package obsolete;

public class MinAssignedWorkersTaskSchedulerTest {

//	private static MinAssignedWorkersTaskExecutionScheduler taskScheduler;
//
//	@Before
//	public void setUpBeforeTest() throws Exception {
//		taskScheduler = MinAssignedWorkersTaskExecutionScheduler.create();
//
//	}
//
//	@Test
//	public void testScheduledTasks() {
//
//		taskScheduler.randomizeFirstTask(false);
//		Task task1 = taskScheduler.schedule(MinAssignedWorkersComparatorTest.createTestTasks());
//		assertEquals(task1.key(), ("test"));
//	}
//
//	@Test
//	public void testFinishedTasks() {
//
//		taskScheduler.randomizeFirstTask(false);
//		Task task1 = taskScheduler.schedule(createAllFinishedTasks());
//		assertEquals(task1, null);
//		Task task2 = taskScheduler.schedule(createOneNotFinishedTasks());
//		assertEquals(task2.key(), "NOTFINISHED");
//	}
//
//	private List<Task> createAllFinishedTasks() {
//		List<Task> tasks = new ArrayList<>();
//		// Not finished, not active, 1 executors
//		tasks.add(Task.create("this")
//				.addOutputDomain(ExecutorTaskDomain.create("this", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//		tasks.add(Task.create("is")
//				.addOutputDomain(ExecutorTaskDomain.create("is", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//		tasks.add(Task.create("a")
//				.addOutputDomain(ExecutorTaskDomain.create("a", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//		tasks.add(Task.create("hello")
//				.addOutputDomain(ExecutorTaskDomain.create("hello", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//		tasks.add(Task.create("world")
//				.addOutputDomain(ExecutorTaskDomain.create("world", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//		tasks.add(Task.create("test")
//				.addOutputDomain(ExecutorTaskDomain.create("test", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//		return tasks;
//	}
//
//	private List<Task> createOneNotFinishedTasks() {
//		List<Task> tasks = new ArrayList<>();
//		// Not finished, not active, 1 executors
//		tasks.add(Task.create("this")
//				.addOutputDomain(ExecutorTaskDomain.create("this", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//		tasks.add(Task.create("is")
//				.addOutputDomain(ExecutorTaskDomain.create("is", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//		tasks.add(Task.create("a")
//				.addOutputDomain(ExecutorTaskDomain.create("a", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//		tasks.add(Task.create("hello")
//				.addOutputDomain(ExecutorTaskDomain.create("hello", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//		tasks.add(Task.create("world")
//				.addOutputDomain(ExecutorTaskDomain.create("world", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//		tasks.add(Task.create("test")
//				.addOutputDomain(ExecutorTaskDomain.create("test", "Executor1", 0, JobProcedureDomain.create("test", "Executor1", "Null", 0))));
//		tasks.add(Task.create("NOTFINISHED"));
//		return tasks;
//	}
//
//	@Test
//	public void testRandomised() {
//		taskScheduler.randomizeFirstTask(true);
//
//		/*
//		 * Let's count some occurrences... If it is uniformly distributed, the larger the iterations, the more the proportions should come to a ratio
//		 * of 1/#tasks
//		 */
//
//		Map<Task, Double> taskOccurrenceCounter = new HashMap<Task, Double>();
//		List<Task> tasks = createEmptyTasks();
//		for (Task task : tasks) {
//			taskOccurrenceCounter.put(task, 0.0);
//		}
//		int numberOfIterations = 10000;
//		for (int i = 0; i < numberOfIterations; ++i) {
//			Task task = taskScheduler.schedule(tasks);
//			Double taskCounter = taskOccurrenceCounter.get(task);
//			++taskCounter;
//			taskOccurrenceCounter.put(task, taskCounter);
//		}
//
//		for (Task task : taskOccurrenceCounter.keySet()) {
//			// System.err.println((1.0 / ((double) tasks.size())) + "," + (((double) taskOccurrenceCounter.get(task)) / ((double)
//			// numberOfIterations)));
//			assertEquals((1.0 / ((double) tasks.size())), (((double) taskOccurrenceCounter.get(task)) / ((double) numberOfIterations)), 0.01);
//		}
//		taskScheduler.randomizeFirstTask(false);
//	}
//
//	private List<Task> createEmptyTasks() {
//		List<Task> tasks = new ArrayList<>();
//		// Not finished, not active, 1 executors
//		tasks.add(Task.create("this"));
//		tasks.add(Task.create("is"));
//		tasks.add(Task.create("a"));
//		tasks.add(Task.create("hello"));
//		tasks.add(Task.create("world"));
//		tasks.add(Task.create("test"));
//		return tasks;
//	}

}
