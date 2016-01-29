package mapreduce.execution.task;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Method;

import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.finishables.AbstractFinishable;
import mapreduce.execution.tasks.Task;
import net.tomp2p.peers.Number160;

public class TaskTest {

	@Test
	public void testTask() {
		String submitter = "S1";
		String executor1 = "E1";
		String executor2 = "E2";
		int trueResult = 100;
		int falseResult = 99;

		Task task = Task.create("hello", "E1").nrOfSameResultHash(1);
		ExecutorTaskDomain etd = ExecutorTaskDomain
				.create("hello", executor1, task.newStatusIndex(),
						JobProcedureDomain.create("job1", 0, submitter, "WordCount", 0))
				.resultHash(Number160.createHash(trueResult));
		task.addOutputDomain(etd);

		assertEquals(true, task.isFinished());
		assertEquals(etd, task.resultOutputDomain());

		task.nrOfSameResultHash(2);

		assertEquals(false, task.isFinished());
		assertEquals(null, task.resultOutputDomain());

		ExecutorTaskDomain etd2 = ExecutorTaskDomain
				.create("hello", executor1, task.newStatusIndex(),
						JobProcedureDomain.create("job1", 0, submitter, "WordCount", 0))
				.resultHash(Number160.createHash(trueResult));

		task.addOutputDomain(etd2);
		assertEquals(true, task.isFinished());
		assertEquals(etd, task.resultOutputDomain());

		etd2.resultHash(Number160.createHash(falseResult));
		assertEquals(false, task.isFinished());
		assertEquals(null, task.resultOutputDomain());

		etd2.resultHash(Number160.createHash(trueResult));
		assertEquals(true, task.isFinished());
		assertEquals(etd, task.resultOutputDomain());

		task.nrOfSameResultHash(3);

		assertEquals(false, task.isFinished());
		assertEquals(null, task.resultOutputDomain());

		ExecutorTaskDomain etd3 = ExecutorTaskDomain
				.create("hello", executor2, task.newStatusIndex(),
						JobProcedureDomain.create("job1", 0, submitter, "WordCount", 0))
				.resultHash(Number160.createHash(trueResult));
		task.addOutputDomain(etd3);
		assertEquals(true, task.isFinished());
		assertEquals(etd, task.resultOutputDomain()); // Always take the first one...
	}

	@Test
	public void testActiveCountAndNrOfSameResultHashSameExecutorPossible() {
		// ===========================================================================================================================================
		// Here the task may be executed by the same executor multiple times.
		// ===========================================================================================================================================

		String localExecutorId = "E1";
		Task task = (Task) Task.create("1", localExecutorId).nrOfSameResultHash(2)
				.needsMultipleDifferentExecutors(false);
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(true, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		task.incrementActiveCount();
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(true, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		task.incrementActiveCount();
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(2), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		// Cannot increment more than number of same result hash
		task.incrementActiveCount();
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(2), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		// Decrementing again
		JobProcedureDomain jpd = JobProcedureDomain.create("J1", 0, "E1", "P1", 1); // faster than mocking...
		task.addOutputDomain(
				ExecutorTaskDomain.create(task.key(), localExecutorId, 0, jpd).resultHash(Number160.ZERO));
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		// Not same result hash, currentMaxNr stays the same
		task.addOutputDomain(
				ExecutorTaskDomain.create(task.key(), localExecutorId, 1, jpd).resultHash(Number160.ONE));
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(true, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		// Increment again by 1
		task.incrementActiveCount();
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		// same domain cannot be added twice. Stays the same!
		task.addOutputDomain(
				ExecutorTaskDomain.create(task.key(), localExecutorId, 0, jpd).resultHash(Number160.ONE));
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		// same result hash, currentMaxNr increases, different executor
		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), "E2", 0, jpd).resultHash(Number160.ONE));
		assertEquals(new Integer(2), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(true, task.isFinished());

		// Another of the other executor... ignored
		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), "E2", 1, jpd).resultHash(Number160.ONE));
		assertEquals(new Integer(2), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(true, task.isFinished());

		// Another of the other executor... ignored
		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), "E2", 2, jpd).resultHash(Number160.ONE));
		assertEquals(new Integer(2), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(true, task.isFinished());

		// Current executor finishes, but has no effect anymore except decreasing the active count
		task.addOutputDomain(
				ExecutorTaskDomain.create(task.key(), localExecutorId, 2, jpd).resultHash(Number160.ZERO));
		assertEquals(new Integer(2), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(true, task.isFinished());
	}

	@Test
	public void testActiveCountAndNrOfSameResultHashDifferentExecutors() {
		// ===========================================================================================================================================
		// Here the task may be executed by the same executor ONLY ONCE.
		// ===========================================================================================================================================

		String executor = "E1";
		Task task = (Task) Task.create("1", executor).nrOfSameResultHash(2)
				.needsMultipleDifferentExecutors(true);
		assertEquals(true, task.canBeExecuted());
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(false, task.isFinished());

		task.incrementActiveCount();
		// Now it should not be possible to execute the task once more
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.isFinished());

		// Now this one finishes the execution and one result domain for this executor is available --> active
		// count cannot be increased anymore, task
		// may not be executed anymore
		JobProcedureDomain jpd = JobProcedureDomain.create("J1", 0, "E1", "P1", 1); // faster than mocking...
		task.addOutputDomain(
				ExecutorTaskDomain.create(task.key(), executor, 0, jpd).resultHash(Number160.ZERO));
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(false, task.isFinished());

		// Active count cannot be increased anymore now
		task.incrementActiveCount();
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(false, task.isFinished());

		// Adding another of the same executor has no effect whatsoever (should not happen anyways)
		task.addOutputDomain(
				ExecutorTaskDomain.create(task.key(), executor, 0, jpd).resultHash(Number160.ZERO));
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(false, task.isFinished());

		// Adding another of the same executor has no effect whatsoever (should not happen anyways)
		task.addOutputDomain(
				ExecutorTaskDomain.create(task.key(), executor, 1, jpd).resultHash(Number160.ZERO));
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(false, task.isFinished());

		// Adding another of a different executor, however, increases the nr of result hashs
		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), "E2", 0, jpd).resultHash(Number160.ZERO));
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(2), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(true, task.isFinished());

		// Next check that every executor may only occur once (external same executor may not be added twice
		// either
		task.nrOfSameResultHash(3);
		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), "E2", 0, jpd).resultHash(Number160.ZERO));
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(2), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(false, task.isFinished());

		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), "E2", 0, jpd).resultHash(Number160.ONE));
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(2), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(false, task.isFinished());

		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), "E3", 0, jpd).resultHash(Number160.ONE));
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(2), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(false, task.isFinished());

		// E3 had it's shot ... although it comes in with another result hash, it is ignored
		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), "E3", 1, jpd).resultHash(Number160.ZERO));
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(2), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(false, task.isFinished());

		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), "E4", 1, jpd).resultHash(Number160.ZERO));
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(3), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(true, task.isFinished());
	}

	@Test
	public void testReset() {
		Task task = Task.create("Key", "E1");
		task.nrOfSameResultHash(2);
		task.incrementActiveCount();
		task.incrementActiveCount();
		IDomain etd = Mockito.mock(ExecutorTaskDomain.class);
		Mockito.when(etd.executor()).thenReturn("E1");
		Mockito.when(etd.resultHash()).thenReturn(Number160.ZERO);
		IDomain etd2 = Mockito.mock(ExecutorTaskDomain.class);
		Mockito.when(etd2.executor()).thenReturn("E2");
		Mockito.when(etd2.resultHash()).thenReturn(Number160.ZERO);
		task.addOutputDomain(etd);
		task.addOutputDomain(etd2);
		task.isInProcedureDomain(true);
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(2, task.nrOfOutputDomains());
		assertEquals(true, task.isInProcedureDomain());
		assertEquals(true, task.isFinished());
		assertEquals(etd, task.resultOutputDomain());
		task.reset();
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(0, task.nrOfOutputDomains());
		assertEquals(false, task.isInProcedureDomain());
		assertEquals(false, task.isFinished());
		assertEquals(null, task.resultOutputDomain());
	}

	@Test
	public void testCanBeExecuted() {
		Task task = Task.create("Key", "E1");
		// Nope, cannot be executed
		task.nrOfSameResultHash(0);
		assertEquals(false, task.canBeExecuted());
		assertEquals(true, task.isFinished());

		// Can
		task.nrOfSameResultHash(1);
		assertEquals(true, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		// Cannot
		task.incrementActiveCount();
		assertEquals(false, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		// Cannot
		IDomain etd = Mockito.mock(ExecutorTaskDomain.class);
		Mockito.when(etd.executor()).thenReturn("E1");
		Mockito.when(etd.resultHash()).thenReturn(Number160.ZERO);
		task.addOutputDomain(etd);
		assertEquals(false, task.canBeExecuted());
		assertEquals(true, task.isFinished());

		// can again...
		task.nrOfSameResultHash(2);
		task.addOutputDomain(etd);
		assertEquals(true, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		// Nope
		task.incrementActiveCount();
		assertEquals(false, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		// Neither... and is finished btw.
		IDomain etd2 = Mockito.mock(ExecutorTaskDomain.class);
		Mockito.when(etd2.resultHash()).thenReturn(Number160.ZERO);
		Mockito.when(etd2.executor()).thenReturn("E2");
		task.addOutputDomain(etd2);
		assertEquals(false, task.canBeExecuted());
		assertEquals(true, task.isFinished());
	}

	@Test
	public void testNewStatusIndex() {
		Task task = Task.create("Key", "E1");
		for (int i = 0; i < 10; ++i) {
			assertEquals(i, task.newStatusIndex());
		}

	}

	@Test
	public void testCheckIfFinished() {
		Task task = Task.create("Key", "E1");

		// Doesn't need different executors
		// finished, as it doesn't need to be executed AT ALL
		assertEquals(true, task.isFinished());
		assertEquals(null, task.resultOutputDomain());
		assertEquals(null, task.resultHash());

		// Finished
		task.nrOfSameResultHash(0);
		assertEquals(true, task.isFinished());
		assertEquals(null, task.resultOutputDomain());
		assertEquals(null, task.resultHash());

		// Not Finished: hash is null
		task.nrOfSameResultHash(1);

		ExecutorTaskDomain etd = Mockito.mock(ExecutorTaskDomain.class);
		Mockito.when(etd.resultHash()).thenReturn(null);
		Mockito.when(etd.executor()).thenReturn("E1");
		task.addOutputDomain(etd);
		assertEquals(false, task.isFinished());
		assertEquals(null, task.resultOutputDomain());
		assertEquals(null, task.resultHash());

		// Finished
		Mockito.when(etd.resultHash()).thenReturn(Number160.ZERO);
		assertEquals(true, task.isFinished());
		assertEquals(etd, task.resultOutputDomain());
		assertEquals(Number160.ZERO, task.resultHash());

		// Not Finished
		task.nrOfSameResultHash(2);
		assertEquals(false, task.isFinished());
		assertEquals(null, task.resultOutputDomain());
		assertEquals(null, task.resultHash());

		// Not Finished
		ExecutorTaskDomain etd2 = Mockito.mock(ExecutorTaskDomain.class);
		Mockito.when(etd2.resultHash()).thenReturn(null);
		Mockito.when(etd2.executor()).thenReturn("E1");
		task.addOutputDomain(etd2);
		assertEquals(false, task.isFinished());
		assertEquals(null, task.resultOutputDomain());
		assertEquals(null, task.resultHash());

		// Finished (not same result hash)
		Mockito.when(etd2.resultHash()).thenReturn(Number160.ONE);
		assertEquals(false, task.isFinished());
		assertEquals(null, task.resultOutputDomain());
		assertEquals(null, task.resultHash());

		// Finished (same result hash)
		Mockito.when(etd2.resultHash()).thenReturn(Number160.ZERO);
		assertEquals(true, task.isFinished());
		assertEquals(etd, task.resultOutputDomain());
		assertEquals(Number160.ZERO, task.resultHash());

		// Needs different executors
		task.needsMultipleDifferentExecutors(true);
		// Not Finished
		assertEquals(false, task.isFinished());
		assertEquals(null, task.resultOutputDomain());
		assertEquals(null, task.resultHash());

		// Finished
		Mockito.when(etd2.executor()).thenReturn("E2");
		assertEquals(true, task.isFinished());
		assertEquals(etd, task.resultOutputDomain());
		assertEquals(Number160.ZERO, task.resultHash());
	}

	@Test
	public void testCurrentMaxNrOfSameResultHash() {
		Task task = Task.create("Key", "E1").nrOfSameResultHash(2);
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());

		ExecutorTaskDomain etd = Mockito.mock(ExecutorTaskDomain.class);
		Mockito.when(etd.resultHash()).thenReturn(null);
		Mockito.when(etd.executor()).thenReturn("E1");
		task.addOutputDomain(etd);
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash()); // null is not a result hash;
		// 1 Result hash
		Mockito.when(etd.resultHash()).thenReturn(Number160.ZERO);
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());

		// 2nd
		ExecutorTaskDomain etd2 = Mockito.mock(ExecutorTaskDomain.class);
		Mockito.when(etd2.resultHash()).thenReturn(null);
		Mockito.when(etd2.executor()).thenReturn("E1");
		task.addOutputDomain(etd2);
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		// No difference: 2 result hashs but are not the same
		Mockito.when(etd2.resultHash()).thenReturn(Number160.ONE);
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		// 2 with same result hash
		Mockito.when(etd2.resultHash()).thenReturn(Number160.ZERO);
		assertEquals(new Integer(2), task.currentMaxNrOfSameResultHash());

	}

	@Test
	public void testContainsExecutor() throws Exception {
		Task task = Task.create("Key", "E1").nrOfSameResultHash(1);
		Method containsExecutor = Task.class.getSuperclass().getDeclaredMethod("containsExecutor",
				String.class);
		containsExecutor.setAccessible(true);
		assertEquals(false, containsExecutor.invoke(task, "E1"));
		IDomain etd = Mockito.mock(ExecutorTaskDomain.class);
		Mockito.when(etd.executor()).thenReturn("E1");
		task.addOutputDomain(etd);
		assertEquals(true, containsExecutor.invoke(task, "E1"));
	}

}
