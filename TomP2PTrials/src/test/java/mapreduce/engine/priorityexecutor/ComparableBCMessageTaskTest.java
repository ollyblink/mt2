package mapreduce.engine.priorityexecutor;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import mapreduce.engine.broadcasting.messages.BCMessageStatus;
import mapreduce.engine.multithreading.ComparableBCMessageTask;
import mapreduce.execution.jobs.PriorityLevel;

public class ComparableBCMessageTaskTest {

	@Test
	public void test() {
		List<ComparableBCMessageTask<Integer>> tasks = new ArrayList<>();
		Runnable r = new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub

			}

		};
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), "0", 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), "0", 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), "0", 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), "0", 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), "0", 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), "0", 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), "0", 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), "0", 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), "0", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), "0", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), "0", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), "0", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), "0", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), "0", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), "0", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), "0", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));

		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), "0", 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), "0", 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), "0", 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), "0", 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), "0", 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), "0", 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), "0", 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), "0", 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), "0", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), "0", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), "0", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), "0", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), "0", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), "0", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), "0", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), "0", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));

		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), "0", 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), "0", 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), "0", 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), "0", 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), "0", 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), "0", 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), "0", 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), "0", 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), "0", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), "0", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), "0", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), "0", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), "0", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), "0", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), "0", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), "0", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));

		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), "1", 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), "1", 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), "1", 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), "1", 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), "1", 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), "1", 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), "1", 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), "1", 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), "1", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), "1", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), "1", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), "1", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), "1", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), "1", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), "1", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), "1", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));

		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), "1", 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), "1", 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), "1", 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), "1", 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), "1", 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), "1", 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), "1", 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), "1", 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), "1", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), "1", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), "1", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), "1", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), "1", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), "1", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), "1", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), "1", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));

		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), "1", 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), "1", 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), "1", 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), "1", 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), "1", 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), "1", 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), "1", 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), "1", 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), "1", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), "1", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), "1", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), "1", 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), "1", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), "1", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), "1", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), "1", 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));

		Collections.sort(tasks);
		// for (ComparableBCMessageTask<Integer> o : tasks) {
		// System.out.println(o.toString());
		// }
		// Too many cases... only testing corner cases
		assertEquals(tasks.get(0).getJobPriority(), PriorityLevel.HIGH);
		assertEquals(tasks.get(tasks.size() / 2).getJobPriority(), PriorityLevel.MODERATE);
		assertEquals(tasks.get(tasks.size() - 1).getJobPriority(), PriorityLevel.LOW);

		assertEquals(tasks.get(0).getJobCreationTime(), new Long(0));
		assertEquals(tasks.get(tasks.size() - 1).getJobCreationTime(), new Long(1));

		assertEquals(tasks.get(0).getJobId(), "0");
		assertEquals(tasks.get(tasks.size() - 1).getJobId(), "1");

		assertEquals(tasks.get(0).getProcedureIndex(), new Integer(1));
		assertEquals(tasks.get(tasks.size() - 1).getProcedureIndex(), new Integer(0));

		assertEquals(tasks.get(0).getMessageStatus(), BCMessageStatus.COMPLETED_PROCEDURE);
		assertEquals(tasks.get(tasks.size() - 1).getMessageStatus(), BCMessageStatus.COMPLETED_TASK);
		
		assertEquals(tasks.get(0).getMessageCreationTime(), new Long(0));
		assertEquals(tasks.get(tasks.size() - 1).getMessageCreationTime(), new Long(1));
	}

}
