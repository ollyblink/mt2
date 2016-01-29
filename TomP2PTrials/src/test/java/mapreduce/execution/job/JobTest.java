package mapreduce.execution.job;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.procedures.WordCountReducer;

public class JobTest {

	@Test
	public void testAddingAndIncrementProcedure() {

		Job job = Job.create("TEST")
				.addSucceedingProcedure(WordCountMapper.create(), null, 1, 1, false, false)
				.addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false);

		assertEquals("StartProcedure", job.procedure(-100).executable().getClass().getSimpleName());
		assertEquals("StartProcedure", job.procedure(-10).executable().getClass().getSimpleName());
		assertEquals("StartProcedure", job.procedure(-1).executable().getClass().getSimpleName());
		assertEquals("StartProcedure", job.procedure(0).executable().getClass().getSimpleName());

		assertEquals("StartProcedure", job.procedure(0).executable().getClass().getSimpleName());
		assertEquals("WordCountMapper", job.procedure(1).executable().getClass().getSimpleName());
		assertEquals("WordCountReducer", job.procedure(2).executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(3).executable().getClass().getSimpleName());

		assertEquals("EndProcedure", job.procedure(10).executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(100).executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(1000).executable().getClass().getSimpleName());

		assertEquals("StartProcedure", job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals("StartProcedure", job.procedure(job.currentProcedure().procedureIndex()).executable()
				.getClass().getSimpleName());

		job.incrementProcedureIndex();
		assertEquals("WordCountMapper", job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals("WordCountMapper", job.procedure(job.currentProcedure().procedureIndex()).executable()
				.getClass().getSimpleName());

		job.incrementProcedureIndex();
		assertEquals("WordCountReducer", job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals("WordCountReducer", job.procedure(job.currentProcedure().procedureIndex()).executable()
				.getClass().getSimpleName());

		job.incrementProcedureIndex();
		assertEquals("EndProcedure", job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(job.currentProcedure().procedureIndex()).executable()
				.getClass().getSimpleName());

		job.incrementProcedureIndex();
		assertEquals("EndProcedure", job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(job.currentProcedure().procedureIndex()).executable()
				.getClass().getSimpleName());

		job.incrementProcedureIndex();
		assertEquals("EndProcedure", job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(job.currentProcedure().procedureIndex()).executable()
				.getClass().getSimpleName());

	}

}
