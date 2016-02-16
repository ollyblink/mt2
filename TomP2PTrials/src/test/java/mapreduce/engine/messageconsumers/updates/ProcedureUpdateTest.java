package mapreduce.engine.messageconsumers.updates;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.EndProcedure;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.execution.procedures.WordCountMapper;
import net.tomp2p.peers.Number160;

public class ProcedureUpdateTest {

//	private JobCalculationExecutor calculationExecutor;
	private JobCalculationMessageConsumer calculationMsgConsumer;
	private Job job;

	private JobProcedureDomain outputDomain;
	private Procedure procedure;
	private ProcedureUpdate procedureUpdate;

	@Before
	public void setUpBeforeTest() throws Exception {

		// Calculation Executor
//		calculationExecutor = Mockito.mock(JobCalculationExecutor.class);
		// Calculation MessageConsumer
		calculationMsgConsumer = Mockito.mock(JobCalculationMessageConsumer.class);
//		Mockito.when(calculationMsgConsumer.executor()).thenReturn(calculationExecutor);
		job = Job.create("S1");
	}

	@Test
	public void testBothNull() {
		// Test if any null
		// Both null
		outputDomain = null;
		procedure = null;

		ProcedureUpdate.create(job, calculationMsgConsumer, outputDomain).executeUpdate(procedure);
		assertEquals(null, procedure);

	}

	@Test
	public void testProcedureNull() {
		// Procedure null
		procedure = null;
		outputDomain = Mockito.mock(JobProcedureDomain.class);
		ProcedureUpdate.create(job, calculationMsgConsumer, outputDomain).executeUpdate(procedure);
		assertEquals(null, procedure);
	}

	@Test
	public void testDomainNull() {
		// Output domain null
		Mockito.mock(Procedure.class);
		Procedure tmp = procedure;
		outputDomain = null;
		ProcedureUpdate.create(job, calculationMsgConsumer, outputDomain).executeUpdate(procedure);
		assertEquals(tmp, procedure);

	}

	@Test
	public void testNonNull() {
		// Both not null
		Mockito.mock(Procedure.class);
		Procedure tmp = procedure;
		outputDomain = Mockito.mock(JobProcedureDomain.class);
		ProcedureUpdate.create(job, calculationMsgConsumer, outputDomain).executeUpdate(procedure);
		assertEquals(tmp, procedure);
	}
 

	@Test
	public void testFinishProcedure() {
		// Here the procedure is tried to be finished and the next procedure to be executed
		// One procedure is needed since else it would directly jump to EndProcedure and finish the job
		// Simplest possible idea: procedure only needs to be executed once
		job = Job.create("S1").addSucceedingProcedure(WordCountMapper.create(), null, 1, 1, false, false);

		// Assumption: only one file expected
		JobProcedureDomain startInJPD = Mockito.mock(JobProcedureDomain.class);
		Mockito.when(startInJPD.expectedNrOfFiles()).thenReturn(1);
		job.currentProcedure().dataInputDomain(startInJPD);
		assertEquals(false, job.procedure(0).isFinished());
		assertEquals(false, job.procedure(1).isFinished());
		assertEquals(true, job.procedure(2).isFinished());
		assertEquals(0, job.currentProcedure().procedureIndex());
		assertEquals(StartProcedure.class.getSimpleName(), job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals(false, job.isFinished());
		assertEquals(job.procedure(0).resultOutputDomain(), job.procedure(1).dataInputDomain()); // null

		// Finish it
		JobProcedureDomain startOutJPD = Mockito.mock(JobProcedureDomain.class);
		Mockito.when(startOutJPD.resultHash()).thenReturn(Number160.ONE);

		ProcedureUpdate.create(job, calculationMsgConsumer, startOutJPD).executeUpdate(job.currentProcedure());
		 
		assertEquals(true, job.procedure(0).isFinished());
		assertEquals(false, job.procedure(1).isFinished());
		assertEquals(true, job.procedure(2).isFinished());
		assertEquals(1, job.currentProcedure().procedureIndex());
		assertEquals(WordCountMapper.class.getSimpleName(), job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals(false, job.isFinished());
		assertEquals(startOutJPD, job.procedure(1).dataInputDomain());

		// Finish it

		JobProcedureDomain res = Mockito.mock(JobProcedureDomain.class);
		Mockito.when(res.resultHash()).thenReturn(Number160.ONE);

		ProcedureUpdate.create(job, calculationMsgConsumer, res).executeUpdate(job.currentProcedure());
		assertEquals(true, job.procedure(0).isFinished());
		assertEquals(true, job.procedure(1).isFinished());
		assertEquals(true, job.procedure(2).isFinished());
		assertEquals(2, job.currentProcedure().procedureIndex());
		assertEquals(EndProcedure.class.getSimpleName(), job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals(true, job.isFinished());
		assertEquals(res, job.procedure(2).dataInputDomain());

	}

}
