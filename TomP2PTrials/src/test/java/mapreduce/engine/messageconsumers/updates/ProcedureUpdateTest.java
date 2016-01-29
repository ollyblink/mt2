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

	private JobCalculationExecutor calculationExecutor;
	private JobCalculationMessageConsumer calculationMsgConsumer;
	private Job job;

	private IDomain outputDomain;
	private Procedure procedure;
	private ProcedureUpdate procedureUpdate;

	@Before
	public void setUpBeforeTest() throws Exception {

		// Calculation Executor
		calculationExecutor = Mockito.mock(JobCalculationExecutor.class);
		// Calculation MessageConsumer
		calculationMsgConsumer = Mockito.mock(JobCalculationMessageConsumer.class);
		Mockito.when(calculationMsgConsumer.executor()).thenReturn(calculationExecutor);
		job = Job.create("S1");
		procedureUpdate = new ProcedureUpdate(job, calculationMsgConsumer);
	}

	@Test
	public void testBothNull() {
		// Test if any null
		// Both null
		outputDomain = null;
		procedure = null;
		procedureUpdate.executeUpdate(outputDomain, procedure);
		assertEquals(null, procedure);

	}

	@Test
	public void testProcedureNull() {
		// Procedure null
		procedure = null;
		outputDomain = Mockito.mock(IDomain.class);
		procedureUpdate.executeUpdate(outputDomain, procedure);
		assertEquals(null, procedure);
	}

	@Test
	public void testDomainNull() {
		// Output domain null
		Mockito.mock(Procedure.class);
		Procedure tmp = procedure;
		outputDomain = null;
		procedureUpdate.executeUpdate(outputDomain, procedure);
		assertEquals(tmp, procedure);

	}

	@Test
	public void testNonNull() {
		// Both not null
		Mockito.mock(Procedure.class);
		Procedure tmp = procedure;
		outputDomain = Mockito.mock(IDomain.class);
		procedureUpdate.executeUpdate(outputDomain, procedure);
		assertEquals(tmp, procedure);
	}

	@Test(expected = ClassCastException.class)
	public void testWrongDomainTypeExceptionCaught() {
		IDomain outputDomain = Mockito.mock(ExecutorTaskDomain.class);
		Procedure procedure = Mockito.mock(Procedure.class);
		procedure.dataInputDomain(Mockito.mock(JobProcedureDomain.class));
		Procedure pTmp = procedure;
		// Wrong domain type --> returns old procedure and logs exception
		procedureUpdate.internalUpdate(outputDomain, procedure);
		assertEquals(pTmp, procedure);
	}

	@Test(expected = NullPointerException.class)
	public void testMessageConsumerNullException() {
		procedureUpdate = new ProcedureUpdate(job, null);
		IDomain outputDomain = Mockito.mock(JobProcedureDomain.class);
		Procedure procedure = Mockito.mock(Procedure.class);
		Mockito.when(procedure.dataInputDomain()).thenReturn(Mockito.mock(JobProcedureDomain.class));
		Mockito.when(procedure.isFinished()).thenReturn(true);
		Procedure pTmp = procedure;
		// Wrong domain type --> returns old procedure and logs exception
		procedureUpdate.internalUpdate(outputDomain, procedure);
		assertEquals(pTmp, procedure);
	}

	@Test(expected = NullPointerException.class)
	public void testJobNullException() {
		procedureUpdate = new ProcedureUpdate(null, calculationMsgConsumer);
		IDomain outputDomain = Mockito.mock(JobProcedureDomain.class);
		Procedure procedure = Mockito.mock(Procedure.class);
		Mockito.when(procedure.dataInputDomain()).thenReturn(Mockito.mock(JobProcedureDomain.class));

		Mockito.when(procedure.isFinished()).thenReturn(true);
		Procedure pTmp = procedure;
		// Wrong domain type --> returns old procedure and logs exception
		procedureUpdate.internalUpdate(outputDomain, procedure);
		assertEquals(pTmp, procedure);
	}

	@Test(expected = NullPointerException.class)
	public void testBothVarsNull() {
		procedureUpdate = new ProcedureUpdate(null, null);
		IDomain outputDomain = Mockito.mock(JobProcedureDomain.class);
		Procedure procedure = Mockito.mock(Procedure.class);
		Mockito.when(procedure.dataInputDomain()).thenReturn(Mockito.mock(JobProcedureDomain.class));

		Mockito.when(procedure.isFinished()).thenReturn(true);
		Procedure pTmp = procedure;
		// Wrong domain type --> returns old procedure and logs exception
		procedureUpdate.internalUpdate(outputDomain, procedure);
		assertEquals(pTmp, procedure);
	}

	@Test
	public void testFinishProcedure() {
		// Here the procedure is tried to be finished and the next procedure to be executed
		// One procedure is needed since else it would directly jump to EndProcedure and finish the job
		// Simplest possible idea: procedure only needs to be executed once
		job = Job.create("S1").addSucceedingProcedure(WordCountMapper.create(), null, 1, 1, false, false);
		procedureUpdate = new ProcedureUpdate(job, calculationMsgConsumer);

		// Assumption: only one file expected
		JobProcedureDomain startInJPD = Mockito.mock(JobProcedureDomain.class);
		Mockito.when(startInJPD.expectedNrOfFiles()).thenReturn(1);
		job.currentProcedure().dataInputDomain(startInJPD);
		assertEquals(false, job.procedure(0).isFinished());
		assertEquals(false, job.procedure(1).isFinished());
		assertEquals(true, job.procedure(2).isFinished()); 
		assertEquals(0, job.currentProcedure().procedureIndex());
		assertEquals(StartProcedure.class.getSimpleName(),
				job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals(false, job.isFinished());
		assertEquals(job.procedure(0).resultOutputDomain(), job.procedure(1).dataInputDomain()); // null

		// Finish it
		JobProcedureDomain startOutJPD = Mockito.mock(JobProcedureDomain.class);
		Mockito.when(startOutJPD.resultHash()).thenReturn(Number160.ONE);

		procedureUpdate.internalUpdate(startOutJPD, job.currentProcedure());

		assertEquals(true, job.procedure(0).isFinished());
		assertEquals(false, job.procedure(1).isFinished());
		assertEquals(true, job.procedure(2).isFinished());
		assertEquals(1, job.currentProcedure().procedureIndex());
		assertEquals(WordCountMapper.class.getSimpleName(),
				job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals(false, job.isFinished());
		assertEquals(job.procedure(0).resultOutputDomain(), job.procedure(1).dataInputDomain());

		// Finish it
		procedureUpdate.internalUpdate(startOutJPD, job.currentProcedure());
		assertEquals(true, job.procedure(0).isFinished());
		assertEquals(true, job.procedure(1).isFinished());
		assertEquals(true, job.procedure(2).isFinished());
		assertEquals(2, job.currentProcedure().procedureIndex());
		assertEquals(EndProcedure.class.getSimpleName(),
				job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals(true, job.isFinished());
		assertEquals(job.procedure(1).resultOutputDomain(), job.procedure(2).dataInputDomain());
 
	}

}
