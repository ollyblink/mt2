package mapreduce.engine.messageconsumers.updates;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.procedures.IExecutable;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.tasks.Task;
import net.tomp2p.peers.Number160;

public class TaskUpdateTest {
	private JobCalculationMessageConsumer calculationMsgConsumer;
	// private JobCalculationExecutor calculationExecutor;

	private JobProcedureDomain jpd = JobProcedureDomain.create("J1", 0, "E1", "P1", 1, 0);
	private ExecutorTaskDomain outputDomain;
	private Procedure procedure;

	@Before
	public void setUpBeforeTest() throws Exception {
		// Calculation Executor
		// calculationExecutor = Mockito.mock(JobCalculationExecutor.class);
		// Mockito.when(calculationExecutor.id()).thenReturn("E1");
		// Calculation MessageConsumer
		calculationMsgConsumer = Mockito.mock(JobCalculationMessageConsumer.class);
		// Mockito.when(calculationMsgConsumer.executor()).thenReturn(calculationExecutor);

	}

	@Test
	public void testBothNull() {
		// Test if any null
		// Both null
		procedure = null;
		List<ExecutorTaskDomain> outputDomains = null;
		TaskUpdate.create(calculationMsgConsumer, outputDomains).executeUpdate(procedure);
		assertEquals(null, procedure);

	}

	@Test
	public void testProcedureNull() {
		// Procedure null
		procedure = null;
		outputDomain = Mockito.mock(ExecutorTaskDomain.class);
		List<ExecutorTaskDomain> outputDomains = new ArrayList<>();
		outputDomains.add(outputDomain);
		TaskUpdate.create(calculationMsgConsumer, outputDomains).executeUpdate(procedure);
		assertEquals(null, procedure);
	}

	@Test
	public void testDomainNull() {
		// Output domain null
		procedure = Mockito.mock(Procedure.class);
		Procedure tmp = procedure;
		outputDomain = null;
		List<ExecutorTaskDomain> outputDomains = new ArrayList<>();
		outputDomains.add(outputDomain);
		TaskUpdate.create(calculationMsgConsumer, outputDomains).executeUpdate(procedure);
		assertEquals(tmp, procedure);

	}

	@Test
	public void testNonNull() {
		// Both not null
		procedure = Mockito.mock(Procedure.class);
		Mockito.when(procedure.executable()).thenReturn(Mockito.mock(IExecutable.class));
		Procedure tmp = procedure;
		ExecutorTaskDomain outputDomain = ExecutorTaskDomain.create("hello", "E1", 0, jpd).resultHash(Number160.ONE);

		List<ExecutorTaskDomain> outputDomains = new ArrayList<>();
		outputDomains.add(outputDomain);
		TaskUpdate.create(calculationMsgConsumer, outputDomains).executeUpdate(procedure);
		assertEquals(tmp, procedure);
	}

	@Test
	public void testTaskUpdateUnfinished() throws NullPointerException {
		// In this case, procedure does not have any tasks --> task will be added to procedure
		// Task will not be finished as it needs two executions to be marked finished
		Procedure procedure = Procedure.create(WordCountMapper.class, 1).nrOfSameResultHashForTasks(2).needsMultipleDifferentExecutorsForTasks(true);
		ExecutorTaskDomain outputDomain = ExecutorTaskDomain.create("hello", "E1", 0, jpd);

		assertEquals(0, procedure.tasksSize());
		List<ExecutorTaskDomain> outputDomains = new ArrayList<>();
		outputDomains.add(outputDomain);
		TaskUpdate.create(calculationMsgConsumer, outputDomains).executeUpdate(procedure);
		assertEquals(1, procedure.tasksSize());
		Task task = procedure.getTask(Task.create("hello", JobCalculationExecutor.classId));
		assertEquals("hello", task.key());
		assertEquals(1, task.nrOfOutputDomains());
		assertEquals(false, task.isFinished());
	}

	@Test
	public void testTaskUpdateSameInputDomainFinished() throws NullPointerException {
		// In this case, procedure does not have any tasks --> task will be added to procedure
		// Task will be finished after second execution
		Procedure procedure = Procedure.create(WordCountMapper.class, 1).nrOfSameResultHashForTasks(2).needsMultipleDifferentExecutorsForTasks(false);
		ExecutorTaskDomain outputDomain = ExecutorTaskDomain.create("hello", "E1", 0, jpd).resultHash(Number160.ONE);
		procedure.dataInputDomain(JobProcedureDomain.create("J1", 0, "E1", StartProcedure.class.getSimpleName(), 0, 0));
		assertEquals(0, procedure.tasksSize());
		List<ExecutorTaskDomain> outputDomains = new ArrayList<>();
		outputDomains.add(outputDomain);
		TaskUpdate.create(calculationMsgConsumer, outputDomains).executeUpdate(procedure);
		assertEquals(1, procedure.tasksSize());
		Task task = procedure.getTask(Task.create("hello", JobCalculationExecutor.classId));
		assertEquals("hello", task.key());
		assertEquals(false, task.isFinished());
		assertEquals(1, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).cancelTask(procedure.dataInputDomain().toString(), task);
		// Mockito.verify(calculationExecutor, Mockito.times(0)).switchDataFromTaskToProcedureDomain(procedure, task);

		outputDomains = new ArrayList<>();
		outputDomains.add(outputDomain);
		TaskUpdate.create(calculationMsgConsumer, outputDomains).executeUpdate(procedure);// Second execution with the same output domain
		// should not have any effect
		assertEquals(1, procedure.tasksSize());
		task = procedure.getTask(task);
		assertEquals("hello", task.key());
		assertEquals(false, task.isFinished());
		assertEquals(1, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).cancelTask(procedure.dataInputDomain().toString(), task);
		// Mockito.verify(calculationExecutor, Mockito.times(0)).switchDataFromTaskToProcedureDomain(procedure, task);

		// From same executor, but different domain (it's an additional execution!!)
		ExecutorTaskDomain outputDomain2 = ExecutorTaskDomain.create("hello", "E1", 1, jpd).resultHash(Number160.ONE);

		outputDomains = new ArrayList<>();
		outputDomains.add(outputDomain2);
		TaskUpdate.create(calculationMsgConsumer, outputDomains).executeUpdate(procedure);
		assertEquals(1, procedure.tasksSize()); // will stay the same as it contains it already
		task = procedure.getTask(task);
		assertEquals("hello", task.key());
		assertEquals(true, task.isFinished());
		assertEquals(2, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(1)).cancelTaskExecution(procedure.dataInputDomain().toString(), task);
		// Mockito.verify(calculationExecutor, Mockito.times(1)).switchDataFromTaskToProcedureDomain(procedure, task);

		// From same executor, but different domain (it's an additional execution!!)
		ExecutorTaskDomain outputDomain3 = ExecutorTaskDomain.create("hello", "E1", 2, jpd).resultHash(Number160.ONE);
		outputDomains = new ArrayList<>();
		outputDomains.add(outputDomain3);
		TaskUpdate.create(calculationMsgConsumer, outputDomains).executeUpdate(procedure);
		assertEquals(1, procedure.tasksSize()); // will stay the same as it contains it already
		task = procedure.getTask(task);
		assertEquals("hello", task.key());
		assertEquals(true, task.isFinished());

		// Nothing changes as this task is already finished
		assertEquals(2, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(1)).cancelTaskExecution(procedure.dataInputDomain().toString(), task);
		// Mockito.verify(calculationExecutor, Mockito.times(1)).switchDataFromTaskToProcedureDomain(procedure, task);
	}

	@Test
	public void testTaskUpdateDifferentInputDomainFinished() throws ClassCastException, NullPointerException {
		// In this case, procedure does not have any tasks --> task will be added to procedure
		// Task will be finished as after second execution
		Procedure procedure = Procedure.create(WordCountMapper.class, 1).nrOfSameResultHashForTasks(2).needsMultipleDifferentExecutorsForTasks(true);
		ExecutorTaskDomain outputDomain = ExecutorTaskDomain.create("hello", "E1", 0, jpd).resultHash(Number160.ONE);
		procedure.dataInputDomain(JobProcedureDomain.create("J1", 0, "E1", StartProcedure.class.getSimpleName(), 0, 0));

		assertEquals(0, procedure.tasksSize());

		List<ExecutorTaskDomain> outputDomains = new ArrayList<>();
		outputDomains.add(outputDomain);
		TaskUpdate.create(calculationMsgConsumer, outputDomains).executeUpdate(procedure);
		assertEquals(1, procedure.tasksSize());
		Task task = procedure.getTask(Task.create("hello", JobCalculationExecutor.classId));
		assertEquals("hello", task.key());
		assertEquals(false, task.isFinished());
		assertEquals(1, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).cancelTaskExecution(procedure.dataInputDomain().toString(), task);
		// Mockito.verify(calculationExecutor, Mockito.times(0)).switchDataFromTaskToProcedureDomain(procedure, task);

		outputDomains = new ArrayList<>();
		outputDomains.add(outputDomain);
		TaskUpdate.create(calculationMsgConsumer, outputDomains).executeUpdate(procedure);// Second execution with the same output domain
		// should not have any effect
		assertEquals(1, procedure.tasksSize());
		task = procedure.getTask(task);
		assertEquals("hello", task.key());
		assertEquals(false, task.isFinished());
		assertEquals(1, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).cancelTaskExecution(procedure.dataInputDomain().toString(), task);
		// Mockito.verify(calculationExecutor, Mockito.times(0)).switchDataFromTaskToProcedureDomain(procedure, task);

		// From same executor, but different domain (it's an additional execution!!)
		ExecutorTaskDomain outputDomain2 = ExecutorTaskDomain.create("hello", "E1", 1, jpd).resultHash(Number160.ONE);
		outputDomains = new ArrayList<>();
		outputDomains.add(outputDomain2);
		TaskUpdate.create(calculationMsgConsumer, outputDomains).executeUpdate(procedure);
		assertEquals(1, procedure.tasksSize()); // will stay the same as it contains it already
		task = procedure.getTask(task);
		assertEquals("hello", task.key());
		assertEquals(false, task.isFinished());
		assertEquals(1, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).cancelTaskExecution(procedure.dataInputDomain().toString(), task);
		// Mockito.verify(calculationExecutor, Mockito.times(0)).switchDataFromTaskToProcedureDomain(procedure, task);
		//
		// From same executor, but different domain (it's an additional execution!!)
		ExecutorTaskDomain outputDomain3 = ExecutorTaskDomain.create("hello", "E1", 2, jpd).resultHash(Number160.ONE);
		outputDomains = new ArrayList<>();
		outputDomains.add(outputDomain3);
		TaskUpdate.create(calculationMsgConsumer, outputDomains).executeUpdate(procedure);
		assertEquals(1, procedure.tasksSize()); // will stay the same as it contains it already
		task = procedure.getTask(task);
		assertEquals("hello", task.key());
		assertEquals(false, task.isFinished());
		assertEquals(1, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).cancelTaskExecution(procedure.dataInputDomain().toString(), task);
		// Mockito.verify(calculationExecutor, Mockito.times(0)).switchDataFromTaskToProcedureDomain(procedure, task);

		// From different executor finally
		ExecutorTaskDomain outputDomain4 = ExecutorTaskDomain.create("hello", "E2", 0, jpd).resultHash(Number160.ONE);
		outputDomains = new ArrayList<>();
		outputDomains.add(outputDomain4);
		TaskUpdate.create(calculationMsgConsumer, outputDomains).executeUpdate(procedure);
		assertEquals(1, procedure.tasksSize()); // will stay the same as it contains it already
		task = procedure.getTask(task);
		assertEquals("hello", task.key());
		assertEquals(true, task.isFinished());
		assertEquals(2, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(1)).cancelTaskExecution(procedure.dataInputDomain().toString(), task);
		// Mockito.verify(calculationExecutor, Mockito.times(1)).switchDataFromTaskToProcedureDomain(procedure, task);

		// From different executor again, but nothing changes anymore due to the finished procedure
		ExecutorTaskDomain outputDomain5 = ExecutorTaskDomain.create("hello", "E3", 0, jpd).resultHash(Number160.ONE);
		outputDomains = new ArrayList<>();
		outputDomains.add(outputDomain5);
		TaskUpdate.create(calculationMsgConsumer, outputDomains).executeUpdate(procedure);
		assertEquals(1, procedure.tasksSize()); // will stay the same as it contains it already
		task = procedure.getTask(task);
		assertEquals("hello", task.key());
		assertEquals(true, task.isFinished());
		assertEquals(2, task.nrOfOutputDomains()); // Won't be added anymore
		Mockito.verify(calculationMsgConsumer, Mockito.times(1)).cancelTaskExecution(procedure.dataInputDomain().toString(), task);
		// Mockito.verify(calculationExecutor, Mockito.times(1)).switchDataFromTaskToProcedureDomain(procedure, task);
	}

}
