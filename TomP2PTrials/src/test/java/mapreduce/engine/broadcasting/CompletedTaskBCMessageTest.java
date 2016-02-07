package mapreduce.engine.broadcasting;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import mapreduce.engine.broadcasting.messages.CompletedTaskBCMessage;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;

public class CompletedTaskBCMessageTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() {
		JobProcedureDomain outputDomain = JobProcedureDomain.create("J1", 0, "E1", "P1", 0, 0);
		CompletedTaskBCMessage msg = CompletedTaskBCMessage.create(outputDomain, null);

		ExecutorTaskDomain outputETD = ExecutorTaskDomain.create("t1", "E1", 0, outputDomain);
		msg.addOutputDomainTriple(outputETD);
		assertEquals(1, msg.allExecutorTaskDomains().size());

		outputETD = ExecutorTaskDomain.create("t1", "E1", 0, outputDomain);
		msg.addOutputDomainTriple(outputETD);
		assertEquals(2, msg.allExecutorTaskDomains().size());
		
		outputETD = ExecutorTaskDomain.create("t1", "E1", 0, outputDomain);
		msg.addOutputDomainTriple(outputETD);
		assertEquals(3, msg.allExecutorTaskDomains().size());
		
		//No change, different JPD
		outputDomain = JobProcedureDomain.create("J1", 0, "E1", "P1", 0, 1);
		outputETD = ExecutorTaskDomain.create("t1", "E1", 0, outputDomain);
		msg.addOutputDomainTriple(outputETD);
		assertEquals(3, msg.allExecutorTaskDomains().size());
	}

}
