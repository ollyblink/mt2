package mapreduce.engine.componenttests;

import java.util.Collection;

import mapreduce.execution.context.IContext;
import mapreduce.execution.procedures.IExecutable;

public class TestWaitingProcedure implements IExecutable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2153979385921820417L;
	/** Default waiting time is 5 seconds */
	private static final long DEFAULT_WAITING_TIME = 5000;
	private long waitingTime;

	private TestWaitingProcedure(long waitingTime) {
		this.waitingTime = waitingTime;
	}

	private TestWaitingProcedure() {
		this(DEFAULT_WAITING_TIME);
	}

	@Override
	public void process(Object keyIn, Collection<Object> valuesIn, IContext context) {
		try {
			Thread.sleep(waitingTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		for(Object valueOut: valuesIn){
			context.write(keyIn, valueOut);
		}
	}

	public static TestWaitingProcedure create(long waitingTime) {
		return new TestWaitingProcedure(waitingTime);
	}

	public static TestWaitingProcedure create() {
		return new TestWaitingProcedure();
	}
}
