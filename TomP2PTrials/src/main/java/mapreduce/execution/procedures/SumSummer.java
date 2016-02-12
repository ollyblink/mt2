package mapreduce.execution.procedures;

import java.util.Collection;

import mapreduce.execution.context.IContext;

public class SumSummer implements IExecutable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8048616744720175048L;

	@Override
	public void process(Object keyIn, Collection<Object> valuesIn, IContext context) {
		long finalSum = 0;
		for (Object o : valuesIn) {
			finalSum += (long) o;

		}
		context.write("FINAL SUM", finalSum);
	}
	
	public static SumSummer create(){
		return new SumSummer();
	}
}