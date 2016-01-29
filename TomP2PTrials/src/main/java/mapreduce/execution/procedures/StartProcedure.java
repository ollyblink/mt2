package mapreduce.execution.procedures;

import java.util.Collection;

import mapreduce.execution.context.IContext;

public class StartProcedure implements IExecutable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 618605646180541532L;

	@Override
	public void process(Object keyIn, Collection<Object> valuesIn, IContext context) {
		for (Object valueIn: valuesIn){
			context.write(keyIn, valueIn);
		}
	}

	public static StartProcedure create() {
		return new StartProcedure();
	}

	private StartProcedure() {

	}

}
