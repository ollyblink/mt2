package mapreduce.execution.procedures;

import java.util.Collection;

import mapreduce.execution.context.IContext;

public class WordsWithSameCounts implements IExecutable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5357959558731339590L;

	@Override
	public void process(Object keyIn, Collection<Object> valuesIn, IContext context) {
		// TODO Auto-generated method stub
		if (valuesIn.iterator().hasNext()) {
			context.write(((Integer) valuesIn.iterator().next()), keyIn);
		}
	}

	public static WordsWithSameCounts create() {
		return new WordsWithSameCounts();
	}

}
