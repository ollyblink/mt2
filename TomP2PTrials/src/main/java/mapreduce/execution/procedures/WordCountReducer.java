package mapreduce.execution.procedures;

import java.util.Collection;

import mapreduce.execution.context.IContext;

public class WordCountReducer implements IExecutable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6674686068934510011L;
	/** Only keeps counts that are larger than this number (default is 0, keeping all counts) */
	private int countAbove;

	private WordCountReducer(int countAbove) {
		this.countAbove = countAbove;
	}

	public static WordCountReducer create() {
		return new WordCountReducer(0);
	}

	public static WordCountReducer create(int countAbove) {
		return new WordCountReducer(countAbove);
	}

	@Override
	public void process(Object keyIn, Collection<Object> valuesIn, IContext context) {
		int sum = 0;
		for (Object o : valuesIn) {
			sum += (Integer) o;
		}
		if (sum >= countAbove) {
			context.write(keyIn, sum);
		}
	}

}
