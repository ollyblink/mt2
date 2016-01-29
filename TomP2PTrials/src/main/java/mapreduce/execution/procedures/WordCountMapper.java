package mapreduce.execution.procedures;

import java.util.Collection;
import java.util.StringTokenizer;

import mapreduce.execution.context.IContext;

public class WordCountMapper implements IExecutable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4762197473747869364L;
	private static final int ONE = 1;
	@Override
	public void process(Object keyIn, Collection<Object> valuesIn, IContext context) {
		for (Object value : valuesIn) {
			String valueString = (String) value;
			StringTokenizer tokens = new StringTokenizer(valueString);
			while (tokens.hasMoreTokens()) {
				String word = tokens.nextToken();
				context.write(word, ONE);
			}
		}
	}

	public static IExecutable create() {
		return new WordCountMapper();
	}

	private WordCountMapper() {

	}
}
