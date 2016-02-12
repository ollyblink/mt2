package mapreduce.execution.procedures;

import java.util.Collection;
import java.util.StringTokenizer;

import mapreduce.execution.context.IContext;

public class StringToSum implements IExecutable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3737009639214001971L;

	@Override
	public void process(Object keyIn, Collection<Object> valuesIn, IContext context) {
		for (Object o : valuesIn) {
			String valueString = (String) o;
			StringTokenizer tokens = new StringTokenizer(valueString);
			long sum = 0;
			while (tokens.hasMoreTokens()) {
				sum += Long.parseLong(tokens.nextToken());

			}
			context.write("SUMS", sum);
		}

	}

	public static StringToSum create() {
		// TODO Auto-generated method stub
		return new StringToSum();
	}
}