package mapreduce.execution.procedures;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.execution.context.IContext;

public class ProceduresTest {

	@Test
	public void testConversion() {
		IExecutable wordCountMapper = Procedures.convertJavascriptToJava(
				"function process(keyIn, valuesIn, context){ " + "for each (var value in valuesIn) {   "
						+ "var splits = value.split(\" \"); " + "for each (var split in splits){ "
						+ "context.write(split, 1); " + "} " + "} " + "}");
		IContext context = Mockito.mock(IContext.class);
		Collection<Object> values = new ArrayList<>();
		values.add("hello world this is a test");
		wordCountMapper.process(null, values, context);

		Mockito.verify(context, Mockito.times(1)).write("hello", 1);
		Mockito.verify(context, Mockito.times(1)).write("this", 1);
		Mockito.verify(context, Mockito.times(1)).write("is", 1);
		Mockito.verify(context, Mockito.times(1)).write("a", 1);
		Mockito.verify(context, Mockito.times(1)).write("test", 1); 
	}

}
