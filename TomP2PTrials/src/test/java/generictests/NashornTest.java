package generictests;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import mapreduce.execution.context.IContext;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.procedures.IExecutable;
import mapreduce.storage.IDHTConnectionProvider;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.peers.Number160;

public class NashornTest {
	public static void main(String[] args) throws ScriptException, NoSuchMethodException {
		ScriptEngineManager engineManager = new ScriptEngineManager();
		ScriptEngine engine = engineManager.getEngineByName("nashorn");
//		engine.eval(
//		"function process(keyIn, valuesIn, context){" 
//		+ " for each (var value in valuesIn) { "  
//		+ "    var splits = value.split(\" \");	" 
//		+ "    for each (var split in splits){ " 
//		+ "      context.write(split, 1);	"
//		+ "    } " 
//		+ "  }" 
//		+ "}");
		engine.eval(
		"function process(keyIn, valuesIn, context){"
		+ "var count = 0;" 
		+ " for each (var value in valuesIn) { "  
		+ "    count += value;	"  
		+ "  }"
		+ "context.write(keyIn, count);" 
		+ "}");

		Invocable invocable = (Invocable) engine;
		IExecutable procedure = invocable.getInterface(IExecutable.class);
		Collection<Object> values = new ArrayList<Object>();
		values.add(1);
		values.add(1);
		values.add(1);
		values.add(1);
		
		procedure.process("hello", values, new IContext() {

			@Override
			public void write(Object keyOut, Object valueOut) {
				System.out.println("<" + keyOut + "," + valueOut + ">");
			}

			@Override
			public Number160 resultHash() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public IContext outputExecutorTaskDomain(ExecutorTaskDomain outputExecutorTaskDomain) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public List<FuturePut> futurePutData() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public IContext dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public IContext combinerContext() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public IContext combiner(IExecutable combiner, IContext combinerContext) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void combine() {
				// TODO Auto-generated method stub

			}
		});
	}

}
