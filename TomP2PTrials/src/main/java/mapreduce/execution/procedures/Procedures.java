package mapreduce.execution.procedures;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class Procedures {

	public static IExecutable convertJavascriptToJava(String javaScriptProcedure) {
		 ScriptEngineManager engineManager = new ScriptEngineManager();
		 ScriptEngine engine = engineManager.getEngineByName("nashorn");
		 try {
		 engine.eval(javaScriptProcedure);
		 } catch (ScriptException e) {
		 e.printStackTrace();
		 }
		 Invocable invocable = (Invocable) engine;
		 IExecutable procedure = invocable.getInterface(IExecutable.class);
		 return procedure;
//		return null;
	}

}
