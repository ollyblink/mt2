package net.tomp2p.mapreduce.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import net.tomp2p.mapreduce.TestClass;

public class SerializeUtils {

	public static Map<String, byte[]> serialize(Class<?> classToSerialize) throws IOException {
		Map<String, byte[]> visitor = new TreeMap<>();
		internalSerialize(classToSerialize, visitor);
		return visitor;
	}

	private static void internalSerialize(Class<?> classToSerialize, Map<String, byte[]> visitor) {
		byte[] byteArray = toByteArray(classToSerialize.getName());
		visitor.put(classToSerialize.getName(), byteArray);
		findAnonymousClasses(visitor, classToSerialize.getName());
		// Get all declared inner classes, interfaces, and so on.
		for (Class<?> clazz : classToSerialize.getDeclaredClasses()) {
			//
			// // returns something like net.tomp2p.mapreduce.Job$InnerTestClass
			// bytesForClasses.put(clazz.getName(),
			// toByteArray(clazz.getName()));
			// // Get all anonymous instantiations in this class clazz
			// findAnonymousClasses(bytesForClasses, clazz.getName());
			internalSerialize(clazz, visitor);
		}

	}

	public static void main(String[] args) {
		Map<String, byte[]> visitor = new HashMap<>();
		findAnonymousClasses(visitor, TestClass.class.getName());
		for (String name : visitor.keySet()) {
			System.out.println(name + " " + visitor.get(name));
		}
	}

	protected static void findAnonymousClasses(Map<String, byte[]> visitor, String classToSerializeName) {

		int lastIndexOf$ = classToSerializeName.lastIndexOf("$");
		String lastNumber = classToSerializeName.substring(lastIndexOf$ + 1, classToSerializeName.length());
		if (lastIndexOf$ == -1 || notFollowedByNumber(lastNumber)) {
			// This is the initial class name .
			classToSerializeName = classToSerializeName + "$" + 1;
			findAnonymousClasses(visitor, classToSerializeName);
		} else {
			// increment the class name and look for the next class
			byte[] byteArray = toByteArray(classToSerializeName);
			if (byteArray == null) {
				classToSerializeName = classToSerializeName.substring(0, classToSerializeName.lastIndexOf("$"));

				int lastIndexOfPrevious$ = classToSerializeName.lastIndexOf("$");
				lastNumber = classToSerializeName.substring(lastIndexOfPrevious$ + 1, classToSerializeName.length());
				if (lastIndexOfPrevious$ == -1 || notFollowedByNumber(lastNumber)) {
					return; // Back at initial classpath
				} else {
					String count = classToSerializeName.substring(lastIndexOfPrevious$ + 1,
							classToSerializeName.length());
					int newCounter = Integer.parseInt(count);
					++newCounter; // Increment it for the next round
					classToSerializeName = classToSerializeName.substring(0, lastIndexOfPrevious$ + 1) + newCounter;
					findAnonymousClasses(visitor, classToSerializeName);
				}
			} else {
				visitor.put(classToSerializeName, byteArray);
				classToSerializeName = classToSerializeName + "$" + 1;
				findAnonymousClasses(visitor, classToSerializeName);
			}
		}

	}

	private static boolean notFollowedByNumber(String convertToNumber) {
		if (convertToNumber == null || convertToNumber.trim().length() == 0) {
			return true;
		}
		try {
			Integer.parseInt(convertToNumber);
			return false;
		} catch (NumberFormatException e) {
			return true;
		}
	}

	private static byte[] toByteArray(String c) {
		try {
			// c.getName looks lik this: mapreduce.execution.jobs.Job
			System.err.println(c);
			InputStream is = SerializeUtils.class.getResourceAsStream(c + ".class");
			if (is == null) {
				is = SerializeUtils.class.getResourceAsStream("/" + c.replace(".", "/") + ".class");
				System.err.println("1" + is);
			}
			if (is == null) {
				is = SerializeUtils.class.getResourceAsStream(c.replace(".", "/") + ".class");
				System.err.println("2" + is);
			}

			if (is == null) {
				return null;
			}

			ByteArrayOutputStream buffer = new ByteArrayOutputStream();

			int nRead;
			byte[] data = new byte[16384];

			while ((nRead = is.read(data, 0, data.length)) != -1) {
				buffer.write(data, 0, nRead);
			}

			buffer.flush();

			return buffer.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static Map<String, Class<?>> deserialize(Map<String, byte[]> classesToDefine) {

		// SerializeUtils.class.getClassLoader().
		ByteClassLoader l = new ByteClassLoader(classesToDefine); //TODO this may be a problem
		Thread.currentThread().setContextClassLoader(l);
		Map<String, Class<?>> classes = new HashMap<>();
		for (String className : classesToDefine.keySet()) {
			try {
				Class<?> c = l.findClass(className);
				classes.put(className, c);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return classes;
	}

	// public static void main(String[] args) throws IOException {
	// // List<Class<?>> classNamesFromJar =
	// // SerializeUtils.getClassNamesFromJar(
	// //
	// "/home/ozihler/git/mt2/TomP2PTrials/src/test/java/net/tomp2p/mapreduce/trialjar.jar");
	// // for (Class<?> c : classNamesFromJar) {
	// Map<String, byte[]> serialize =
	// SerializeUtils.serialize(net.tomp2p.mapreduce.Job.class);
	// Object deserialize = SerializeUtils.deserialize(serialize,
	// net.tomp2p.mapreduce.Job.class.getName());
	// System.out.println(deserialize);
	// // }
	// }

}
