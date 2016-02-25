package net.tomp2p.mapreduce.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import net.tomp2p.mapreduce.Test;

public class SerializeUtils {

	public static Map<String, byte[]> serialize(Class<?> classToSerialize) throws IOException {
		Map<String, byte[]> bytesForClasses = new HashMap<>();
		byte[] byteArray = toByteArray(classToSerialize.getName());
		bytesForClasses.put(classToSerialize.getName(), byteArray);
		findAnonymousClasses(bytesForClasses, classToSerialize.getName());
		// Get all declared inner classes, interfaces, and so on.
		for (Class clazz : classToSerialize.getDeclaredClasses()) {
			// returns something like net.tomp2p.mapreduce.Job$InnerTestClass
			bytesForClasses.put(clazz.getName(), toByteArray(clazz.getName()));
			// Get all anonymous instantiations in this class clazz
			findAnonymousClasses(bytesForClasses, clazz.getName());
		}

		return bytesForClasses;
	}

	public static void main(String[] args) {
		Map<String, byte[]> visitor = new HashMap<>();
		findAnonymousClasses(visitor, Test2.class.getName());
		for (String name : visitor.keySet()) {
			System.out.println(name + " " + visitor.get(name));
		}
	}
	public class Test2 {
		Runnable r = new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				Runnable r = new Runnable() {

					@Override
					public void run() {
						// TODO Auto-generated method stub
						Runnable r = new Runnable() {

							@Override
							public void run() {
								// TODO Auto-generated method stub
								Runnable r = new Runnable() {

									@Override
									public void run() {
										// TODO Auto-generated method stub

									}
								};
								Runnable r2 = new Runnable() {

									@Override
									public void run() {
										// TODO Auto-generated method stub

									}
								};
								Runnable r3 = new Runnable() {

									@Override
									public void run() {
										// TODO Auto-generated method stub

									}
								};
							}
						};
					}
				};
			}
		};
		Runnable r2 = new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				Runnable r = new Runnable() {

					@Override
					public void run() {
						// TODO Auto-generated method stub

					}
				};
			}
		};
		Runnable r3 = new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub

			}
		};
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
				if (lastIndexOfPrevious$ == -1|| notFollowedByNumber(lastNumber)) {
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
		if(convertToNumber == null || convertToNumber.trim().length() == 0){
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

	public static Object deserialize(Map<String, byte[]> classesToDefine, String classToInstantiate) {
		Object o = null;
		// SerializeUtils.class.getClassLoader().
		ByteClassLoader l = new ByteClassLoader(classesToDefine);
		// Map<String, Class<?>> classes = new HashMap<>();
		for (String className : classesToDefine.keySet()) {
			try {
				Class<?> c = l.findClass(className);
				// classes.put(className, c);
				if (c.getName().equals(classToInstantiate)) {
					try {
						o = Class.forName(classToInstantiate).newInstance();
					} catch (InstantiationException | IllegalAccessException e) {
						e.printStackTrace();
					}
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return o;
	}

	public static List<Class<?>> getClassNamesFromJar(String pathToJar) {
		List<Class<?>> classes = new ArrayList<>();
		try {
			JarFile jarFile = new JarFile(pathToJar);
			Enumeration e = jarFile.entries();

			URL[] urls = { new URL("jar:file:" + pathToJar + "!/") };
			URLClassLoader cl = URLClassLoader.newInstance(urls);

			while (e.hasMoreElements()) {
				JarEntry je = (JarEntry) e.nextElement();
				if (je.isDirectory() || !je.getName().endsWith(".class")) {
					continue;
				}
				// -6 because of .class
				String className = je.getName().substring(0, je.getName().length() - 6);
				className = className.replace('/', '.');
				Class<?> c = cl.loadClass(className);
				classes.add(c);
			}
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
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
