package net.tomp2p.mapreduce.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import net.tomp2p.mapreduce.Task;

public class SerializeUtils {

	public static Map<String, byte[]> serializeClassFile(Class<?> classToSerialize) throws IOException {
		Map<String, byte[]> visitor = new TreeMap<>();
		// Check if class is declared inside another class. If so, start serialization from parent
//		System.out.println("Class to serialize: " +classToSerialize.getName());
		while (classToSerialize.getDeclaringClass() != null) {
			classToSerialize = classToSerialize.getDeclaringClass();
//			System.out.println("Class to serialize: " +classToSerialize.getName());
		}
		// Serialize down the tree
		internalSerialize(classToSerialize, visitor);
		return visitor;
	}

	private static void internalSerialize(Class<?> classToSerialize, Map<String, byte[]> visitor) {
		byte[] byteArray = toByteArray(classToSerialize.getName());
		visitor.put(classToSerialize.getName(), byteArray);
		findAnonymousClasses(visitor, classToSerialize.getName());
		// Get all declared inner classes, interfaces, and so on.
		for (Class<?> clazz : classToSerialize.getDeclaredClasses()) {
			internalSerialize(clazz, visitor);
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
					String count = classToSerializeName.substring(lastIndexOfPrevious$ + 1, classToSerializeName.length());
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
			// System.err.println(c);
			InputStream is = SerializeUtils.class.getResourceAsStream(c + ".class");
			if (is == null) {
				is = SerializeUtils.class.getResourceAsStream("/" + c.replace(".", "/") + ".class");
				// System.err.println("1" + is);
			}
			if (is == null) {
				is = SerializeUtils.class.getResourceAsStream(c.replace(".", "/") + ".class");
				// System.err.println("2" + is);
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

	public static Map<String, Class<?>> deserializeClassFiles(Map<String, byte[]> classesToDefine) {

		ByteClassLoader l = new ByteClassLoader(ClassLoader.getSystemClassLoader(), classesToDefine);
		Map<String, Class<?>> classes = new HashMap<>();
		for (String className : classesToDefine.keySet()) {
			try {
//				System.out.println("ClassName in deserialize: " + className);
				Class<?> c = l.findClass(className);
//				System.out.println("Class found is : " + c.getName());
				classes.put(className, c);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return classes;
	}

	public static Object deserializeJavaObject(byte[] objectData, Map<String, Class<?>> classes) {
		Object object = null;
		try {
			ByteObjectInputStream objectStream = new ByteObjectInputStream(new ByteArrayInputStream(objectData), classes);
			object = objectStream.readObject();
			objectStream.close();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		return object;
	}

	public static byte[] serializeJavaObject(Object object) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			ObjectOutput out = new ObjectOutputStream(bos);
			out.writeObject(object);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		byte[] objectInBytes = bos.toByteArray();
		return objectInBytes;
	}

}
