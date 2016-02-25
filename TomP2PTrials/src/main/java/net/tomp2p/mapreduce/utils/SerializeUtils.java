package net.tomp2p.mapreduce.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import net.tomp2p.mapreduce.Job;
import net.tomp2p.mapreduce.Task;

public class SerializeUtils {
	public static void main(String[] args) throws IOException, IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		// SerializeUtils serializer = new SerializeUtils();
		// Job job = new Job();
		Map<String, byte[]> serialize = SerializeUtils.serialize(Job.class);
		for (String s : serialize.keySet()) {
			byte[] bs = serialize.get(s);
			System.out.print(s + " ");
			for (Byte b : bs) {
				System.out.print(b + ",");
			}
			System.out.println();
		}

		Map<String, Class<?>> classes = SerializeUtils.deserialize(serialize);
		for (String className : classes.keySet()) {

			try {
				Class<?> class1 = classes.get(className);
				if (class1.isAssignableFrom(Job.class)) {
					Job job = (Job) Class.forName(className).newInstance();
					System.out.println(job);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static Map<String, byte[]> serialize(Class<?> classToSerialize) {
		Map<String, byte[]> bytesForClasses = new HashMap<>();
		List<Class<?>> classesInPackage = ClassFinder.find(classToSerialize.getPackage().getName());

		for (Class<?> c : classesInPackage) {
			if (c.getSimpleName().startsWith(classToSerialize.getSimpleName())) {
				try {
					bytesForClasses.put(c.getName(), toByteArray(c));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return bytesForClasses;
	}

	private static byte[] toByteArray(Class<?> c) throws IOException {
		InputStream is = c.getClassLoader().getResourceAsStream(c.getName() + ".class");
		if (is == null) {
			is = c.getClassLoader().getResourceAsStream(c.getName().replace(".", "/") + ".class");
		}
		if (is == null) {
			is = c.getClassLoader().getResourceAsStream(c.getName().replace("\\", "/") + ".class");
		}
		if (is == null) {
			is = c.getClassLoader().getResourceAsStream(c.getName().replace(".", "\\") + ".class");
		}
		if (is == null) {
			is = c.getClassLoader().getResourceAsStream(c.getName().replace("/", "\\") + ".class");
		}
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();

		int nRead;
		byte[] data = new byte[16384];

		while ((nRead = is.read(data, 0, data.length)) != -1) {
			buffer.write(data, 0, nRead);
		}

		buffer.flush();

		return buffer.toByteArray();
	}

	public static Map<String, Class<?>> deserialize(Map<String, byte[]> classesToDefine) {
		ByteClassLoader l = new ByteClassLoader(classesToDefine);
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
}
