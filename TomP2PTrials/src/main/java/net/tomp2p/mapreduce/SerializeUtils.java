package net.tomp2p.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class SerializeUtils {
	public static void main(String[] args) throws IOException {
		SerializeUtils serializer = new SerializeUtils();
		// Job job = new Job();
 		NavigableMap<String, byte[]> serialize = serializer.serialize(Job.class);
		for (String s : serialize.keySet()) {
			byte[] bs = serialize.get(s);
			System.out.print(s +" ");
			for(Byte b:bs){
				System.out.print(b +",");
			}
			System.out.println();
		}
	}

	public NavigableMap<String, byte[]> serialize(Class<?> classToSerialize) {
		NavigableMap<String, byte[]> bytesForClasses = new TreeMap<>();
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

	private <T> byte[] toByteArray(Class<T> c) throws IOException {
		InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(c.getName() + ".class");
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
	
	public Object deserialize(byte[] toDeserialize){
		return toDeserialize;
		
	}
}
