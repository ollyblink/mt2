package net.tomp2p.mapreduce.utils;

import java.util.HashMap;
import java.util.Map;

public class ByteClassLoader extends ClassLoader {
	private final Map<String, byte[]> extraClassDefs;

	public ByteClassLoader(Map<String, byte[]> extraClassDefs) { 
		this.extraClassDefs = new HashMap<String, byte[]>(extraClassDefs);
	}

	@Override
	protected Class<?> findClass(final String name) throws ClassNotFoundException {
		byte[] classBytes = this.extraClassDefs.remove(name);
		if (classBytes != null) {
			return defineClass(name, classBytes, 0, classBytes.length);
		}
		return super.findClass(name);
	}

}
