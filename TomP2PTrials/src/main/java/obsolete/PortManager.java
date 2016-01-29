package obsolete;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public enum PortManager {
	INSTANCE;
	private static final Random RND = new Random();
	private static final int SMALLEST_PORT = 1024;
	private static final int LARGEST_PORT = 49151;
	private static final int PORT_DELTA = LARGEST_PORT - SMALLEST_PORT;

	private Set<Integer> alreadyUsedPorts = new HashSet<>();

	public int generatePort() {
		int port = 0;
		do {
			port = SMALLEST_PORT + RND.nextInt(PORT_DELTA);
		} while (this.alreadyUsedPorts.contains(port));
		this.alreadyUsedPorts.add(port);
		return port;
	}

}
