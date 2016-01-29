package mapreduce.engine.executors.performance;
/**
 * 
 * @author Oliver
 *
 */
public   class PerformanceInfo {
	/** cpu speed per core, in MHz. Default is 100 MHz */
	private float cpuSpeed = 100;
	/** number of cores in cpu. Default is 1 core */
	private int nrOfCores = 1;
	/**
	 * Random Access Memory, in MB (meaning 1000^2 kB, where 1kB = 1000 bytes as it's usually described). 1000MB = 1GB. No multiple of 2! Multiple of
	 * 10. Default is 10 MB
	 */
	private int ram = 10;
	/** storage space in MB (see ram, again multiple of 10, 1GB = 1000 MB = 1 000 000 bytes). Default is 100 MB */
	private int storageSpace = 100;
	/** if storage space is ssd or hdd. true = ssd, false = hdd. Default is false */
	private boolean isSSD = false;
	// Additional things: e.g. network bandwidth etc.

	private PerformanceInfo() {

	}

	public static PerformanceInfo create() {
		return new PerformanceInfo();
	}

	public float cpuSpeed() {
		return this.cpuSpeed;
	}

	public int nrOfCores() {
		return this.nrOfCores;
	}

	public int ram() {
		return this.ram;
	}

	public int storageSpace() {

		return this.storageSpace;
	}

	public boolean isSSD() {
		return this.isSSD;
	}

	public PerformanceInfo cpuSpeed(float cpuSpeed) {
		this.cpuSpeed = cpuSpeed;
		return this;
	}

	public PerformanceInfo nrOfCores(int nrOfCores) {
		this.nrOfCores = nrOfCores;
		return this;
	}

	public PerformanceInfo ram(int ram) {
		this.ram = ram;
		return this;
	}

	public PerformanceInfo storageSpace(int storageSpace) {
		this.storageSpace = storageSpace;
		return this;
	}

	public PerformanceInfo isSSD(boolean isSSD) {
		this.isSSD = isSSD;
		return this;
	}
}
