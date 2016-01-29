package obsolete;

public class FormatUtils {

	public static boolean isCorrectIP4(String ip) {
		String[] ipParts = ip.split("\\."); 
		System.out.println(ipParts.length);
		if (ipParts.length != 4) {
			return false;
		} else {
			for (String part : ipParts) {
				try {
					if (part.length() > 3) {
						return false;
					}
					Integer.parseInt(part);
				} catch (NumberFormatException e) {
					return false;
				}
			}
			return true;
		}
	}

	public static boolean isCorrectPort(String port) {
		try {
			Integer.parseInt(port);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}

}
