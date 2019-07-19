package cn.thinkingdata.kafka.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class CommonUtils {

	private static Integer roundRobinCount = 0;

	private static final Logger logger = LoggerFactory
			.getLogger(CommonUtils.class);

	public static <T> T roundRobin(List<T> list) {
		if (list.size() == 0) {
			roundRobinCount = 0;
			return null;
		}
		if (roundRobinCount == list.size()) {
			roundRobinCount = 0;
		}
		T t = list.get(roundRobinCount);
		roundRobinCount++;
		return t;
	}

	public static String getHostNameForLiunx() {
		try {
			return (InetAddress.getLocalHost()).getHostName();
		} catch (UnknownHostException uhe) {
			String host = uhe.getMessage(); // host = "hostname: hostname"
			if (host != null) {
				int colon = host.indexOf(':');
				if (colon > 0) {
					return host.substring(0, colon);
				}
			}
			return "UnknownHost";
		}
	}

	public static String getHostName() {
		if (System.getenv("COMPUTERNAME") != null) {
			return System.getenv("COMPUTERNAME");
		} else {
			return getHostNameForLiunx();
		}
	}
	
	public static String getStackTraceAsString(Throwable throwable) {
		StringWriter stringWriter = new StringWriter();
		throwable.printStackTrace(new PrintWriter(stringWriter));
		return stringWriter.toString();
	}

}
