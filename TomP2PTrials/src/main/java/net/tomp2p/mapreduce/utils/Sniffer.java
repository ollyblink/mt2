package net.tomp2p.mapreduce.utils;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author jspamdetection team
 */
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jnetpcap.JBufferHandler;
import org.jnetpcap.Pcap;
import org.jnetpcap.PcapBpfProgram;
import org.jnetpcap.PcapHeader;
import org.jnetpcap.PcapIf;
import org.jnetpcap.nio.JBuffer;
import org.jnetpcap.nio.JMemory;
import org.jnetpcap.packet.PcapPacket;
import org.jnetpcap.protocol.lan.Ethernet;
import org.jnetpcap.protocol.tcpip.Tcp;

/**
 *
 * @author LVT
 */
public class Sniffer {
	public static List<Integer> allVals = Collections.synchronizedList(new ArrayList<>());

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) {
		// TODO code application logic here
		int snaplen = 64 * 2048;
		int promicious = Pcap.MODE_NON_PROMISCUOUS;
		int timeout = 10 * 1000;
		boolean capturing = true;
		PcapBpfProgram filter = new PcapBpfProgram();
		String expression = "tcp port 4004";
		int optimize = 0; // 0 = false
		int netmask = 0xFFFFFF00; // 255.255.255.0\
		// tao 1 list de chua cac card mang
		List<PcapIf> alldevice = new ArrayList<PcapIf>();
		// List<String> ans = new ArrayList<String>();
		StringBuilder err = new StringBuilder();
		// goi phuong thuc findalldevs de tim cac card mang
		int r = Pcap.findAllDevs(alldevice, err);
		// System.out.println(r);
		System.err.println("ALL DEVICES: "+alldevice);

		if (r == Pcap.NOT_OK || alldevice.isEmpty()) {
			System.err.printf("no devices found?", err.toString());
			return;
		}
		/*
		 * for (PcapIf device : alldevice) { System.out.println(device.getName()); System.out.println(device.getDescription());
		 * 
		 * }
		 */
		for (int i = 0; i < alldevice.size(); i++) {
			System.out.println("#" + i + ": " + alldevice.get(i).getDescription());

		}

		InputStreamReader isr = new InputStreamReader(System.in);
		// BufferedReader br = new BufferedReader(isr);
		int I = 0;
		// try {
		// String l = br.readLine();
		// I = new Integer(l).intValue();
		//
		// } catch (Exception e) {
		// System.out.println(e);
		// }

		// Integer i = Integer.valueOf();
		PcapIf netInterface = alldevice.get(I);
 		// open nic to capture packet
		// tao dispatcher

		while (capturing) {
			Pcap pcap = Pcap.openLive(netInterface.getName(), snaplen, promicious, timeout, err);

			// compile filter

			if (pcap == null) {
				System.out.println("khong the mo card mang");
			}
			if (pcap.compile(filter, expression, optimize, netmask) != Pcap.OK) {
				System.out.println(pcap.getErr());
				return;
			}
			// gan filter vao pcap
			pcap.setFilter(filter);
			// vong loop bat goi tin
			List<Integer> vals = Collections.synchronizedList(new ArrayList<>());
			JBufferHandler<String> printSummaryHandler = new JBufferHandler<String>() {
				Tcp tcp = new Tcp();
				InetAddress dest_ip;
				InetAddress sour_ip;
				final PcapPacket packet = new PcapPacket(JMemory.POINTER);

				public void nextPacket(PcapHeader header, JBuffer buffer, String user) {
					Timestamp timestamp = new Timestamp(header.timestampInMillis());
					packet.peer(buffer);
					packet.getCaptureHeader().peerTo(header, 0);
					packet.scan(Ethernet.ID);
					// packet.getHeader(tcp);

					if (packet.hasHeader(tcp)) {
						int length = tcp.getPrefixLength() + tcp.getHeaderLength() + tcp.getGapLength() + tcp.getPayloadLength() + tcp.getPostfixLength();

						// System.out.println("LENGTH: " + packet.getTotalSize());

						allVals.add(length);
						// long sum = 0;
						// for (Integer i : allVals) {
						// sum += i;
						// }
						// System.err.println("SUM: " + sum);
						// String str = "" + tcp.destination();
						// System.out.printf("tcp.dst_port=%d%n", tcp.destination());
						// System.out.printf("tcp.src_port=%d%n", tcp.source());
						// // System.out.printf("tcp.ack=%x%n", tcp.ack());
						// System.out.println("tcp.length=" + tcp.getPayload().length);
					}

				}
			};
			// dat vong loop la 10 packet
			pcap.loop(Pcap.LOOP_INFINITE, printSummaryHandler, "jNetPcap rocks!");

			// tao file luu .pcap
			// StringBuilder errbuf = new StringBuilder();
			// String fname = "test/test-afs.pcap";
			// Pcap pcap1 = Pcap.openOffline(fname, errbuf);
			// String ofile = "tmp-capture-file.cap";
			// PcapDumper dumper = pcap1.dumpOpen(ofile); // output file
			// JBufferHandler<PcapDumper> dumpHandler = new JBufferHandler<PcapDumper>() {
			//
			// public void nextPacket(PcapHeader header, JBuffer buffer, PcapDumper dumper) {
			//
			// dumper.dump(header, buffer);
			// }
			// };
			// pcap.loop(10, dumpHandler, dumper); // Special native dumper call to loop
			//
			// File file = new File(ofile);
			// System.out.printf("%s file has %d bytes in it!\n", ofile, file.length());
			//
			// dumper.close();
			// end code tao file luu
			// dong pcap
			pcap.close();
		}

	}

	public static String getHexString(byte[] b) {
		String result = "";
		for (int i = 0; i < b.length; i++) {
			result += Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1);
		}
		return result;
	}

}
