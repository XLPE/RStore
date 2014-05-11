/**
 * 2011 HStreaming LLC
 *
 * A stream generator streaming files to TCP and UDP endpoints.
 */
package DataCubeRefresh;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A stream generator streaming files to TCP and UDP endpoints.
 */
public class StreamGenerator {

	/**
	 * Inner class representing a stream generator instance.
	 */
	static class StreamGen {

		int bufferSize = 4096;
		int waitTimeMsec = 0;
		int waitTimeCount = 0;

		SocketAddress remoteAddress;
		SocketAddress localAddress;

		ArrayList fileObjects;
		boolean gzipped;

		SocketChannel tcpChannel;
		ServerSocketChannel tcpServerChannel;

		LoadPartTable update = new LoadPartTable();

		boolean tcpChannelClosed = true;
		boolean tcpServerChannelClosed = true;

		/** EOF byte. */
		public static final int EOF = 26;

		static Selector selector;
		static ByteBuffer inBuffer = ByteBuffer.allocate(512);
		static LinkedList<StreamGen> streamGens = new LinkedList<StreamGen>();
		private int count;
		private int index;
		private int numOfKeys;
		private int numOfUpdate;
		private int numOfUpdatedKeys;

		/**
		 * Helper function to wait a <code>waitTimeMsec</code> milliseconds
		 * after each <code>waitTimeCount</code>-th object/line/buffer.
		 * 
		 */
		private void waitTime() {
			if (waitTimeMsec > 0 && count++ % waitTimeCount == 0)
				try {
					Thread.sleep(waitTimeMsec);
				} catch (InterruptedException i) {
				}
		}

		/**
		 * A thread for streaming to a connected TCP endpoint.
		 */
		class TCPThread extends Thread {
			private SocketChannel tcpChannel;
			private byte[] buffer;
			int index = 0;
			int numOfUpdate = 0;
			int numOfKeys = 0;
			int currentUpdate = 0;
			int numOfUpdatedKeys = 0;
			List<Integer> keyList = new ArrayList();
			Random keyRand = new Random();
			private long cur;
			Configuration config;
			HTable table;

			/**
			 * Constructor.
			 * 
			 * @param tcpChannel
			 *            socket channel
			 * @throws IOException
			 *             if an input or output exception occurred
			 */
			public TCPThread(SocketChannel tcpChannel, int index,
					int numOfKeys, int numOfUpdate, int numOfUpdatedKeys)
					throws IOException {
				super("TCPThread");
				this.tcpChannel = tcpChannel;
				this.index = index;
				this.numOfKeys = numOfKeys;
				this.numOfUpdate = numOfUpdate;
				this.numOfUpdatedKeys = numOfUpdatedKeys;
				try{
				    config = HBaseConfiguration.create();
				    table = new HTable(config, "part");
				}
				catch(Exception e){
					System.out.println(e.getMessage());
				}
				int maxKey = (index + 1) * numOfKeys;
				for (int i = 0; i < numOfUpdatedKeys; i++) {
					int curKey = 1 + keyRand.nextInt() % maxKey;
					keyList.add(curKey);
				}
			}

			/**
			 * Run stream writer thread.
			 */
			@Override
			public void run() {
				try {
					while (true) {
						String line;
						while ((line = getNextRow()) != null) {
							StringBuilder st = new StringBuilder();
							st.append(line);
							st.append("\n");
							tcpChannel.write(ByteBuffer.wrap(st.toString()
									.getBytes()));
							waitTime();
						}
					}
				} catch (IOException e) {
					System.out.println("TCP channel disconnect: " + e);
				}

				try {
					System.out.println("TCP channel close");
					tcpChannelClosed = true;
					tcpChannel.socket().close();
					restart();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			private String getNextRow() {
				int printout = numOfKeys / 100;
				if (currentUpdate < numOfKeys) { // inital value of each item
					int key = index * numOfKeys + currentUpdate;
					String keyStr = key + "";
					String valueStr = update.generatePartValue();
					String tuple = keyStr + "@" + valueStr;
					if (currentUpdate % printout == 0) {
						long pre = cur;
						cur = System.currentTimeMillis();
						System.out.println("percentage: " + currentUpdate
								/ printout);
						System.out.println("transaction per sec: " + printout
								/ ((cur - pre) / 1000));
					}
					insertIntoHbase(key, valueStr);
					currentUpdate++;
					return tuple;
				} else { // update to the existing value
					int key = keyList.get(keyRand.nextInt(numOfUpdatedKeys));
					String keyStr = key + "";
					String valueStr = update.generatePartValue();
					String tuple = keyStr + "|" + valueStr;
					insertIntoHbase(key, valueStr);
					currentUpdate++;
					return tuple;
				}
			}

			private void insertIntoHbase(int key, String valueStr) {
				Put p = new Put(Bytes.toBytes(key + ""));
				p.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"),
						Bytes.toBytes(valueStr));
				try {
					table.put(p);
				} catch (Exception e) {
						
				}
			}
		}

		/**
		 * Restart closed TCP or UDP channels.
		 * 
		 * @throws IOException
		 *             if an input or output exception occurred
		 */
		public void restart() throws IOException {
			boolean wakeupSelector = false;

			if (selector == null)
				selector = Selector.open();

			if (tcpServerChannelClosed) {
				System.out.println("TCP server channel start");
				tcpServerChannel = ServerSocketChannel.open();
				tcpServerChannel.socket().bind(localAddress);
				tcpServerChannel.configureBlocking(false);
				tcpServerChannel.register(selector, SelectionKey.OP_ACCEPT,
						this);
				tcpServerChannelClosed = false;
				wakeupSelector = true;
			}
			if (wakeupSelector)
				selector.wakeup();
		}

		public void handleSelect(Channel channel, SelectionKey key)
				throws IOException {

			if (selector == null)
				selector = Selector.open();

			if (channel == tcpServerChannel) {
				if (key.isAcceptable()) {
					System.out.println("TCP server channel accept");
					new TCPThread(tcpServerChannel.accept(), index, numOfKeys,
							numOfUpdate, numOfUpdatedKeys).start();
				}
			}
		}

		public StreamGen(SocketAddress address, int waitTimeMsec,
				int waitTimeCount, int index, int numOfKeys, int numOfUpdate, int numOfUpdatedKeys)
				throws IOException {

			this.localAddress = address;
			this.waitTimeMsec = waitTimeMsec;
			this.waitTimeCount = waitTimeCount;
			this.index = index;
			this.numOfUpdate = numOfUpdate;
			this.numOfKeys = numOfKeys;
			this.numOfUpdate = numOfUpdate;
			this.numOfUpdatedKeys = numOfUpdatedKeys;

			streamGens.add(this);

			try {
				restart();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		/**
		 * Restart all closed TCP or UDP channels.
		 * 
		 * @throws IOException
		 *             if an input or output exception occurred
		 */
		public static void restartAll() throws IOException {
			for (StreamGen streamGen : streamGens)
				streamGen.restart();
		}

		/**
		 * Run the selector loop.
		 * 
		 * @throws IOException
		 *             if an input or output exception occurred
		 */
		static void runSelect() throws IOException {

			if (selector == null)
				selector = Selector.open();

			try {
				while (true) {

					int num = selector.select(1000);
					if (num == 0) {
						StreamGen.restartAll();
					} else {

						Set<SelectionKey> keys = selector.selectedKeys();

						// Iterate through the Set of keys.
						for (Iterator<SelectionKey> i = keys.iterator(); i
								.hasNext();) {
							SelectionKey key = i.next();
							i.remove();

							Channel channel = key.channel();
							StreamGen streamGen = (StreamGen) key.attachment();
							streamGen.handleSelect(channel, key);
						}
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * StreamGenerator main class.
	 * 
	 * @param args
	 *            arguments
	 * @throws Exception
	 *             if an exceptions occurs
	 */
	public static void main(String[] args) throws Exception {
		try {
			String host = args[0];
			int port = Integer.parseInt(args[1]);
			int waitTimeMsec = Integer.parseInt(args[2]);
			int waitTimeCount = Integer.parseInt(args[3]);
			int index = Integer.parseInt(args[4]);
			int numOfKeys = Integer.parseInt(args[5]);
			int numOfUpdate = Integer.parseInt(args[6]);
			float skewfactor = Float.parseFloat(args[7]);
			int numOfUpdatedKeys = (int)(numOfKeys*skewfactor);
			new StreamGen(new InetSocketAddress(host, port), waitTimeMsec,
					waitTimeCount, index, numOfKeys, numOfUpdate, numOfUpdatedKeys);
			StreamGen.runSelect();
		} catch (Exception e) {
			System.out.println(e);
			System.exit(0);
		}
	}
}
