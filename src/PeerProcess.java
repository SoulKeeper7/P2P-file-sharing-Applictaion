import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class PeerProcess {

	private static final boolean doPrint = false;
	private static Logger LOGGER;
	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);

	public static int numberOfPreferredNeighbors;
	public static int unchokingInterval;
	public static int optimisticUnchokingInterval;
	public static String fileName;
	public static int fileSize;
	public static int pieceSize;
	public static int noOfPieces;

	private static Map<String, String> peerInfoMap;
	private static List<PeerManager> remotePeerInfoList;

	private static String peerProcessPeerId;
	private static int peerProcessListenCount = 0;
	private static AtomicBoolean hasFile = new AtomicBoolean(false);

	static {
		loadPeerCommonConfig();
		loadPeerInfoConfig();
		remotePeerInfoList = Collections.synchronizedList(new ArrayList<>());
	}

	public static void loadPeerCommonConfig() {
		try {
			final BufferedReader bufferedReader = new BufferedReader(new FileReader("Common.cfg"));
			numberOfPreferredNeighbors = Integer.parseInt(bufferedReader.readLine().split("\\s+")[1]);
			unchokingInterval = Integer.parseInt(bufferedReader.readLine().split("\\s+")[1]);
			optimisticUnchokingInterval = Integer.parseInt(bufferedReader.readLine().split("\\s+")[1]);
			fileName = bufferedReader.readLine().split("\\s+")[1];
			fileSize = Integer.parseInt(bufferedReader.readLine().split("\\s+")[1]);
			pieceSize = Integer.parseInt(bufferedReader.readLine().split("\\s+")[1]);
			noOfPieces = (int) Math.ceil(fileSize / (double) pieceSize);
			bufferedReader.close();
		}
		catch (final IOException e) {
			e.printStackTrace();
		}
	}

	public static void loadPeerInfoConfig() {
		peerInfoMap = new HashMap<>();
		try {
			final BufferedReader bufferedReader = new BufferedReader(new FileReader("PeerInfo.cfg"));
			String line;
			while (( line = bufferedReader.readLine() ) != null) {
				final String[] parts = line.split("\\s+", 2);
				peerInfoMap.put(parts[0], parts[1]);
			}
			bufferedReader.close();
		}
		catch (final IOException e) {
			e.printStackTrace();
		}
	}

	public static void loadPeerProcessBitfield() {
		PeerManager.setPeerProcessPeerId(peerProcessPeerId);
		final byte[] fileData = new byte[fileSize];
		final BitSet bitSet = new BitSet();

		final StringBuilder fileNameBuilder = new StringBuilder("peer_");
		fileNameBuilder.append(peerProcessPeerId).append(File.separator).append(fileName);
		fileName = fileNameBuilder.toString();

		final String[] peerInfo = peerInfoMap.get(peerProcessPeerId).split("\\s+");
		final String hasFileOrNot = peerInfo[2];

		if (hasFileOrNot.equals("1")) {
			hasFile.set(true);
			final File file = new File(fileName);
			if (file.exists() && !file.isDirectory()) {
				try {
					final BufferedInputStream bufferedInputStream = new BufferedInputStream(
							new FileInputStream(fileName));
					bufferedInputStream.read(fileData);
					bufferedInputStream.close();
					bitSet.set(0, noOfPieces);
				}
				catch (final IOException e) {
					e.printStackTrace();
				}
			}
			else {
				throw new RuntimeException("File Not Found, hasFileOrNot bit was 1");
			}
		}
		else {
			final File file = new File(fileName);
			try {
				file.delete();
			}
			catch (final Exception e) {

			}
			try {
				file.createNewFile();
			}
			catch (final IOException e) {
				e.printStackTrace();
			}
		}

		PeerManager.setPeerProcessFile(fileData);
		PeerManager.setPeerProcessBitField(bitSet);
	}

	public void createDirectory() {
		new File("peer_" + peerProcessPeerId).mkdir();
	}

	public void peerClientConnect() {
		for (final String peerId : peerInfoMap.keySet()) {
			if (peerId.compareTo(peerProcessPeerId) < 0) {
				final String[] peerInfo = peerInfoMap.get(peerId).split("\\s+");
				final String peerAddress = peerInfo[0];
				final String peerPort = peerInfo[1];
				try {
					print("Trying to connect");
					final Socket socket = new Socket(peerAddress, Integer.parseInt(peerPort));
					final PeerManager peerManager = new PeerManager(peerId, socket, true);
					peerManager.start();
					remotePeerInfoList.add(peerManager);
				}
				catch (final NumberFormatException e) {
					e.printStackTrace();
				}
				catch (final UnknownHostException e) {
					e.printStackTrace();
				}
				catch (final IOException e) {
					e.printStackTrace();
				}
			}
			else if (peerId.compareTo(peerProcessPeerId) > 0) {
				peerProcessListenCount++;
			}
		}
	}

	public void acceptClientConnections() {
		final Thread acceptClientConnectionsThread = new Thread() {

			@Override
			public void run() {
				final String peerPort = peerInfoMap.get(peerProcessPeerId).split("\\s+")[1];
				try (final ServerSocket serverSocket = new ServerSocket(Integer.parseInt(peerPort))) {
					while (peerProcessListenCount > 0) {
						print("Waiting to accept");
						final Socket acceptSocket = serverSocket.accept();
						print("Accepted");
						if (acceptSocket != null) {
							final PeerManager peerManager = new PeerManager(null, acceptSocket, false);
							peerManager.start();
							remotePeerInfoList.add(peerManager);
							peerProcessListenCount--;
						}
					}
				}
				catch (final NumberFormatException e) {
					e.printStackTrace();
				}
				catch (final IOException e) {
					e.printStackTrace();
				}
			}
		};

		acceptClientConnectionsThread
				.setName("Thread - Accept Client Connections - peerID [" + peerProcessPeerId + "]: ");
		acceptClientConnectionsThread.start();
	}

	public void determinePreferredNeighbours() {
		final Runnable preferredNeighboursRunnable = new Runnable() {

			@Override
			public void run() {
				final StringBuilder preferredNeighboursPeerId = new StringBuilder();
				int k = numberOfPreferredNeighbors;

				synchronized (PeerManager.getInterestedNeighbors()) {
					Iterator<RemotePeerInfo> iterator = PeerManager.getInterestedNeighbors().iterator();
					if (hasFile.get()) {
						final List<RemotePeerInfo> list = new ArrayList<>();
						while (iterator.hasNext()) {
							list.add(iterator.next());
						}
						Collections.shuffle(list);
						iterator = list.iterator();
					}

					while (iterator.hasNext()) {
						final RemotePeerInfo remotePeerInfo = iterator.next();
						if (k > 0) {
							PeerManager.getPreferredNeighbors().add(remotePeerInfo);
							k--;
							if (remotePeerInfo.isChoked()) {
								remotePeerInfo.setChoked(false);
								if (!remotePeerInfo.isOptimisticallyUnchoked()) {
									try {
										remotePeerInfo.sendActualMessage(Constants.ActualMessageType.UNCHOKE.ordinal(),
												null);
									}
									catch (final IOException e) {
										e.printStackTrace();
									}
								}
							}
							preferredNeighboursPeerId.append(remotePeerInfo.getPeerId()).append(",");
						}
						else {
							PeerManager.getPreferredNeighbors().remove(remotePeerInfo);
							if (!remotePeerInfo.isChoked()) {
								remotePeerInfo.setChoked(true);
								// We won't send choke message if the peer is optimistically unchoked
								if (!remotePeerInfo.isOptimisticallyUnchoked()) {
									try {
										remotePeerInfo.sendActualMessage(Constants.ActualMessageType.CHOKE.ordinal(),
												null);
									}
									catch (final IOException e) {
										e.printStackTrace();
									}
								}
							}
						}
					}
				}

				if (!preferredNeighboursPeerId.toString().isEmpty()) {
					preferredNeighboursPeerId.deleteCharAt(preferredNeighboursPeerId.length() - 1);
					LOGGER.info("Peer [peer_ID " + peerProcessPeerId + "] has the preferred neighbors ["
							+ preferredNeighboursPeerId.toString() + "]");
				}
			}
		};
		scheduler.scheduleAtFixedRate(preferredNeighboursRunnable, unchokingInterval, unchokingInterval,
				TimeUnit.SECONDS);
	}

	public void determineOptimisticallyUnchokedNeighbour() {
		final Runnable optimisticallyUnchokedNeighbourRunnable = new Runnable() {

			@Override
			public void run() {
				if (PeerManager.getPreferredNeighbors().size() <= numberOfPreferredNeighbors) {
					return;
				}
				final List<RemotePeerInfo> chokeList = PeerManager.getInterestedNeighbors().stream()
						.filter(peer -> !PeerManager.getPreferredNeighbors().contains(peer))
						.collect(Collectors.toList());
				try {
					final RemotePeerInfo remotePeerInfo = chokeList.get(new Random().nextInt(chokeList.size()));
					if (remotePeerInfo != PeerManager.getOptimisticallyUnchokedNeighbor()) {
						if (PeerManager.getOptimisticallyUnchokedNeighbor() != null) {
							PeerManager.getOptimisticallyUnchokedNeighbor().setOptimisticallyUnchoked(false);
							if (!PeerManager.getPreferredNeighbors()
									.contains(PeerManager.getOptimisticallyUnchokedNeighbor())) {
								PeerManager.getOptimisticallyUnchokedNeighbor()
										.sendActualMessage(Constants.ActualMessageType.CHOKE.ordinal(), null);
							}
						}
						PeerManager.setOptimisticallyUnchokedNeighbor(remotePeerInfo);
						remotePeerInfo.setOptimisticallyUnchoked(true);
						remotePeerInfo.sendActualMessage(Constants.ActualMessageType.UNCHOKE.ordinal(), null);
						LOGGER.info(
								"Peer [peer_ID " + peerProcessPeerId + "] has the optimistically unchoked neighbor ["
										+ PeerManager.getOptimisticallyUnchokedNeighbor().getPeerId() + "]");
					}
				}
				catch (final IllegalArgumentException e) {
					// This implies that choke list size is 0.
					PeerManager.getOptimisticallyUnchokedNeighbor().setOptimisticallyUnchoked(false);
					PeerManager.setOptimisticallyUnchokedNeighbor(null);
				}
				catch (final IOException e) {
					e.printStackTrace();
				}
			}
		};

		scheduler.scheduleAtFixedRate(optimisticallyUnchokedNeighbourRunnable, optimisticUnchokingInterval,
				optimisticUnchokingInterval, TimeUnit.SECONDS);
	}

	public void determineShutDownProcess() {
		final Runnable shutDownProcessRunnable = new Runnable() {

			@Override
			public void run() {
				if (hasFile.get() && peerProcessListenCount <= 0) {
					boolean shutDown = true;
					synchronized (PeerProcess.getRemotePeerInfoList()) {
						final Iterator<PeerManager> iterator = PeerProcess.getRemotePeerInfoList().iterator();
						while (iterator.hasNext()) {
							final PeerManager peerManager = iterator.next();
							if (peerManager.getConnectedPeer().getBitField().cardinality() != noOfPieces) { 
								if(peerManager.isTerminated())
								{
									shutDown = true;
									break;
								}
								shutDown=false;
								break;
							} else {
								if(peerManager.isTerminated())
								{
									shutDown = true;
									break;
								}
							}
						}
					}

					if (shutDown) {
						synchronized (PeerProcess.getRemotePeerInfoList()) {
							final Iterator<PeerManager> iterator = PeerProcess.getRemotePeerInfoList().iterator();
							while (iterator.hasNext()) {
								final PeerManager peerManager = iterator.next();

								// Close Socket
								try {
									peerManager.setTerminated(true);
									peerManager.getConnectedPeer()
											.sendActualMessage(Constants.ActualMessageType.TERMINATE.ordinal(), null);
									peerManager.getConnectedPeer()
											.sendActualMessage(Constants.ActualMessageType.TERMINATE.ordinal(), null);
									peerManager.getConnectedPeer().getSocket().close();
								}
								catch (final IOException e) {
									//	e.printStackTrace();
								}

								// Terminate threads
								peerManager.interrupt();
							}
						}

						// Shutdown Scheduler
						scheduler.shutdown();
						if (!scheduler.isShutdown()) {
							scheduler.shutdownNow();
						}
						try {
							scheduler.awaitTermination(5, TimeUnit.SECONDS);
						}
						catch (final InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
		};

		scheduler.scheduleAtFixedRate(shutDownProcessRunnable, 5, 5, TimeUnit.SECONDS);
	}

	private static void print(String string) {
		if (doPrint) {
			System.out.println(string);
		}
	}

	public static String getFileName() {
		return fileName;
	}

	public static int getNoOfPieces() {
		return noOfPieces;
	}

	public static int getPieceSize() {
		return pieceSize;
	}

	public static List<PeerManager> getRemotePeerInfoList() {
		return remotePeerInfoList;
	}

	public static void setHasFile(boolean hasFile) {
		PeerProcess.hasFile.compareAndSet(false, true);
	}

	public static void main(String[] args) {

		final PeerProcess peerProcess = new PeerProcess();
		peerProcessPeerId = args[0];

		CustomLogFormatter.setupLogger(peerProcessPeerId);
		LOGGER = CustomLogFormatter.getLogger();
		loadPeerProcessBitfield();

		peerProcess.createDirectory();
		peerProcess.peerClientConnect();
		peerProcess.acceptClientConnections();

		peerProcess.determinePreferredNeighbours();
		peerProcess.determineOptimisticallyUnchokedNeighbour();
		peerProcess.determineShutDownProcess();
	}

}
