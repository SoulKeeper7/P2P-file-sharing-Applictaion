import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

public class PeerManager extends Thread {

	private static final boolean doPrint = false;
	private static final Logger LOGGER = CustomLogFormatter.getLogger();

	private static String peerProcessPeerId;
	private static BitSet peerProcessBitField;
	private static byte[] peerProcessFile;

	private static Set<RemotePeerInfo> interestedNeighbors = Collections.synchronizedSet(new TreeSet<>());
	private static Set<RemotePeerInfo> preferredNeighbors = Collections.synchronizedSet(new HashSet<>());
	private static RemotePeerInfo optimisticallyUnchokedNeighbor;

	private final RemotePeerInfo connectedPeer;
	private long startTime;
	private long endTime;
	private int pieceForTime;
	private boolean terminated;

	public PeerManager(String peerId, Socket socket, boolean isClient) throws IOException {
		this.connectedPeer = new RemotePeerInfo(peerId, socket);
		this.terminated = false;

		/*
		 * HandShaking
		 */
		if (isClient) {
			this.connectedPeer.sendHandShakeMessage(peerProcessPeerId);
			final HandShakeMessage receivedHandShakeMessage = this.connectedPeer.receiveHandShakeMessage();
			// this.print("Client Received HandShake Message - " + receivedHandShakeMessage.toString());

			assert Arrays.equals(receivedHandShakeMessage.getHeader(), Constants.HEADER_STRING.getBytes());
			assert Arrays.equals(receivedHandShakeMessage.getZeroBits(), new byte[10]);
			assert receivedHandShakeMessage.getPeerId().equals(this.connectedPeer.getPeerId());
		}
		else {
			final HandShakeMessage receivedHandShakeMessage = this.connectedPeer.receiveHandShakeMessage();
			// this.print("Server Received HandShake Message - " + receivedHandShakeMessage.toString());
			assert Arrays.equals(receivedHandShakeMessage.getHeader(), Constants.HEADER_STRING.getBytes());
			assert Arrays.equals(receivedHandShakeMessage.getZeroBits(), new byte[10]);
			final String receivedPeerId = receivedHandShakeMessage.getPeerId();
			this.connectedPeer.setPeerId(receivedPeerId);

			this.connectedPeer.sendHandShakeMessage(peerProcessPeerId);
		}

		this.setName("peer_ID " + this.connectedPeer.getPeerId());
		if (isClient) {
			LOGGER.info("Peer [peer_ID " + peerProcessPeerId + "] makes a connection to Peer [" + this.getName() + "]");
		}
		else {
			LOGGER.info("Peer [peer_ID " + peerProcessPeerId + "] is connected from Peer [" + this.getName() + "]");
		}
	}

	public void exchangeBitfields() throws IOException {
		/*
		 * Exchange BitFields
		 */
		byte[] bitFieldArray;
		synchronized (peerProcessBitField) {
			bitFieldArray = peerProcessBitField.toByteArray();
		}
		this.connectedPeer.sendActualMessage(Constants.ActualMessageType.BITFIELD.ordinal(), bitFieldArray);
		// this.print("My BitField in byte[] form: " + bitFieldArray);
		final ActualMessages receivedActualMessage = this.connectedPeer.receiveActualMessage();
		// this.print("Received BitField Message - " + receivedActualMessage.toString());
		if (receivedActualMessage.getMessageType() == Constants.ActualMessageType.BITFIELD) {
			if (receivedActualMessage.getMessagePayload() == null) {
				this.connectedPeer.setBitField(new BitSet());
			}
			else {
				this.connectedPeer.setBitField(BitSet.valueOf(receivedActualMessage.getMessagePayload()));
			}
			// this.print("Recieved BitField: " + this.connectedPeer.getBitField());
		}
		else {
		}

		/*
		 * Check and send Interested / Not-Interested
		 */
		if (this.getInterestedBit() != -1) {
			this.connectedPeer.sendActualMessage(Constants.ActualMessageType.INTERESTED.ordinal(), null);
		}
		else {
			this.connectedPeer.sendActualMessage(Constants.ActualMessageType.NOT_INTERESTED.ordinal(), null);
		}

	}

	@Override
	public void run() {
		try {
			this.exchangeBitfields();

			ActualMessages receivedActualMessage = null;
			while (!this.isTerminated()) {
				receivedActualMessage = this.connectedPeer.receiveActualMessage();
				// this.print("Actual Message: " + receivedActualMessage.toString());

				switch (receivedActualMessage.getMessageType()) {
					case CHOKE:
						assert receivedActualMessage.getMessagePayload() == null;
						LOGGER.info("Peer [peer_ID " + peerProcessPeerId + "] is choked by [" + this.getName() + "]");
						break;

					case UNCHOKE:
						assert receivedActualMessage.getMessagePayload() == null;
						int requestPieceIndex = this.getInterestedBit();
						if (requestPieceIndex == -1) {
							this.connectedPeer.sendActualMessage(Constants.ActualMessageType.NOT_INTERESTED.ordinal(),
									null);
						}
						else {
							final byte[] requestPayload = ByteBuffer.allocate(4).putInt(requestPieceIndex).array();
							this.connectedPeer.sendActualMessage(Constants.ActualMessageType.REQUEST.ordinal(),
									requestPayload);
						}
						LOGGER.info("Peer [peer_ID " + peerProcessPeerId + "] is unchoked by [" + this.getName() + "]");
						break;

					case INTERESTED:
						assert receivedActualMessage.getMessagePayload() == null;
						interestedNeighbors.add(this.connectedPeer);
						// this.print("Interested Neighbors: " + interestedNeighbors);
						LOGGER.info("Peer [peer_ID " + peerProcessPeerId
								+ "] received the 'interested' message from Peer [" + this.getName() + "]");
						break;

					case NOT_INTERESTED:
						assert receivedActualMessage.getMessagePayload() == null;
						interestedNeighbors.remove(this.connectedPeer);
						preferredNeighbors.remove(this.connectedPeer);
						this.connectedPeer.setChoked(true);
						// this.print("Interested Neighbors: " + interestedNeighbors);
						LOGGER.info("Peer [peer_ID " + peerProcessPeerId
								+ "] received the 'not interested' message from Peer [" + this.getName() + "]");
						break;

					case HAVE:
						assert receivedActualMessage.getMessagePayload().length == 4;
						int pieceIndex = ByteBuffer.wrap(receivedActualMessage.getMessagePayload()).getInt();
						assert pieceIndex >= 0;
						this.connectedPeer.setBitFieldIndex(pieceIndex);
						if (this.pieceForTime == pieceIndex) {
							this.endTime = System.currentTimeMillis();
							if (this.endTime == this.startTime) {
								this.endTime = this.startTime + 1;
							}
							int pieceLength = PeerProcess.getPieceSize();
							if (pieceIndex == PeerProcess.getNoOfPieces() - 1) {
								final int startIndex = pieceIndex * PeerProcess.getPieceSize();
								final int endIndex = peerProcessFile.length - 1;
								pieceLength = endIndex - startIndex + 1;
							}
							final long downloadRate = pieceLength / ( this.endTime - this.startTime );
							this.connectedPeer.setDownloadRate(downloadRate);
						}
						if (this.getInterestedBit() != -1) {
							this.connectedPeer.sendActualMessage(Constants.ActualMessageType.INTERESTED.ordinal(),
									null);
						}
						LOGGER.info("Peer [peer_ID " + peerProcessPeerId + "] received the 'have' message from Peer ["
								+ this.getName() + "] for the piece [" + pieceIndex + "]");
						break;

					case BITFIELD:
						// Should not reach here.
						// Sending BitField is done right after connection establishment
						break;

					case REQUEST:
						assert receivedActualMessage.getMessagePayload().length == 4;
						pieceIndex = ByteBuffer.wrap(receivedActualMessage.getMessagePayload()).getInt();
						if (preferredNeighbors.contains(this.connectedPeer)
								|| this.connectedPeer.equals(optimisticallyUnchokedNeighbor)) {
							final byte[] piecePayload = this.getPiece(pieceIndex);
							this.connectedPeer.sendActualMessage(Constants.ActualMessageType.PIECE.ordinal(),
									piecePayload);
							this.startTime = System.currentTimeMillis();
							this.pieceForTime = pieceIndex;
						}
						break;

					case PIECE:
						assert receivedActualMessage.getMessagePayload().length > 4;
						final byte[] piecePayload = new byte[receivedActualMessage.getMessagePayload().length - 4];

						final ByteBuffer byteBuffer = ByteBuffer.wrap(receivedActualMessage.getMessagePayload());
						pieceIndex = byteBuffer.getInt();
						assert pieceIndex >= 0;
						byteBuffer.get(piecePayload);
						this.putPiece(pieceIndex, piecePayload);
						synchronized (peerProcessBitField) {
							peerProcessBitField.set(pieceIndex);
						}
						LOGGER.info("Peer [peer_ID " + peerProcessPeerId + "] has downloaded the piece [" + pieceIndex
								+ "] from Peer [" + this.getName() + "]");

						// Notify others that I have this piece
						final byte[] havePayload = ByteBuffer.allocate(4).putInt(pieceIndex).array();
						synchronized (PeerProcess.getRemotePeerInfoList()) {
							final Iterator<PeerManager> iterator = PeerProcess.getRemotePeerInfoList().iterator();
							while (iterator.hasNext()) {
								final PeerManager peerManager = iterator.next();
								peerManager.getConnectedPeer()
										.sendActualMessage(Constants.ActualMessageType.HAVE.ordinal(), havePayload);
							}
						}

						// Request next piece
						requestPieceIndex = this.getInterestedBit();
						if (requestPieceIndex == -1) {
							this.connectedPeer.sendActualMessage(Constants.ActualMessageType.NOT_INTERESTED.ordinal(),
									null);
						}
						else {
							final byte[] requestPayload = ByteBuffer.allocate(4).putInt(requestPieceIndex).array();
							this.connectedPeer.sendActualMessage(Constants.ActualMessageType.REQUEST.ordinal(),
									requestPayload);
						}

						// Check if complete file received
						int peerProcessBitFieldCardinality;
						synchronized (peerProcessBitField) {
							peerProcessBitFieldCardinality = peerProcessBitField.cardinality();
						}
						if (requestPieceIndex == -1 && peerProcessBitFieldCardinality == PeerProcess.getNoOfPieces()) {

							final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(
									new FileOutputStream(PeerProcess.getFileName()));
							bufferedOutputStream.write(peerProcessFile);
							bufferedOutputStream.flush();
							bufferedOutputStream.close();
							PeerProcess.setHasFile(true);

							LOGGER.info("Peer [peer_ID " + peerProcessPeerId + "] has downloaded the complete file");

							synchronized (PeerProcess.getRemotePeerInfoList()) {
								final Iterator<PeerManager> iterator = PeerProcess.getRemotePeerInfoList().iterator();
								while (iterator.hasNext()) {
									final PeerManager peerManager = iterator.next();
									peerManager.getConnectedPeer().sendActualMessage(
											Constants.ActualMessageType.NOT_INTERESTED.ordinal(), null);
								}
							}
						}
						break;

					case TERMINATE:
						assert receivedActualMessage.getMessagePayload() == null;
						this.terminated = true;
						break;
				}
				receivedActualMessage = null;
			}
		}
		catch (final IOException e) {
			if (!this.terminated) {
				e.printStackTrace();
			}
		}
	}

	private int getInterestedBit() {
		if (this.connectedPeer.getBitField().isEmpty()) {
			return -1;
		}

		final BitSet connectedPeerBitField = (BitSet) this.connectedPeer.getBitField().clone();
		synchronized (peerProcessBitField) {
			connectedPeerBitField.xor(peerProcessBitField);
			connectedPeerBitField.andNot(peerProcessBitField);
		}
		// this.print("Interested Bits: " + connectedPeerBitField);

		if (connectedPeerBitField.isEmpty()) {
			return -1;
		}
		else {
			final int randomIndex = new Random().nextInt(connectedPeerBitField.length());
			// this.print("Random Index: " + randomIndex);
			return connectedPeerBitField.nextSetBit(randomIndex);
		}
	}

	private byte[] getPiece(int pieceIndex) {
		final int startIndex = pieceIndex * PeerProcess.getPieceSize();
		int endIndex = startIndex + PeerProcess.getPieceSize() - 1;
		if (endIndex > peerProcessFile.length - 1) {
			endIndex = peerProcessFile.length - 1;
		}
		final int pieceLength = endIndex - startIndex + 1;

		final ByteBuffer byteBuffer = ByteBuffer.allocate(4 + pieceLength);
		byteBuffer.putInt(pieceIndex);
		byteBuffer.put(peerProcessFile, startIndex, pieceLength);
		return byteBuffer.array();
	}

	private void putPiece(int pieceIndex, byte[] piecePayload) {
		final int startIndex = pieceIndex * PeerProcess.getPieceSize();
		final ByteBuffer byteBuffer = ByteBuffer.wrap(peerProcessFile, startIndex, piecePayload.length);
		byteBuffer.put(piecePayload);
	}

	private void print(String string) {
		if (doPrint) {
			System.out.println(string);
		}
	}

	public static String getPeerProcessPeerId() {
		return peerProcessPeerId;
	}

	public static void setPeerProcessPeerId(String peerProcessPeerId) {
		PeerManager.peerProcessPeerId = peerProcessPeerId;
	}

	public static BitSet getPeerProcessBitField() {
		return peerProcessBitField;
	}

	public static void setPeerProcessBitField(BitSet peerProcessBitField) {
		PeerManager.peerProcessBitField = peerProcessBitField;
	}

	public static Set<RemotePeerInfo> getInterestedNeighbors() {
		return interestedNeighbors;
	}

	public static void setInterestedNeighbors(Set<RemotePeerInfo> interestedNeighbors) {
		PeerManager.interestedNeighbors = interestedNeighbors;
	}

	public static Set<RemotePeerInfo> getPreferredNeighbors() {
		return preferredNeighbors;
	}

	public static void setPreferredNeighbors(Set<RemotePeerInfo> preferredNeighbors) {
		PeerManager.preferredNeighbors = preferredNeighbors;
	}

	public static RemotePeerInfo getOptimisticallyUnchokedNeighbor() {
		return optimisticallyUnchokedNeighbor;
	}

	public static void setOptimisticallyUnchokedNeighbor(RemotePeerInfo optimisticallyUnchokedNeighbor) {
		PeerManager.optimisticallyUnchokedNeighbor = optimisticallyUnchokedNeighbor;
	}

	public static byte[] getPeerProcessFile() {
		return peerProcessFile;
	}

	public static void setPeerProcessFile(byte[] peerProcessFile) {
		PeerManager.peerProcessFile = peerProcessFile;
	}

	public boolean isTerminated() {
		return this.terminated;
	}

	public void setTerminated(boolean terminated) {
		this.terminated = terminated;
	}

	public RemotePeerInfo getConnectedPeer() {
		return this.connectedPeer;
	}

}
