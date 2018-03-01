import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.BitSet;

public class RemotePeerInfo implements Comparable<RemotePeerInfo> {

	private String peerId;
	private final Socket socket;
	private final InputStream inputStream;
	private final OutputStream outputStream;

	private BitSet bitField;
	private long downloadRate;
	private boolean choked;
	private boolean optimisticallyUnchoked;

	public RemotePeerInfo(String peerId, Socket socket) throws IOException {
		this.peerId = peerId;
		this.socket = socket;

		this.inputStream = new BufferedInputStream(socket.getInputStream());
		this.outputStream = new BufferedOutputStream(socket.getOutputStream());

		this.choked = true;
		this.optimisticallyUnchoked = false;
		this.downloadRate = 0;
	}

	public RemotePeerInfo(String peerId) {
		this.peerId = peerId;
		this.socket = null;
		this.inputStream = null;
		this.outputStream = null;
		this.choked = true;
		this.optimisticallyUnchoked = false;
		this.downloadRate = 0;
	}

	public HandShakeMessage receiveHandShakeMessage() throws IOException {
		final byte[] receivedHandShakeMessage = new byte[32];
		synchronized (this.inputStream) {
			this.inputStream.read(receivedHandShakeMessage);
		}

		final byte[] receivedHandShakeHeader = new byte[18];
		final byte[] receivedZeroBits = new byte[10];
		final byte[] receivedPeerId = new byte[4];

		final ByteBuffer byteBuffer = ByteBuffer.wrap(receivedHandShakeMessage);
		byteBuffer.get(receivedHandShakeHeader);
		byteBuffer.get(receivedZeroBits);
		byteBuffer.get(receivedPeerId);

		return new HandShakeMessage(receivedHandShakeHeader, receivedZeroBits, receivedPeerId);
	}

	public void sendHandShakeMessage(String peerId) throws IOException {
		final HandShakeMessage handShakeMessage = new HandShakeMessage(peerId);
		synchronized (this.outputStream) {
			this.outputStream.write(handShakeMessage.getHandShakeMessage());
			this.outputStream.flush();
		}
	}

	public ActualMessages receiveActualMessage() throws IOException {
		final byte[] receivedActualMessageLength = new byte[4];
		synchronized (this.inputStream) {
			this.inputStream.read(receivedActualMessageLength);
		}
		final int messageLength = ByteBuffer.wrap(receivedActualMessageLength).getInt();
		assert messageLength > 0;

		final byte[] receivedActualMessage = new byte[messageLength];
		synchronized (this.inputStream) {
			this.inputStream.read(receivedActualMessage);
		}

		final ByteBuffer byteBuffer = ByteBuffer.wrap(receivedActualMessage);
		final byte receivedMessageType = byteBuffer.get();
		byte[] receivedMessagePayload = null;
		if (messageLength > 1) {
			receivedMessagePayload = new byte[messageLength - 1];
			byteBuffer.get(receivedMessagePayload);
		}

		return new ActualMessages(messageLength, receivedMessageType, receivedMessagePayload);
	}

	public void sendActualMessage(int messageType, byte[] messagePayload) throws IOException {
		ActualMessages actualMessages;
		if (messagePayload != null) {
			actualMessages = new ActualMessages(1 + messagePayload.length, messageType, messagePayload);
		}
		else {
			actualMessages = new ActualMessages(1, messageType, messagePayload);
		}
		synchronized (this.outputStream) {
			this.outputStream.write(actualMessages.getActualMessage());
			this.outputStream.flush();
		}
	}

	public String getPeerId() {
		return this.peerId;
	}

	public void setPeerId(String peerId) {
		this.peerId = peerId;
	}

	public BitSet getBitField() {
		return this.bitField;
	}

	public void setBitField(BitSet bitField) {
		this.bitField = bitField;
	}

	public void setBitFieldIndex(int index) {
		this.bitField.set(index);
	}

	public long getDownloadRate() {
		return this.downloadRate;
	}

	public void setDownloadRate(long downloadRate) {
		this.downloadRate = downloadRate;
	}

	public boolean isChoked() {
		return this.choked;
	}

	public void setChoked(boolean choked) {
		this.choked = choked;
	}

	public boolean isOptimisticallyUnchoked() {
		return this.optimisticallyUnchoked;
	}

	public void setOptimisticallyUnchoked(boolean optimisticallyUnchoked) {
		this.optimisticallyUnchoked = optimisticallyUnchoked;
	}

	public Socket getSocket() {
		return this.socket;
	}

	public InputStream getInputStream() {
		return this.inputStream;
	}

	public OutputStream getOutputStream() {
		return this.outputStream;
	}

	@Override
	public int compareTo(RemotePeerInfo other) {
		if (this.equals(other)) {
			return 0;
		}
		else {
			if (this.downloadRate == other.downloadRate) {
				return Integer.compare(this.hashCode(), other.hashCode());
			}
			else {
				return Long.compare(this.downloadRate, other.downloadRate);
			}
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ( this.peerId == null ? 0 : this.peerId.hashCode() );
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		final RemotePeerInfo other = (RemotePeerInfo) obj;
		if (this.peerId == null) {
			if (other.peerId != null) {
				return false;
			}
		}
		else if (!this.peerId.equals(other.peerId)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "RemotePeerInfo [peerId=" + this.peerId + ", socket=" + this.socket + ", bitField=" + this.bitField
				+ ", downloadRate=" + this.downloadRate + ", choked=" + this.choked + ", optimisticallyUnchoked="
				+ this.optimisticallyUnchoked + "]";
	}

}
