import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class HandShakeMessage {

	private final byte[] handShakeHeader;
	private final byte[] zeroBits;
	private final byte[] peerId;

	public HandShakeMessage(String peerId) {
		this.handShakeHeader = Constants.HEADER_STRING.getBytes();
		this.peerId = peerId.getBytes();
		assert this.peerId.length == 4;
		this.zeroBits = new byte[10];
	}

	public HandShakeMessage(byte[] handShakeHeader, byte[] zeroBits, byte[] peerId) {
		this.handShakeHeader = handShakeHeader;
		this.zeroBits = zeroBits;
		this.peerId = peerId;
	}

	public byte[] getHandShakeMessage() throws IOException {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(32);
		byteBuffer.put(this.handShakeHeader);
		byteBuffer.put(this.zeroBits);
		byteBuffer.put(this.peerId);
		return byteBuffer.array();
	}

	public byte[] getHeader() {
		return this.handShakeHeader;
	}

	public byte[] getZeroBits() {
		return this.zeroBits;
	}

	public String getPeerId() {
		return new String(this.peerId);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(this.handShakeHeader);
		result = prime * result + Arrays.hashCode(this.peerId);
		result = prime * result + Arrays.hashCode(this.zeroBits);
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
		final HandShakeMessage other = (HandShakeMessage) obj;
		if (!Arrays.equals(this.handShakeHeader, other.handShakeHeader)) {
			return false;
		}
		if (!Arrays.equals(this.peerId, other.peerId)) {
			return false;
		}
		if (!Arrays.equals(this.zeroBits, other.zeroBits)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "HandShake [header=" + new String(this.handShakeHeader) + ", zeroBits=" + Arrays.toString(this.zeroBits)
				+ ", peerId=" + new String(this.peerId) + "]";
	}

}
