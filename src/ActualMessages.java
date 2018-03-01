import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class ActualMessages {

	private final byte[] messageLength;
	private final byte messageType;
	private final byte[] messagePayload;

	public ActualMessages(int messageLength, int messageType, byte[] messagePayload) {
		this.messageLength = ByteBuffer.allocate(4).putInt(messageLength).array();
		this.messageType = (byte) messageType;
		this.messagePayload = messagePayload;
	}

	public byte[] getActualMessage() throws IOException {
		final ByteBuffer byteBuffer;
		if (this.messagePayload == null) {
			byteBuffer = ByteBuffer.allocate(5);
		}
		else {
			byteBuffer = ByteBuffer.allocate(5 + this.messagePayload.length);
		}
		byteBuffer.put(this.messageLength);
		byteBuffer.put(this.messageType);
		if (this.messagePayload != null) {
			byteBuffer.put(this.messagePayload);
		}
		return byteBuffer.array();
	}

	public int getMessageLength() {
		return ByteBuffer.wrap(this.messageLength).getInt();
	}

	public Constants.ActualMessageType getMessageType() {
		final int messageType = new Byte(this.messageType).intValue();
		return Constants.ActualMessageType.values()[messageType];
	}

	public byte[] getMessagePayload() {
		return this.messagePayload;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(this.messageLength);
		result = prime * result + Arrays.hashCode(this.messagePayload);
		result = prime * result + this.messageType;
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
		final ActualMessages other = (ActualMessages) obj;
		if (!Arrays.equals(this.messageLength, other.messageLength)) {
			return false;
		}
		if (!Arrays.equals(this.messagePayload, other.messagePayload)) {
			return false;
		}
		if (this.messageType != other.messageType) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "ActualMessages [messageLength=" + this.getMessageLength() + ", messageType=" + this.getMessageType()
				+ ", messagePayload=" + Arrays.toString(this.messagePayload) + "]";
	}

}
