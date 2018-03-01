import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class CustomLogFormatter extends SimpleFormatter {

	private static Logger logger;
	private static DateFormat dateFormat;

	@Override
	public synchronized String format(LogRecord record) {
		final StringBuilder logBuilder = new StringBuilder();
		logBuilder.append("[").append(dateFormat.format(record.getMillis())).append("]: ").append(record.getMessage())
				.append(System.getProperty("line.separator"));
		return logBuilder.toString();
	}

	@Override
	public String getHead(Handler h) {
		return super.getHead(h);
	}

	@Override
	public String getTail(Handler h) {
		return super.getTail(h);
	}

	public static void setupLogger(String peerId) {
		logger = Logger.getLogger(CustomLogFormatter.class.getName());
		dateFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss.SSS");
		final StringBuilder path = new StringBuilder(System.getProperty("user.dir"));
		path.append(File.separator).append("log_peer_").append(peerId).append(".log");
		FileHandler fileHandler = null;
		try {
			fileHandler = new FileHandler(path.toString());
			fileHandler.setFormatter(new CustomLogFormatter());
			logger.addHandler(fileHandler);
		}
		catch (final Exception e) {
			System.out.println("Exception occured while creating log file.");
			e.printStackTrace();
		}
	}

	public static Logger getLogger() {
		return logger;
	}

}
