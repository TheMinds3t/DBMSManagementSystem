package cs485.dbms.main;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import javax.swing.JOptionPane;

/**
 * A utility class for refining the java console output. Additionally enables the console to be printed to a file for review.
 * 
 * I implemented this because while debugging it was not possible to see the full log. This circumvents that issue.
 * 
 * @author Ashton Schultz
 * @instructor Prof. Mark Funk
 * @class CS485
 * @date 4.5.2021
 */
public class DebugLog 
{
	private File debugFile;
	private DebugLevel level = DebugLevel.REQUESTS; //A constant essentially, just to refine the java console log.
	private PrintWriter debugWriter;
	
	public DebugLog(String filename)
	{
		try {
			//Create the file reference,
			debugFile = new File(filename);
			
			debugWriter = new PrintWriter(new FileOutputStream(debugFile));
		} catch (IOException e) {}
	}
	
	/**
	 * @return the current debug level of this DebugLog. defaults to DebugLevel.REQUESTS.
	 */
	public DebugLevel getDebugLevel()
	{
		return this.level;
	}
	
	/**
	 * Sets the debug level of this DebugLog object.
	 * @param level the level to set this log to
	 */
	public void setDebugLevel(DebugLevel level)
	{
		if(level != null)
			this.level = level;
	}
	
	/**
	 * Prints the message at the specified debug level within the out stream, creating a message in the console.
	 * 
	 * @param levl the debug level to output this message at.
	 * @param message the message to output.
	 */
	public synchronized void print(DebugLevel levl, String message)
	{
		if(this.level.ordinal() >= levl.ordinal())
		{
			System.out.println(message);
			debugWriter.print(message + "\n");
		}
	}
	
	/**
	 * Prints the message at the specified debug level within the error stream, creating a distinctive message in the console.
	 * In the output log, these messages are prefixed with a "***" sequence.
	 * @param levl the debug level to output this message at.
	 * @param message the message to output.
	 */
	public synchronized void warn(DebugLevel levl, String message)
	{
		if(this.level.ordinal() >= levl.ordinal())
		{
			System.err.println(message);
			debugWriter.print("***" + message + "\n");
		}
	}
	
	/**
	 * Flushes and closes the log file.
	 */
	public void close()
	{
		debugWriter.flush();
		debugWriter.close();
	}
	
	/**
	 * An enum indicative of the level of console output for DebugLog.
	 * 
	 * @author Ashton Schultz
	 * @instructor Prof. Mark Funk
	 * @class CS485
	 * @date 4.12.2021
	 */
	public enum DebugLevel {
		/**
		 * Completion Percentage
		 */
		PERCENT("Completion Percentage"),
		/**
		 * Inits, Terminations, Teller Sent
		 */
		NONE("Inits, Terminations, Teller Sent"), //Ordinal 0 - Only connected and terminated messages
		/**
		 * Inits, Terminations, Teller Sent, Processor Parse
		 */
		REQUESTS("Inits, Terminations, Teller Sent, Processor Parse"), //Ordinal 1 - All request related messages
		/**
		 * Inits, Terminations, Teller Sent, Processor Parse, Locks
		 */
		LOCKS_REQUESTS("Inits, Terminations, Teller Sent, Processor Parse, Locks"); //Ordinal 2 - All request related messages and all lock related messages
		
		//Identifier for DebugLevel.promptUser
		private final String content;
		
		private DebugLevel(String content)
		{
			this.content = content;
		}
		
		/**
		 * Prompts the user to select a debug level, based on descriptors assigned to each level.
		 * @return the selected debug level, or null if the prompt was exited.
		 */
		public static DebugLevel promptUser()
		{
			String[] options = new String[DebugLevel.values().length];
			for(int i = 0; i < DebugLevel.values().length; ++i)
				options[i] = DebugLevel.values()[i].content;
			
			int debugLevel = JOptionPane.showOptionDialog(null, "Please select a console output precision level:", "Select Precision Level", JOptionPane.YES_NO_OPTION, JOptionPane.QUESTION_MESSAGE, null, options, options[0]);
			
			if(debugLevel >= 0 && debugLevel <= DebugLevel.values().length)
				return DebugLevel.values()[debugLevel];
			
			return null;
		}
	}
}
