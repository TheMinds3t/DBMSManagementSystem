package cs485.dbms;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;

import cs485.dbms.data.AccountTable;
import cs485.dbms.data.AccountWriter;
import cs485.dbms.data.SyncQueue;
import cs485.dbms.data.Teller;
import cs485.dbms.main.DebugLog.DebugLevel;
import cs485.dbms.main.Main;

/**
 *  The class containing all attributes for this database.
 *  
 *  Watches the processing threads and holds the shared variables for 
 *  all child threads.
 * 
 * @author Ashton Schultz
 * @instructor Prof. Mark Funk
 * @class CS485
 * @date 3.30.2021
 */
public class DatabaseMS
{
	//The two instances of DatabaseMS, one for a primary database and one for a backup.
	private static DatabaseMS instance;
	private static DatabaseMS backupInstance;
		
	/**
	 * Indicates whether or not this database is a backup.
	 */
	public final boolean isBackup;

	
	//Indicates when the processing and teller threads should start their logic
	private boolean finishedInit = false;

	//active accounts within the database
	private final AccountTable accountTable = new AccountTable();
	private final AccountWriter accountWriter;

	//active tellers in the database
	private final Teller[] tellers;
	//active processing threads for this database
	private final DBProcessThread[] processThreads;
	private Integer curRequestNumber = -1;
	
	//The output file, logging each transaction step
	private final File logFile;
	private PrintWriter logWriter;
	
	//A queue holding a single set of instructions for curProcess
	private SyncQueue<String> dbRequests = new SyncQueue<String>();
	//A queue of responses given from requests.
	private SyncQueue<String> dbResponses = new SyncQueue<String>();
	
	//A list of all updates for each request, stored by request number.
	private HashMap<Integer, LinkedList<DBUpdatePacket>> updatePackets = new HashMap<Integer, LinkedList<DBUpdatePacket>>();
	//A list of all requests that have been committed. Essentially a flag, true when committedRequests.contains(request#)
	private SyncQueue<Integer> committedRequests = new SyncQueue<Integer>();
	
	//The constructor is private to prevent multiple instances of the DatabaseMS from being created.
	private DatabaseMS(boolean backup)
	{
		isBackup = backup;
		accountWriter = new AccountWriter(this, "Accounts" + (backup ? "Replicate" : "Primary") + ".txt");
		
		if(!backup)
			tellers = new Teller[4];
		else
			tellers = new Teller[0];//No tellers for backup database
		
		//If this is a backup thread, only needs a single processing. Otherwise match the number of tellers.
		processThreads = new DBProcessThread[backup ? 1 : tellers.length];
		
		//Create the log file that will be modified, based on whether this database is a backup or not
		logFile = new File("DBLog" + (backup ? "_Backup" : "") + ".txt");
		try {
			if(logFile.exists())
				logFile.delete();
			//And create the stream to write to the file with.
			logWriter = new PrintWriter(logFile);
		} catch (FileNotFoundException e) { System.err.println("Unable to open stream to write database log.\n" + e.toString());}
	}
	
	//This indicates a primary database. Private to prevent instantiation, creating singular instances of both the primary and backup database
	private DatabaseMS()
	{
		this(false);
	}
	
	/**
	 * Starts the database, and all associated processing threads / tellers.
	 */
	private void startDatabase()
	{
		//Initialize and start each processing thread
		for(int i = 0; i < processThreads.length; ++i)
		{
			processThreads[i] = new DBProcessThread(this, i);
			processThreads[i].start();
		}
		
		//Start by reading the accounts from the file
		accountWriter.readAccountsFromFile();
		
		//If this is the primary database, initialize and connect the tellers
		if(!isBackup)
			initTellers();
		
		finishInit();
	}
	
	//Indicates to child threads to) begin executing
	private void finishInit()
	{
		Main.log.print(DebugLevel.NONE, "[" + this.getName() + "] Database has finished initialization.");
		//Indicate to all tellers and processors that requests can be sent now
		finishedInit = true;
	}
	
	//Initializes the primary database connections
	private void initTellers()
	{
		//Initialize and start each teller thread
		for(int i = 0; i < tellers.length; ++i)
		{
			tellers[i] = new Teller(this, i);
			tellers[i].start();
		}
		
		//Tie the processing threads and the teller threads together
		for(int i = 0; i < processThreads.length; ++i)
		{
			processThreads[i].setTeller(tellers[i]);
		}
	}
	
	//Called once the main thread exits
	private void closeDatabase()
	{
		accountWriter.closeAccountFile();
		logWriter.flush();
		logWriter.close();
	}
	
	/**
	 * @return the name of this database.
	 */
	public String getName()
	{
		return (isBackup ? "(Backup) " : "") + "DBMS";
	}
	
	/**
	 * @return true once the accounts have been parsed and integrated into this database, false if not finished flushing accounts.
	 */
	public boolean isDatabaseInitialized()
	{
		return finishedInit;
	}
	
	/**
	 * Writes a line to the logfile, outputted to "DBLog.txt", or "DBLog_Backup.txt" depending on which database this is called from.
	 * @param line the line to write to the file.
	 */
	protected void writeToLog(String line)
	{
		logWriter.append(line + "\n");
		logWriter.flush();
	}
	
	/**
	 * @return the up-to-date AccountTable associated with this database.
	 */
	public AccountTable getAccountTable()
	{
		return accountTable;
	}
	
	/**
	 * @return the AccountWriter tied with this database.
	 */
	protected AccountWriter getAccountWriter()
	{
		return accountWriter;
	}
	
	/**
	 * Increments the current request number for this database by 1.
	 * @return The new request number.
	 */
	public int incrementRequestNumber() 
	{
		int ret = -1;
		synchronized(curRequestNumber)
		{
			++curRequestNumber;
			ret = curRequestNumber;
		}
		
		return ret;
	}
	
	/**
	 * @return the current request number for this database.
	 */
	public int getRequestNumber()
	{
		synchronized(curRequestNumber)
		{
			return curRequestNumber;			
		}
	}
	
	/**
	 * Communicates with each processing thread tied to this database to see if any are processing a packet.
	 * @return true if any processing thread is active, false if all are waiting.
	 */
	public boolean isDatabaseProcessing()
	{
		synchronized(processThreads)
		{
			for(DBProcessThread thread : processThreads)
			{
				if(thread.isProcessing())
				{
					return true;
				}
			}
			
			return false;
		}
	}
	
	/**
	 * Starts the given request
	 * @param request the request # to start
	 * @return true if request is not started yet, false if already started.
	 */
	protected boolean startRequest(int request)
	{
		synchronized(updatePackets)
		{
			//Clear the stored updates for this request if it's began already
			if(updatePackets.get(request) != null) updatePackets.get(request).clear();
			
			updatePackets.put(request, new LinkedList<DBUpdatePacket>());
			return true;
		}
	}
	
	/**
	 * Checks if the request has been started.
	 * @param request the request # to check
	 * @return true if the request is started, false if not
	 */
	public boolean hasStarted(int request)
	{
		synchronized(updatePackets)
		{
			return updatePackets.get(request) != null;
		}
	}
	
	protected SyncQueue<Integer> getCommittedRequests()
	{
		return committedRequests;
	}
	
	/**
	 * Adds an update packet for the indicated request.
	 * @param request the request # to add this update to
	 * @param packet the update packet to add
	 * @return true if packet addition was successful, false if request not begun
	 */
	protected boolean addUpdatePacket(int request, DBUpdatePacket packet)
	{
		synchronized(updatePackets)
		{
			if(updatePackets.get(request) == null)
				{
				System.out.println("Updates were null, not adding");
				return false;
				}
			
			updatePackets.get(request).add(packet);
			return true;
		}
	}
	
	/**
	 * Counts the number of packets for the given request within the request queue of this database.
	 * @param reqNum the request # to check
	 * @return the number of packets within the response queue for this request #
	 */
	protected int countPacketsInQueue(int reqNum)
	{
		synchronized(dbRequests)
		{
			int ret = 0;
			
			if(dbRequests.isEmpty())
				return 0;
			
			for(int i = 0; i < dbRequests.size(); ++i)
			{
				try {
					String req = dbRequests.get(i);
					if(req == null)
						break;
					if(Integer.parseInt(req.substring(req.indexOf(" ") + 1, req.indexOf(">"))) == reqNum)
						++ret;
					
				} catch(Exception e) {continue;}
			}
			
			return ret;
		}
	}
	
	/**
	 * Retrieves the maximum number of update packets for the given request number.
	 * @param curNum the request number to check.
	 * @return The number of update packets received in the request queue.
	 */
	protected int countUpdatesCached(int curNum) 
	{
		synchronized(updatePackets)
		{
			return updatePackets.get(curNum) == null ? 0 : updatePackets.get(curNum).size();
		}
	}
	
	/**
	 * Retrieves the list of update packets for the given request.
	 * 
	 * @param request the request # to retrieve for
	 * @return the current list of update packets for the given request.
	 */
	protected LinkedList<DBUpdatePacket> getUpdatePackets(int request)
	{
		synchronized(updatePackets)
		{
			return updatePackets.get(request);
		}
	}
	
	/**
	 * Sends a request to the backup database, if backups are enabled.
	 */
	public void sendToBackupDatabase(String request)
	{
		if(this.isBackup)
		{
			//Once this is finished initing, add a request to the only processing thread.
			while(!this.finishedInit) try{Thread.sleep(10);} catch(InterruptedException e) {}
			this.addRequest(request);
			//System.out.println("Sent " + request + " to backup database");
		}
		else
		{
			//Call the function in the backup database.
			DatabaseMS.getInstance(true).sendToBackupDatabase(request);
		}
	}
	
	public SyncQueue<String> getResponses()
	{
		return dbResponses;
	}
	
	/**
	 * Adds a request to this processing thread.
	 * @param req the request to add.
	 */
	public void addRequest(String req)
	{
		dbRequests.add(req);
	}
	
	/**
	 * @return true if each teller has committed Teller.MAX_REQUESTS requests to this database, false if not.
	 */
	public boolean isRequestsFinished()
	{
		synchronized(committedRequests)
		{
			return committedRequests.size() >= getInstance(false).tellers.length * Teller.MAX_REQUESTS;				
		}
	}
	
	/**
	 * Retrieves the current request.
	 */
	protected String getRequest()
	{
		String ret = dbRequests.poll();
		return ret;
	}
	
	/**
	 * Sorts the requests by their request number.
	 */
	protected void sortRequests()
	{
		dbRequests.sort((String a, String b) -> {return Integer.parseInt(a.substring(a.indexOf(" ")+1, a.indexOf(">")));});
	}
		
	/**
	 * @return The primary DatabaseMS. See the version of this call with a parameter to access the backup.
	 */
	public static DatabaseMS getInstance()
	{
		return getInstance(false);
	}

	/**
	 * Retrieves the active DatabaseMS, or creates one if it has not been activated.
	 * Additionally, creates the backup DatabaseMS if it does not exist.
	 * @param backup true if accessing the backup database, false if not.
	 * @return the primary DatabaseMS or the backup DatabaseMS, depending on the parameter.
	 */
	public static DatabaseMS getInstance(boolean backup)
	{
		if(instance == null)
		{
			instance = new DatabaseMS();
		}

		if(backupInstance == null)
		{
			backupInstance = new DatabaseMS(true);
		}
		
		return backup ? backupInstance : instance;
	}
	
	/**
	 * Sets up both the primary and backup databases.
	 * Creates and connects the DBProcessThreads and Tellers together, and starts the requests.
	 */
	public static void startDatabases()
	{
		getInstance(false).startDatabase();
		getInstance(true).startDatabase();
	}
	
	/**
	 * Closes the files for both the primary and backup databases.
	 */
	public static void closeDatabases()
	{
		getInstance(true).closeDatabase();
		getInstance(false).closeDatabase();
	}
}