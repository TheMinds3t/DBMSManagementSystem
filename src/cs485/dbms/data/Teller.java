package cs485.dbms.data;

import java.util.LinkedList;

import cs485.dbms.DBProcessThread;
import cs485.dbms.DatabaseMS;
import cs485.dbms.main.DebugLog.DebugLevel;
import cs485.dbms.main.Main;

/**
 * The class that sends randomly generated requests to the databaseMS.
 * 
 * @author Ashton Schultz
 * @instructor Prof. Mark Funk
 * @class CS485
 * @date 3.30.2021
 */
public class Teller extends Thread
{	
	public static final int MAX_REQUESTS = 100;
	//a reference to the account numbers 
	private LinkedList<Integer> accountIDs;
	
	//The database this teller is tied to.
	private DatabaseMS database;

	public DBProcessThread processor;
	
	public Teller(DatabaseMS databaseMS, int id)
	{
		super("Bank Teller " + id);
		database = databaseMS;
	}
	
	public void run()
	{
		//Wait until the database has been initialized and the processor is connected.
		while(!database.isDatabaseInitialized())
		{
			try {
				sleep(100);
			} catch (InterruptedException e) {
				continue;
			}
		}
		
		//Attempt to initialize local list of account ids
		accountIDs = database.getAccountTable().getKeySet();
		
		//As long as the accounts have not been initialized,
		while(accountIDs.size() == 0)
		{
			//keep checking for the ids to get filled.
			accountIDs = database.getAccountTable().getKeySet();
			try {
				sleep(5);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		//Notify that transactions are going to begin 
		Main.log.print(DebugLevel.NONE, "[" + this.getName() + "] is beginning to send requests.");
		
		//For 100 requests,
		for(int i = 0; i < MAX_REQUESTS; ++i)
		{
			//Increase the request number
			int curReq = database.incrementRequestNumber();
			database.addRequest("<BEGIN " + curReq + ">");
			
			//Create the updates for this request
			for(int l = 0; l < Main.rand.nextInt(5) + 1; ++l)
			{
				int source = accountIDs.get(Main.rand.nextInt(accountIDs.size()));
				int target = accountIDs.get(Main.rand.nextInt(accountIDs.size()));
				
				//Keep reselecting the target if it is the same as the source
				while(target == source)
				{
					target = accountIDs.get(Main.rand.nextInt(accountIDs.size()));
				}
				
				//Determine an amount and send it to the processor.
				double transferAmount = Main.rand.nextDouble() * 1000.0d;
				database.addRequest("<UPDATE " + curReq + ">" + source + "," + target + "," + String.format("%.2f", transferAmount));
			}
			
			//Send the commit,
			database.addRequest("<COMMIT " + curReq + ">");
			//Notify the console of a request being sent,
			Main.log.print(DebugLevel.NONE, "[" + this.getName() + "] Sent request #" + curReq);
			processor.interrupt();
			
			//The flag for the found, correct commit tag.
			boolean found = false;
			
			//Wait until the right commit tag is found before continuing
			while(!found)
			{
				String response = database.getResponses().poll();
				
				if(response == null)
				{
					try {
						sleep(100);
					} catch (InterruptedException e) {}

					continue;
				}
				
				//If the number in the message matches the current request this teller sent, move forward.
				if(response.startsWith("<COMPLETE ") && Integer.parseInt(response.substring(10, response.indexOf(">"))) == curReq)
				{
					//Exit the loop
					found = true;
				}
				else //Otherwise this is the incorrect complete message, rotate responses.
				{
					database.getResponses().add(response);
				}
			}
		}
		
		//This signifies to the database that requests are finished! This is only called by the last teller to finish
		if(database.getRequestNumber() == 399)
		{
			//Incrementing it one more changes the flag in database.isRequestsFinished() for the processors to close and the main thread to close.
			database.incrementRequestNumber();
		}
		
		//Wait for the database to finish (this is for tellers that finish before the last teller)
		while(!DatabaseMS.getInstance().isRequestsFinished())
		{
			try { sleep(100); } catch(InterruptedException e) {}
		}
		
		//Notify the console that this teller is terminated.
		Main.log.warn(DebugLevel.NONE, "[" + getName() + "] has terminated successfully.");
	}
}
