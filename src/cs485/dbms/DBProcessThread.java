package cs485.dbms;

import java.util.LinkedList;

import cs485.dbms.data.Account;
import cs485.dbms.data.Teller;
import cs485.dbms.main.DebugLog.DebugLevel;
import cs485.dbms.main.Main;

/**
 *  A thread to process requests and update the database that is 
 *  tied to this processing thread.
 * 
 * @author Ashton Schultz
 * @instructor Prof. Mark Funk
 * @class CS485
 * @date 3.30.2021
 */
public class DBProcessThread extends Thread 
{
	//private Teller curTeller = null;
	private final DatabaseMS database;
	
	//Indicates which lock on the accounttable this processor will try to hold
	private boolean rolledBack = false;
	
	//Whether or not this thread is processing a request
	private boolean isProcessing = false;
	
	private boolean holdingRLock = false, holdingWLock = false;
	
	public DBProcessThread(DatabaseMS owner, int id)
	{
		super(owner.getName() + " (Processing Thread " + id + ")");
		this.database = owner;
	}
	
	public void run()
	{
		//Wait until the database has been flushed and the teller has been connected to this thread.
		while(!database.isDatabaseInitialized()) try{ sleep(100); } catch(InterruptedException e) {}		
		Main.log.print(DebugLevel.NONE, "[" + this.getName() + "] is now ready to handle requests.");
		
		while(!database.isRequestsFinished())
		{
			String request = database.getRequest();
			
			if(request == null)
			{
				try {
					sleep(5);
				} catch (InterruptedException e) {}			

				continue;
			}
			
			//If handle request returns false then an issue occurred
			if(!handleRequest(request))
			{
				int curNum = Integer.parseInt(request.substring(request.indexOf(" ") + 1, request.indexOf(">")));

				//If the request is not committed, reinsert the request.
				if(!database.getCommittedRequests().contains(curNum))
				{
					database.addRequest(request);
				}
				
				//Wait 5 milliseconds to attempt to handle another request.
				try {
					sleep(5);
				} catch (InterruptedException e) {}			
				
				continue;
			}
		}
		
		//For the backup processor, wait until primary is done before the backup closes
		while(!DatabaseMS.getInstance().isRequestsFinished())
		{try { sleep(100); } catch(InterruptedException e) {}}
		
		Main.log.warn(DebugLevel.NONE, "[" + getName() + "] has terminated successfully.");
	}
	
	private boolean lockDatabase()
	{
		if(database.isBackup)
			return true;
		
		//If rolledback, then attain write lock.
		if(rolledBack)
		{
			holdingWLock = database.getAccountTable().writeLockTable();
			return holdingWLock;
		}
		else //Else attain read lock
		{
			holdingRLock = database.getAccountTable().readLockTable();
			return holdingRLock;
		}
	}
	
	private void unlockDatabase()
	{
		if(database.isBackup)
			return;
		
		if(holdingRLock)
		{
			database.getAccountTable().unreadLockTable();
			holdingRLock = false;
		}
		
		if(holdingWLock)
		{
			database.getAccountTable().unwriteLockTable();
			holdingWLock = false;
		}
	}
	
	private boolean handleRequest(String request)
	{	
		int curNum = Integer.parseInt(request.substring(request.indexOf(" ") + 1, request.indexOf(">")));
		
		//If this is the primary database,
		if(!database.isBackup)
		{
			//Prevent processing packets further than the backup database
			if(DatabaseMS.getInstance(true).getRequestNumber()+1 < curNum && curNum > 0)
			{
				//If the current packet has begun,
				if(database.hasStarted(curNum))
				{
					unlockDatabase();
					database.addRequest("<BEGIN " + curNum + ">");
				}
				
				//Return false, unsuccessful cause premature packet.
				return false;
			}
		}
		
		//If the request hasn't been started in the database,
		if(!database.hasStarted(curNum) && !request.equals("<BEGIN " + curNum + ">"))
		{
			//Premature update packet. Ignore it!
			Main.log.print(DebugLevel.REQUESTS, "[" + getName() + "] hasn't started request #" + curNum + ", skipping");
			return false;
		}
		
		//Switch based on the prefix of each request
		switch(request.substring(0, request.indexOf(" ")))
		{
			case "<BEGIN":
			{
				//Determine the request number
				isProcessing = true;				
				return database.startRequest(curNum);
			}
			case "<UPDATE":
			{

				//Assemble an update packet for unpacking on parsing commit
				String[] args = request.substring(request.indexOf(">")+1).split(",");
				Account source = database.getAccountTable().get(Integer.parseInt(args[0]));
				Account target = database.getAccountTable().get(Integer.parseInt(args[1]));
				double transferAmount = Double.parseDouble(args[2]);
				//Synchronized method in database. Returns true if the packet is unique and the request has begun
				boolean update = database.addUpdatePacket(curNum, new DBUpdatePacket(curNum, source, target, transferAmount));
				
				if(update)
					Main.log.print(DebugLevel.REQUESTS, "[" + getName() + "] Added update \'" + request + "\' for request #" + curNum);
				else
				{
					//Update failed to apply
					unlockDatabase();
					rolledBack = true;
					return false;
				}
				
				return update;
			}
			case "<COMMIT":
			{
				//If the lock can't be attained
				if(!lockDatabase())
				{
					Main.log.print(DebugLevel.REQUESTS, "[" + getName() + "] Failed to lock database for request \'" + request + "\' , not committing yet. rollback = " + rolledBack);
					//rolledBack = true;
					return false;
				}
				
				LinkedList<DBUpdatePacket> packets = database.getUpdatePackets(curNum);
				
				//If there are still packets in the request queue,
				int packetCount = database.countPacketsInQueue(curNum);
				if(packetCount > 0)
				{
					//This means that this request is not ready to commit.
					Main.log.print(DebugLevel.REQUESTS, "[" + getName() + "] " + packetCount + " packets exist for request " + curNum + ", not committing yet.");
					unlockDatabase();
					return false;
				}
				
				if(database.countUpdatesCached(curNum) == 0)
				{
					Main.log.print(DebugLevel.REQUESTS, "[" + getName() + "] Req #" + curNum + " has not cached any update packets. Not committing");
					unlockDatabase();
					return false;
				}
				
				int rollbackIndex = -1;
				
				//Start the log addition
				String logAppend = "<BEGIN " + curNum + ">\n";
				
				LinkedList<Account> accounts = new LinkedList<Account>();
				
				if(packets != null)
				{
					//Iterate through and apply each packet, adding it to the log string
					for(int i = 0; i < packets.size(); ++i)
					{
						DBUpdatePacket packet = packets.get(i);
						Account source = packet.sourceAccount;
						Account target = packet.targetAccount;
						double transferAmount = packet.transferAmount;
						
						//If database is backup, no need to lock so true. Otherwise true if lock is attained
						boolean sourceLock = database.isBackup ? true : database.getAccountTable().lockPartition(source.getAccountNumber());
						boolean targetLock = database.isBackup ? true : database.getAccountTable().lockPartition(target.getAccountNumber());
						
						//If either lock failed, release locks just attained and stop updates
						if(!targetLock || !sourceLock)
						{
							if(targetLock) database.getAccountTable().unlockPartition(target.getAccountNumber());
							if(sourceLock) database.getAccountTable().unlockPartition(source.getAccountNumber());
							rollbackIndex = i;
							break;
						}
						
						accounts.add(source);
						accounts.add(target);
						
						//Transfer balance and update accounts in file
						source.setBalance(source.getBalance() - transferAmount);
						target.setBalance(target.getBalance() + transferAmount);
						database.getAccountWriter().writeAccount(source);
						database.getAccountWriter().writeAccount(target);
						
						logAppend += packet.toCommand() + "\n";						
					}
					
					if(rollbackIndex > -1)
					{
						//Revert changes already applied
						for(int i = 0; i < rollbackIndex; ++i)
						{
							DBUpdatePacket packet2 = packets.get(i);
							Account source2 = packet2.sourceAccount;
							Account target2 = packet2.targetAccount;
							double transferAmount2 = packet2.transferAmount;
							
							source2.setBalance(source2.getBalance() + transferAmount2);
							target2.setBalance(target2.getBalance() - transferAmount2);
							database.getAccountWriter().writeAccount(source2);
							database.getAccountWriter().writeAccount(target2);
							
							database.getAccountTable().unlockPartition(source2.getAccountNumber());
							database.getAccountTable().unlockPartition(target2.getAccountNumber());
							database.addRequest(packet2.toCommand());
						}
						
						unlockDatabase();
						rolledBack = true;
						//Locks could not be attained, rolling back
						return false;
					}
										
					//Send requests to backup database and wait for it to process before finalizing commit
					if(!database.isBackup && DatabaseMS.getInstance(true).isBackup)
					{
						database.sendToBackupDatabase("<BEGIN " + curNum + ">");
						
						synchronized(packets)
						{
							for(DBUpdatePacket pack : packets)
							{
								database.sendToBackupDatabase(pack.toCommand());
							}							
						}
						
						database.sendToBackupDatabase("<COMMIT " + curNum + ">");
						
						//Wait on backup database to commit this transaction
						int backNum = DatabaseMS.getInstance(true).getRequestNumber();
						while(backNum < curNum) {
							backNum = DatabaseMS.getInstance(true).getRequestNumber();
							try { sleep(100); } catch(InterruptedException e) {}
						}

						Main.log.print(DebugLevel.REQUESTS, "[" + getName() + "] Finished waiting on backup, proceeding with commit");
					}
					
					//If this is the backup processing thread, tellers don't update this.
					//So update the request number once it is done being executed
					if(database.isBackup)
					{
						int ret = database.incrementRequestNumber();
						Main.log.print(DebugLevel.REQUESTS, "[" + getName() + "] Incremented request number from backup to #" + ret);
					}
					
					//Write the full log string, plus a commit 
					database.writeToLog(logAppend + "<COMMIT " + curNum + ">");
					database.getCommittedRequests().add(curNum);
					
					isProcessing = false;
					rolledBack = false;

					if(!database.isBackup)
					{
						for(Account acc : accounts)
						{
							database.getAccountTable().unlockPartition(acc.getAccountNumber());
						}						
						
						//Send a response to the teller, waking the thread
						this.database.getResponses().add("<COMPLETE " + curNum + ">");
					}
					
					Main.log.print(DebugLevel.REQUESTS, "["+this.getName()+"] Committed request #" + curNum);
					unlockDatabase();
					
					//Successful commit!
					return true;
				}
				
				//This means a commit was received before a begin
				return false;
			}
			
			//This is an invalid operation
			default: return false;
		}
	}	
	
	public boolean isProcessing() 
	{
		return isProcessing;
	}

	public void setTeller(Teller teller) {
		//this.teller = teller;
		teller.processor = this;
	}
}
