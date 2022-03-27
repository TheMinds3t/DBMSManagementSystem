package cs485.dbms.data;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import cs485.dbms.main.Main;
import cs485.dbms.main.DebugLog.DebugLevel;

/**
 *  A structured, partitioned list of accounts stored 
 *  based on their id number.
 * 
 * @author Ashton Schultz
 * @instructor Prof. Mark Funk
 * @class CS485
 * @date 3.30.2021
 */
public class AccountTable implements Iterable<Account>
{
	//A constant for how many times to try to acquire a lock
	private static final int LOCK_DEPTH = 5;
	
	//The array of lists of accounts.
	private AccountTableEntry[] entryArray;
	//The number of partitions that the account table is divided into
	private static final int partitionCount = 20;
	
	//# Accounts inside (constant in the assignment, but in real application useful)
	private int size = 0;
	
	//Locks for this data repository
	private final WriteLock writeLock;
	private final ReadLock readLock;
	
	//Set of account numbers
	private LinkedList<Integer> keySet = new LinkedList<Integer>();
	
	/**
	 * Creates an account table to store accounts, partitioned into 20 sublists.
	 */
	public AccountTable()
	{
		entryArray = new AccountTableEntry[partitionCount];
		for(int i = 0; i < partitionCount; ++i)
		{
			entryArray[i] = new AccountTableEntry(i);
		}
		
		ReentrantReadWriteLock l = new ReentrantReadWriteLock();
		writeLock = l.writeLock();
		readLock = l.readLock();
	}
	
	/**
	 * Adds an account to this AccountTable if it is not already present, indexing based on the account number.
	 * @param account the account to be added to this list.
	 */
	public void add(Account account)
	{
		int hashIndex = account.getAccountNumber() % partitionCount;
		
		if(!entryArray[hashIndex].accounts.contains(account))
		{
			entryArray[hashIndex].accounts.add(account);
			keySet.add(account.getAccountNumber());
			account.setTableIndex(size);
			++size;			
		}
	}
	
	/**
	 * Retrieves the stored account based on the account number.
	 * @param accountNumber the account number to search for within this AccountTable.
	 * @return the account if found, null if the designated account number is not found.
	 */
	public Account get(int accountNumber)
	{
		int hashIndex = accountNumber % partitionCount;
		
		for(Account account: entryArray[hashIndex].accounts)
		{
			if(account.getAccountNumber() == accountNumber)
			{
				return account;
			}
		}
		
		return null;
	}
	
	/**
	 * Locks the partition of this AccountTable containing the given account.
	 * @param accountNumber the number of the account to lock
	 * @return true if successful, false if lock not acquired
	 */
	public boolean lockPartition(int accountNumber)
	{
		return entryArray[accountNumber % partitionCount].lockPartition();
	}
	
	/**
	 * Unlocks the partition of this AccountTable with the given account.
	 * @param accountNumber the number of the account to unlock
	 */
	public void unlockPartition(int accountNumber)
	{
		entryArray[accountNumber % partitionCount].unlockPartition();
	}
	
	/**
	 * Attempts to lock the readLock for this AccountTable.
	 * @return true if successful, false if lock not acquired
	 */
	public boolean readLockTable()
	{
		try {
			int depth = LOCK_DEPTH;
			while(depth > 0)
			{
				if(readLock.tryLock() || readLock.tryLock(300, TimeUnit.MILLISECONDS))
				{
					Main.log.print(DebugLevel.LOCKS_REQUESTS, "[" + Thread.currentThread().getName() + "]\tRetrieved read lock!");
					return true;
				}
				
				//System.out.println("Didn't retrieve read lock, trying again");
			}
		} catch (InterruptedException e) {
			return readLockTable();
		}
		
		Main.log.warn(DebugLevel.LOCKS_REQUESTS, "[" + Thread.currentThread().getName() + "]\tDidn't retrieve read lock");
		return false;
	}
	
	/**
	 * Unlocks the read lock for this AccountTable.
	 */
	public void unreadLockTable()
	{
		readLock.unlock();
		Main.log.print(DebugLevel.LOCKS_REQUESTS, "[" + Thread.currentThread().getName() + "]\tUnlocked table read lock");
	}
	
	/**
	 * Attempts to lock the writeLock for this AccountTable.
	 * @return true if successful, false if lock not acquired
	 */
	public boolean writeLockTable()
	{
		try {
			//Attempt LOCK_DEPTH number of times to hold the write lock
			int depth = LOCK_DEPTH;
			while(depth > 0)
			{
				if(writeLock.tryLock() || writeLock.tryLock(500, TimeUnit.MILLISECONDS))
				{
					Main.log.print(DebugLevel.LOCKS_REQUESTS, "[" + Thread.currentThread().getName() + "]\tRetrieved table write lock!");
					return true;
				}
				
				//System.out.println("Didn't retrieve write lock, trying again");
				--depth;
			}
		} catch (InterruptedException e) {
			return writeLockTable();
		}
		
		Main.log.warn(DebugLevel.LOCKS_REQUESTS, "[" + Thread.currentThread().getName() + "]\tDidn't retrieve table write lock");
		return false;
	}
	
	/**
	 * Unlocks the write lock for this AccountTable.
	 */
	public void unwriteLockTable()
	{
		writeLock.unlock();
		Main.log.print(DebugLevel.LOCKS_REQUESTS, "[" + Thread.currentThread().getName() + "]\tUnlocked table write lock");
	}

	/**
	 * Searches for and removes the account associated with the indicated account number.
	 * @param accountNumber the account number to search for
	 * @return the removed account, or null if the account was not removed.
	 */
	public Account remove(int accountNumber)
	{
		int hashIndex = accountNumber % partitionCount;
		
		for(int i = 0; i < entryArray[hashIndex].accounts.size(); ++i)
		{
			Account account = entryArray[hashIndex].accounts.get(i);
			if(account.getAccountNumber() == accountNumber)
			{
				--size;
				keySet.remove(account.getAccountNumber());
				return entryArray[hashIndex].accounts.remove(i);
			}
		}
		
		return null;
	}
	
	/**
	 * Clears the AccountTable of all stored Accounts.
	 */
	public void clear()
	{
		for(int i = 0; i < partitionCount; ++i)
		{
			entryArray[i].accounts.clear();
		}
		
		size = 0;
	}

	/**
	 * @return the number of accounts within this account table.
	 */
	public int size() {
		return size;
	}
	
	/**
	 * @return a list of account numbers, associated with {@link Account}s within this AccountTable.
	 */
	public LinkedList<Integer> getKeySet()
	{
		return keySet;
	}
	
	@Override
	//Implemented to allow foreach loops using an AccountTable
	public Iterator<Account> iterator() 
	{
		return new Iterator<Account>() {
			//The current partition
			private int curEntry = 0;
			//The current entry in the current partition
			private int curItem = 0;
			
			@Override
			public boolean hasNext() {
				//If the current partition is less than the max and the current entry is less than the size of the current partition, can continue.
				return curEntry < partitionCount && curItem < entryArray[curEntry].accounts.size();
			}
	
			@Override
			public Account next() {
				//Get the item,
				Account next = entryArray[curEntry].accounts.get(curItem);
				
				//Increment the current entry position,
				curItem++;
				
				//And if the current entry is larger than the number of entries in this partition (and the current partition is less than the last partition)
				if(curItem >= entryArray[curEntry].accounts.size() && curEntry < partitionCount - 1)
				{
					//Move to the next partition and reset the curItem to the first item.
					++curEntry;
					curItem = 0;
				}
							
				return next;
			}};
	}
	
	//A class containing the list of accounts. This is what the primary AccountTable is utilizing to partition the list.
	private class AccountTableEntry
	{
		//Accounts contained in this partition
		public LinkedList<Account> accounts = new LinkedList<Account>();
		private final WriteLock writeLock;
		//A referential number for this partition, used to indicate which lock was attained/released.
		private final int partitionNumber;
		
		private AccountTableEntry(int num)
		{
			ReentrantReadWriteLock l = new ReentrantReadWriteLock();
			writeLock = l.writeLock();
			partitionNumber = num;
		}
		
		/**
		 * Attempts to lock the write Lock for this partition.
		 * @return true if successful, false if lock not acquired
		 */
		public boolean lockPartition()
		{
			try {
				int depth = LOCK_DEPTH;
				while(depth > 0)
				{
					if(writeLock.tryLock() || writeLock.tryLock(500, TimeUnit.MILLISECONDS))
					{
						Main.log.print(DebugLevel.LOCKS_REQUESTS, "[" + Thread.currentThread().getName() + "]\tLocked partition lock for partition #" + partitionNumber);			
						return true;
					}
					
					--depth;
					//System.out.println("Did not retrieve lock, trying again");
				}
			} catch (InterruptedException e) {
				return lockPartition();
			}
			
			Main.log.warn(DebugLevel.LOCKS_REQUESTS, "[" + Thread.currentThread().getName() + "]\tFailed to lock partition lock for partition #" + partitionNumber + ", hold count on lock: " + writeLock.getHoldCount());			
			return false;
		}
		
		/**
		 * Unlocks the write lock for this partition.
		 */
		public void unlockPartition()
		{
			writeLock.unlock();
			Main.log.print(DebugLevel.LOCKS_REQUESTS, "[" + Thread.currentThread().getName() + "]\tUnlocked partition lock for partition #" + partitionNumber);			
		}
	}
}
