package cs485.dbms;

import cs485.dbms.data.Account;

/**
 *  A class for transporting information regarding updates within requests,
 *  utilized by process threads to cache the updates until the commit is executed.
 * 
 * @author Ashton Schultz
 * @instructor Prof. Mark Funk
 * @class CS485
 * @date 3.30.2021
 */
public class DBUpdatePacket 
{
	public final Account sourceAccount;
	public final Account targetAccount;
	public final double transferAmount;
	public final int requestNumber;
	
	public DBUpdatePacket(int requestNum, Account source, Account target, double transfer)
	{
		requestNumber = requestNum;
		sourceAccount = source;
		targetAccount = target;
		transferAmount = transfer;
	}
	
	public String toString()
	{
		return "[Transfer $" + transferAmount + " from Acct#" + sourceAccount.getAccountNumber() + " to Acct#" + targetAccount.getAccountNumber() + "]";
	}
	
	public String toCommand()
	{
		return "<UPDATE " + requestNumber + ">" + sourceAccount.getAccountNumber() + "," + targetAccount.getAccountNumber() + "," + transferAmount;
	}
	
	public boolean equals(DBUpdatePacket packet)
	{
		return sourceAccount == packet.sourceAccount && targetAccount == packet.targetAccount && transferAmount == packet.transferAmount && requestNumber == packet.requestNumber;
	}
}
