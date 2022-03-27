package cs485.dbms.data;

import java.math.RoundingMode;
import java.text.DecimalFormat;

/**
 * The class holding the values for each individual account.
 * 
 * @author Ashton Schultz
 * @instructor Prof. Mark Funk
 * @class CS485
 * @date 3.30.2021
 */
public class Account 
{
	private String firstName, lastName;
	private int accountNumber;
	private double accountBalance;
	
	//A cache of the rounded balance
	private String roundedBalance;
	//A reference to the rounding formatting
	private DecimalFormat roundFormat;
	
	//The index of this account in the AccountTable
	private int tableIndex = -1;
	
	public Account(String first, String last, int account, double balance)
	{
		firstName = first;
		lastName = last;
		accountNumber = account;
		accountBalance = balance;
		
		roundFormat = new DecimalFormat("#######.##");
		roundFormat.setRoundingMode(RoundingMode.HALF_UP);
	}
	
	/**
	 * Sets the table index, if it has not been set yet.
	 * @param index the index this account is at in the account table.
	 */
	protected void setTableIndex(int index)
	{
		if(tableIndex == -1)
		{
			tableIndex = index;			
		}
	}
	
	/**
	 * @return the index in the {@link AccountTable} this account is contained in, or -1 if it is not in a table.
	 */
	public int getTableIndex()
	{
		return tableIndex;
	}
		
	public String getFirstName()
	{
		return firstName;
	}
	
	public String getLastName()
	{
		return lastName;
	}
	
	public int getAccountNumber()
	{
		return accountNumber;
	}
	
	/**
	 * Sets the balance of this account, and recalculates the rounded balance.
	 * @param balance the new balance
	 */
	public synchronized void setBalance(double balance)
	{
		accountBalance = balance;
		calculateRoundedBalance();
	}
	
	public synchronized double getBalance()
	{
		return accountBalance;
	}	
	
	public synchronized String getRoundedBalance()
	{
		if(roundedBalance == null)
			calculateRoundedBalance();
		return roundedBalance;
	}
	
	private synchronized void calculateRoundedBalance()
	{
		roundedBalance = roundFormat.format(accountBalance);
	}
	
	public String toString()
	{
		return "Account Name: " + getFirstName() + " " + getLastName() + "\t||\tAccount #: " + accountNumber + "\t||\tAccount Balance: $" + accountBalance;
	}
}
