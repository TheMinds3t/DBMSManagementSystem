package cs485.dbms.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import cs485.dbms.DatabaseMS;

/**
 * A class utilizing memory mapped file io that dynamically updates accounts as they are modified
 * to the output file. All it takes is to call "writeAccount(account)" and it will reflect in the output file.
 * 
 * @author Ashton Schultz
 * @instructor Prof. Mark Funk
 * @class CS485
 * @date 3.30.2021
 */
public class AccountWriter
{
	//The max length of each written field in accounts. Order: FirstName, LastName, AccountNum, AccountBal
	private static final int[] maxLengths = {10,10,10,10};

	//The name of the output file
	private final String fileName;
	
	//A reference to the account table
	private AccountTable accountTable;
	
	//The length, in bytes, of each line in the output file
	private int byteLength = maxLengths[0] + maxLengths[1] + maxLengths[2] + maxLengths[3] + 1;
	
	//The output file and stream
	private RandomAccessFile file;
	private MappedByteBuffer buffer;
	
	public AccountWriter(DatabaseMS db, String fn)
	{
		fileName = fn;
		accountTable = db.getAccountTable();
	}
	
	/**
	 * Reads the accounts from the input file "Accounts.txt", and stores them into the {@link AccountTable} stored in {@link DatabaseMS}.
	 */
	public void readAccountsFromFile()
	{
		accountTable.clear();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File("Accounts.txt")));
			String line = reader.readLine();
			while(line != null)
			{
				String[] tokens = line.split("\t");
				accountTable.add(new Account(tokens[0], tokens[1], Integer.parseInt(tokens[2]), Double.parseDouble(tokens[3])));
				line = reader.readLine();
			}
			
			reader.close();
			
			File f = new File(fileName);
			if(f.exists())
				f.delete();
			file = new RandomAccessFile(fileName, "rw");
			buffer = file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, byteLength * accountTable.size());

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Closes the stream to the memory mapped file to update accounts from.
	 */
	public void closeAccountFile()
	{
		try {
			file.close();
			file = null;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Updates the output file to reflect the passed account's current values.
	 * @param acc the account to update in the output file.
	 */
	public void writeAccount(Account acc)
	{		
		//Update the formatted account in the file at the designated position for this account
		buffer.put(acc.getTableIndex()*byteLength, formatAccount(acc).getBytes());
		buffer.force();
	}
	
	//A helper function to simplify formatAccount.
	private String addSpaceTo(String component, int num)
	{
		for(;num > 0;--num)
			component += " ";
		return component;
	}
	
	//Formats the account information into a uniform length string for the file
	private String formatAccount(Account acc)
	{
		int fnDif = maxLengths[0] - acc.getFirstName().length();
		int lnDif = maxLengths[1] - acc.getLastName().length();
		int anDif = maxLengths[2] - new String("" + acc.getAccountNumber()).length();
		int blDif = maxLengths[3] - acc.getRoundedBalance().length();
		return addSpaceTo(acc.getFirstName(), fnDif) + "" + addSpaceTo(acc.getLastName(), lnDif) 
		+ "" + addSpaceTo("" + acc.getAccountNumber(), anDif) + "" + addSpaceTo(acc.getRoundedBalance(), blDif) + "\n";
	}
}
