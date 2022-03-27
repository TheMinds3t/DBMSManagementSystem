package cs485.dbms.main;

import java.util.Random;

import cs485.dbms.DatabaseMS;
import cs485.dbms.main.DebugLog.DebugLevel;

/**
 * Assignment - Final DBMS
 * 
 *  Emulates a Database Management System, complete with processing threads,
 *  requesters (Tellers), and a backup database.
 * 
 * @author Ashton Schultz
 * @instructor Prof. Mark Funk
 * @class CS485
 * @date 3.30.2021
 */
public class Main {
	/**
	 * A common random number generator utilized within this database program.
	 */
	public static final Random rand = new Random();
	
	/**
	 * The log file to refine the console output
	 */
	public static final DebugLog log = new DebugLog("JavaLog.txt");
	
	public static void main(String[] args) {
		//Let the user pick how detailed the console output is
		log.setDebugLevel(DebugLevel.promptUser());
		
		//Start the primary and backup databases
		DatabaseMS.startDatabases();
		
		//And stay alive as long either database is active
		boolean exit = false;
		
		while(!exit)
		{
			//Debug level for front-end level console
			if(log.getDebugLevel() == DebugLevel.PERCENT)
			{
				int amt = DatabaseMS.getInstance().getRequestNumber();
				
				if(amt > -1)
					System.out.println("(Completion: " + String.format("%.2f",amt / 4.0) + "%)");
			}
			
			exit = (DatabaseMS.getInstance().isRequestsFinished() && DatabaseMS.getInstance(true).isRequestsFinished());

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {}
		}
		
		log.warn(DebugLevel.NONE, "[" + Thread.currentThread().getName() + "] has terminated successfully.");
		log.close();
	}
}
