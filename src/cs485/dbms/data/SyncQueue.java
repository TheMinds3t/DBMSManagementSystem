package cs485.dbms.data;

import java.util.Comparator;
import java.util.LinkedList;

/**
 * A wrapper class for a LinkedList<E> object, forces synchronized 
 * actions to ensure concurrency.
 * 
 * 
 * @author Ashton Schultz
 * @instructor Prof. Mark Funk
 * @class CS485
 * @date 4.12.2021
 */
public class SyncQueue<E>
{
	private final LinkedList<E> queue = new LinkedList<E>();
	
	public synchronized void add(E message)
	{
		queue.add(message);
	}
	
	public synchronized E poll()
	{
		return queue.poll();
	}
	
	public synchronized void sort(Comparator<? super E> comp)
	{
		queue.sort(comp);
	}

	public synchronized boolean isEmpty() 
	{
		return queue.isEmpty();
	}

	public synchronized int size()
	{
		return queue.size();
	}
	
	public synchronized E get(int i)
	{
		return queue.get(i);
	}
	
	public synchronized boolean contains(E item)
	{
		return queue.contains(item);
	}
}
