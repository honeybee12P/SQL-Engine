package bPlusTree;

import java.util.Properties;
import java.util.ArrayList;
import java.util.Arrays;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;
import jdbm.helper.StringComparator;
import jdbm.helper.IntegerComparator;

import jdbm.btree.BTree;

// Jython - Custom Java Interfacing libraries with Python
import bPlusTree.interfaces.debugInterface;
import bPlusTree.interfaces.bptpdInterface;

public class BPT
{
	String DATABASE;
	String BTREE_NAME;
	
	int	iFlag;
	
	RecordManager recman;
	long          recid;
	Tuple         tuple;
	TupleBrowser  browser;
	BTree         tree;
	Properties    props;
	
	public BPT (String psDBName, String psBPTName)
	{
		this.DATABASE	= psDBName;
		this.BTREE_NAME	= psBPTName;
		
		this.iFlag	= 0;
		
		this.props	= new Properties();
		this.tuple	= new Tuple ();
		
		try
		{
			// open database and setup an object cache
			this.recman	= RecordManagerFactory.createRecordManager (this.DATABASE, this.props);
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
		
		return;
	}
	
	public int getNumRecords ()
	{
		try
		{
			// try to reload an existing B+Tree
			this.recid	= this.recman.getNamedObject (this.BTREE_NAME);
			
			if (this.recid != 0)
			{
				this.tree = BTree.load (this.recman, this.recid);
				
				return this.tree.size ();
			}
			else
			{
				return 0;
			}
		}
		catch (Exception je)
		{
			je.printStackTrace();
		}
		
		return -1;
	}
	
	public void TblInsert (int piKey, ArrayList <byte[]> palValue)
	{
		try
		{
			// try to reload an existing B+Tree
			this.recid	= this.recman.getNamedObject (this.BTREE_NAME);
			
			if (this.recid != 0)
			{
				this.tree = BTree.load (this.recman, this.recid);
			}
			else
			{
				// create a new B+Tree data structure and use a IntegerComparator
				// to order the records based on people's name.
				this.tree = BTree.createInstance (this.recman, new IntegerComparator (), null, null, 32);
				this.recman.setNamedObject (BTREE_NAME, this.tree.getRecid ());
			}
			
			this.tree.insert (piKey, palValue, false);
			
			// Make the data persistent in the database
			this.recman.commit();
		}
		catch (Exception je)
		{
			je.printStackTrace();
		}
	}
	
	public void IdxInsert (String psKey, ArrayList <byte[]> palValue)
	{
		try
		{
			// try to reload an existing B+Tree
			this.recid	= this.recman.getNamedObject (this.BTREE_NAME);
			
			if (this.recid != 0)
			{
				this.tree = BTree.load (this.recman, this.recid);
			}
			else
			{
				// create a new B+Tree data structure and use a IntegerComparator
				// to order the records based on people's name.
				this.tree = BTree.createInstance (this.recman, new StringComparator (), null, null, 32);
				this.recman.setNamedObject (BTREE_NAME, this.tree.getRecid ());
			}
			
			this.tree.insert (psKey, palValue, true);
			
			// Make the data persistent in the database
			this.recman.commit();
		}
		catch (Exception je)
		{
			je.printStackTrace();
		}
	}
	
	public void TblUpdate (int piKey, ArrayList <byte[]> palValue)
	{
		try
		{
			// try to reload an existing B+Tree
			this.recid	= this.recman.getNamedObject (this.BTREE_NAME);
			
			if (this.recid != 0)
			{
				this.tree = BTree.load (this.recman, this.recid);
			}
			else
			{
				// create a new B+Tree data structure and use a IntegerComparator
				// to order the records based on people's name.
				this.tree = BTree.createInstance (this.recman, new IntegerComparator (), null, null, 32);
				this.recman.setNamedObject (BTREE_NAME, this.tree.getRecid ());
			}
			
			this.tree.insert (piKey, palValue, true);
			
			// Make the data persistent in the database
			this.recman.commit();
		}
		catch (Exception je)
		{
			je.printStackTrace();
		}
	}
	
	public void TblDrop (int piKey)
	{
		try
		{
			this.tree.remove (piKey);
			
			// Reset the iFlag
			this.iFlag	= 0;
			
			// Make the data persistent in the database
			this.recman.commit ();
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
	}
	
	public void IdxDrop (String psKey, String psDataType)
	{
		try
		{
			// try to reload an existing B+Tree
			this.recid	= this.recman.getNamedObject (this.BTREE_NAME);
			
			if (this.recid != 0)
			{
				this.tree = BTree.load (this.recman, this.recid);
			}
			else
			{
				// create a new B+Tree data structure and use a IntegerComparator
				// to order the records based on people's name.
				this.tree = BTree.createInstance (this.recman, new StringComparator (), null, null, 32);
				this.recman.setNamedObject (BTREE_NAME, this.tree.getRecid ());
			}
			
			if (psKey == null || psKey.equals ("null"))
			{
				psKey	= "NULL";
			}
			
			this.tree.remove (psKey);
			
			// Reset the iFlag
			this.iFlag	= 0;
			
			// Make the data persistent in the database
			this.recman.commit ();
		}
		catch (Exception je)
		{
			// Do Nothing if a Key is not found as Index file is properly mainteined
			// still if a key is absent.
			;
		}
	}
	
	public void BPTPrint ()
	{
		try
		{
			this.browser = this.tree.browse ();
            		
			while (this.browser.getNext (this.tuple) && this.tuple != null && this.tuple.getKey () != null)
			{
                		System.out.println ("Key : " + this.tuple.getKey ());
				System.out.println ("Value : " + (Arrays.toString ((byte []) ((ArrayList <byte []>) this.tuple.getValue ()).get(0))));
			}
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
	}
	
	public Tuple BPTBrowse (int piOrder)
	{
		// piFlag 0/1	-> In Order / Reverse Order
		try
		{
			if (this.iFlag == 0)
			{
				// try to reload an existing B+Tree
				this.recid	= this.recman.getNamedObject (this.BTREE_NAME);
				
				if (this.recid != 0)
				{
					this.tree = BTree.load (this.recman, this.recid);
				}
				else
				{
					// create a new B+Tree data structure and use a IntegerComparator
					// to order the records based on people's name.
					this.tree = BTree.createInstance (this.recman, new IntegerComparator (), null, null, 32);
					this.recman.setNamedObject (BTREE_NAME, this.tree.getRecid ());
				}
				
				this.browser = this.tree.browse ();
					
			}
			
			if (piOrder == 0)
			{
				if (this.iFlag == 0)
				{
					this.iFlag	= 1;
				}
				
				if (this.browser.getNext (this.tuple))
				{
					return this.tuple;
				}
				else
				{
					this.iFlag	= 0;
					
					return null;
				}
			}
			else
			{
				if (this.iFlag == 0)
				{
					browser = tree.browse (null);
					
					this.iFlag	= 1;
					
					return this.tuple;
				}
				
				if (this.browser.getPrevious (this.tuple))
				{
					return this.tuple;
				}
				else
				{
					this.iFlag	= 0;
					
					return null;
				}
			}
		}
		catch (Exception je)
		{
			this.iFlag	= 0;
			
			return null;
		}
	}
	
	public int BPTIdxSearch (String psKey)
	{
		String	sKey	= "";
		
		try
		{
			// try to reload an existing B+Tree
			this.recid	= this.recman.getNamedObject (this.BTREE_NAME);
			
			if (this.recid != 0)
			{
				this.tree = BTree.load (this.recman, this.recid);
			}
			else
			{
				// create a new B+Tree data structure and use a IntegerComparator
				// to order the records based on people's name.
				this.tree = BTree.createInstance (this.recman, new StringComparator (), null, null, 32);
				this.recman.setNamedObject (BTREE_NAME, this.tree.getRecid());
				
				return 0;
			}
			
			this.browser = this.tree.browse (psKey);
			
			while (this.browser.getNext (this.tuple))
			{
				sKey = (String) this.tuple.getKey();
				
				if (sKey.equals (psKey))	
				{
					return 1;
				}
				else
				{
					break;
				}
			}
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
			
		return 0;
	}
}
