package bPlusTree;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

// Jython - Java Interfacing libraries with Python
import org.python.core.PyException;
import org.python.core.PyInteger;
import org.python.core.PyObject;
import org.python.core.PyList;

// Jython - Custom Java Interfacing libraries with Python
import bPlusTree.interfaces.debugInterface;
import bPlusTree.interfaces.bptpdInterface;

import bPlusTree.javaBPTMain;
import bPlusTree.BPT;

public class javaBPTTblInsert extends javaBPTMain
{
	// BPlus Tree Header information Variables
	
	protected	int	iPageType;
	protected	int	iNumCellsPP;	// Number of Cells per page
	protected	int 	iStartOfCCA;	// Start of Cell Content Area
	protected	int	iRightPtr;	// Right Child Page Pointer (Holds Page Number)
	protected	int	iCellsOffset;	// 2*n byte holding the start offset of each Cell(Record) in the page
	protected	int	iPageFragment;	// Number of Free Bytes per page
	
	protected	int	iBranchingFactor;
	protected	int	iMinLeaf;
	protected	int	iMaxLeaf;
	protected	int	iMinInt;
	protected	int	iMaxInt;
	protected	int	iMinRootLeaf;
	protected	int	iMaxRootLeaf;
	protected	int	iMinRootInt;
	protected	int	iMaxRootInt;
	
	protected 	Iterator	listIterator;
	protected 	Iterator	listIterator2;
	
	private		String	sColumnName;
	private		String	sColumnDataType;
	private		String	sColumnValue;
	
	private		int	iColumnOrdPos;
	private		int	iColumnSerialCode;
	private		int	iColumnDataTypeLength;
	private		int	iNumColumns;
	private		int	iRecordSize;
	private		int	iCtr;
	private		int	iCtr2;
	private		int	iLCPageNum;
	private		int	iNumIntKeys;	// Holds the number of internal keys
	private		int	iNumIntNodes;	// Holds the number of internal nodes
	
	private		long	liCurrentPage;
	private		long	liCurrentPosToWrite;	// Holds the offset in page where to write the new record
	
	private		byte[]	bByteArray;
	
	private		PyList	pyoList2;
	
	private		ArrayList<PyList>	alList;
	private		ArrayList<byte[]>	alList2;
	
	private		BPT	oBPT;	
	
	public javaBPTTblInsert (String psFile, String psTmpFile, String psDBName, String psTblName, PyObject poDebug, PyObject poBPTPD)
	{
		super (psFile, psTmpFile, psDBName, psTblName, poDebug, poBPTPD);
		
		// Constructor
		this.iPageType		= 0;
		this.iNumCellsPP	= 0;
		this.iStartOfCCA	= 0;
		this.iRightPtr		= 0;
		this.iCellsOffset	= 0;
		this.iPageFragment	= 0;
		
		this.iNumColumns	= 0;
		this.iRecordSize	= 0;
		
		this.liCurrentPage		= 0;
		this.liCurrentPosToWrite	= 0;
		
		// Define B-Plus Tree Attributes here
		this.iBranchingFactor	= super.pyoBPTPD.getBranchingFactor ();
		
		this.iMinLeaf		= (int) (Math.ceil (this.iBranchingFactor / 2));
		this.iMaxLeaf		= this.iBranchingFactor - 1;
		this.iMinInt		= (int) (Math.ceil (this.iBranchingFactor / 2));
		this.iMaxInt		= this.iBranchingFactor;
		this.iMinRootLeaf	= 1;
		this.iMaxRootLeaf	= this.iBranchingFactor - 1;
		this.iMinRootInt	= 2;
		this.iMaxRootInt	= this.iBranchingFactor;
		
		this.iNumIntNodes	= 0;
		
		this.oBPT	= new BPT (psDBName, psTblName);
		
		return;
	}
	
	public int getBTreeRecCount ()
	{
		return this.oBPT.getNumRecords ();
	}
	
	public void tmpInsert (PyList pyoList, int piNumColumns, int piRecordSize)
	{
		int	iTreeKey	= -1;
		
		try
		{
			this.iNumColumns	= piNumColumns;
			this.iRecordSize	= piRecordSize;
			
			super.openTmpFile ();
			
			this.listIterator	= pyoList.iterator ();
			
			// Initialize an Array list
			this.alList		= new ArrayList<PyList> ();
			
			// The following loop parses each column in the record
			while (this.listIterator.hasNext ())
			{
				this.pyoList2		= (PyList) this.listIterator.next ();
				
				this.listIterator2	= this.pyoList2.iterator ();
				
				// Sort the pyoList2 based on Columns Ordinal position
				if (this.listIterator2.hasNext ())
				{
					// Get the Table Column's Ordinal Position
					this.iColumnOrdPos	= Integer.parseInt ((String) this.listIterator2.next ());
					
					if (alList.size () < this.iColumnOrdPos - 1)
					{
						// This loop allows to add into ArrayList out of order
						while (alList.size() < this.iColumnOrdPos - 1)
						{
							alList.add (alList.size(), null);
    						}
					}	
					
					// Remove the spurious null if present so as to maintain consistency in array.
					if (alList.size () > this.iColumnOrdPos - 1)
					{
						alList.remove (this.iColumnOrdPos - 1);
					}
					
					this.alList.add (this.iColumnOrdPos - 1, this.pyoList2);
				}
			}
			
			// Two Byte is added to encode the Size of the Payload
			this.iRecordSize	= this.iRecordSize + 2;
			
			// One Byte is added to encode the Number of Columns
			this.iRecordSize	= this.iRecordSize + 1;
			
			// One Byte is added to encode the Serial Type Code per each Column
			this.iRecordSize	= this.iRecordSize + this.iNumColumns;
			
			/* Following are the Cell Headers */
			// Write a SMALLINT indicating total payload size (Excluding its size)
			super.oRAF2.writeShort (this.iRecordSize - 2);
			
			// Write a TINYINT Indicating the number of columns
			super.oRAF2.writeByte (this.iNumColumns);
			
			// Write the Serial Codes for the column
			for (this.iCtr = 0; this.iCtr < this.alList.size(); this.iCtr++)
			{
				// Re-assign the list and iterators here
				this.pyoList2		= (PyList) alList.get (iCtr);
				
				this.listIterator2	= this.pyoList2.iterator ();
				
				// Get the Table Column's Ordinal Position
				this.iColumnOrdPos	= Integer.parseInt ((String) this.listIterator2.next ());
				
				// Get the Table Column Name
				this.sColumnName	= (String) this.listIterator2.next ();
				
				// Get the Column Data Type
				this.sColumnDataType	= (String) this.listIterator2.next ();
				
				// Get the Column Value
				this.sColumnValue	= (String) this.listIterator2.next ();
				
				// Get the Column Serial Code - Hex to Integer Conversion is done as well
				this.iColumnSerialCode	= Integer.decode ((String) this.listIterator2.next ());
				
				// Get the Column Data Type Length in Bytes - Value is converted to Integer
				this.iColumnDataTypeLength	= Integer.parseInt ((String) this.listIterator2.next ());
				
				writeSerialCodes (super.oRAF2);
			}
			
			// The following loop parses each column's information in the record
			for (this.iCtr = 0; this.iCtr < this.alList.size(); this.iCtr++)
			{
				// Re-assign the list and iterators here
				this.pyoList2		= (PyList) alList.get (iCtr);
				
				this.listIterator2	= this.pyoList2.iterator ();
				
				// Get the Table Column's Ordinal Position
				this.iColumnOrdPos	= Integer.parseInt ((String) this.listIterator2.next ());
				
				// Get the Table Column Name
				this.sColumnName	= (String) this.listIterator2.next ();
				
				// Get the Column Data Type
				this.sColumnDataType	= (String) this.listIterator2.next ();
				
				// Get the Column Value
				this.sColumnValue	= (String) this.listIterator2.next ();
				
				// Get the Column Serial Code - Hex to Integer Conversion is done as well
				this.iColumnSerialCode	= Integer.decode ((String) this.listIterator2.next ());
				
				// Get the Column Data Type Length in Bytes - Value is converted to Integer
				this.iColumnDataTypeLength	= Integer.parseInt ((String) this.listIterator2.next ());
				
				if (this.sColumnName.equals ("ROWID"))
				{
					iTreeKey	= Integer.parseInt (this.sColumnValue);
				}
				
				// Write to the File here
				writeColumns (super.oRAF2);
			}
			
			// Seek to the start of the Temporary File
			super.oRAF2.seek (0);
			
			// Read all the data into a Byte Array from this file
			this.bByteArray	= new byte [this.iRecordSize];
			
			super.oRAF2.readFully (bByteArray);
			
			// Clear the contents from the file
			super.oRAF2.setLength (0);
			
			alList2	= new ArrayList<byte[]> ();
			
			alList2.add (bByteArray);
			
			// Insert this value into B Plus Tree Array
			this.oBPT.TblInsert (iTreeKey, alList2);
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
		
		return;
	}
	
	public void insert (PyList pyoList, int piNumColumns, int piRecordSize)
	{
		tmpInsert (pyoList, piNumColumns, piRecordSize);
	}
	
	public void tmpUpdate (PyList pyoList, int piNumColumns, int piRecordSize)
	{
		int	iTreeKey	= -1;
		
		try
		{
			this.iNumColumns	= piNumColumns;
			this.iRecordSize	= piRecordSize;
			
			super.openTmpFile ();
			
			this.listIterator	= pyoList.iterator ();
			
			// Initialize an Array list
			this.alList		= new ArrayList<PyList> ();
			
			// The following loop parses each column in the record
			while (this.listIterator.hasNext ())
			{
				this.pyoList2		= (PyList) this.listIterator.next ();
				
				this.listIterator2	= this.pyoList2.iterator ();
				
				// Sort the pyoList2 based on Columns Ordinal position
				if (this.listIterator2.hasNext ())
				{
					// Get the Table Column's Ordinal Position
					this.iColumnOrdPos	= Integer.parseInt ((String) this.listIterator2.next ());
					
					if (alList.size () < this.iColumnOrdPos - 1)
					{
						// This loop allows to add into ArrayList out of order
						while (alList.size() < this.iColumnOrdPos - 1)
						{
							alList.add (alList.size(), null);
    						}
					}	
					
					// Remove the spurious null if present so as to maintain consistency in array.
					if (alList.size () > this.iColumnOrdPos - 1)
					{
						alList.remove (this.iColumnOrdPos - 1);
					}
					
					this.alList.add (this.iColumnOrdPos - 1, this.pyoList2);
				}
			}
			
			// Two Byte is added to encode the Size of the Payload
			this.iRecordSize	= this.iRecordSize + 2;
			
			// One Byte is added to encode the Number of Columns
			this.iRecordSize	= this.iRecordSize + 1;
			
			// One Byte is added to encode the Serial Type Code per each Column
			this.iRecordSize	= this.iRecordSize + this.iNumColumns;
			
			/* Following are the Cell Headers */
			// Write a SMALLINT indicating total payload size (Excluding its size)
			super.oRAF2.writeShort (this.iRecordSize - 2);
			
			// Write a TINYINT Indicating the number of columns
			super.oRAF2.writeByte (this.iNumColumns);
			
			// Write the Serial Codes for the column
			for (this.iCtr = 0; this.iCtr < this.alList.size(); this.iCtr++)
			{
				// Re-assign the list and iterators here
				this.pyoList2		= (PyList) alList.get (iCtr);
				
				this.listIterator2	= this.pyoList2.iterator ();
				
				// Get the Table Column's Ordinal Position
				this.iColumnOrdPos	= Integer.parseInt ((String) this.listIterator2.next ());
				
				// Get the Table Column Name
				this.sColumnName	= (String) this.listIterator2.next ();
				
				// Get the Column Data Type
				this.sColumnDataType	= (String) this.listIterator2.next ();
				
				// Get the Column Value
				this.sColumnValue	= (String) this.listIterator2.next ();
				
				// Get the Column Serial Code - Hex to Integer Conversion is done as well
				this.iColumnSerialCode	= Integer.decode ((String) this.listIterator2.next ());
				
				// Get the Column Data Type Length in Bytes - Value is converted to Integer
				this.iColumnDataTypeLength	= Integer.parseInt ((String) this.listIterator2.next ());
				
				writeSerialCodes (super.oRAF2);
			}
			
			// The following loop parses each column's information in the record
			for (this.iCtr = 0; this.iCtr < this.alList.size(); this.iCtr++)
			{
				// Re-assign the list and iterators here
				this.pyoList2		= (PyList) alList.get (iCtr);
				
				this.listIterator2	= this.pyoList2.iterator ();
				
				// Get the Table Column's Ordinal Position
				this.iColumnOrdPos	= Integer.parseInt ((String) this.listIterator2.next ());
				
				// Get the Table Column Name
				this.sColumnName	= (String) this.listIterator2.next ();
				
				// Get the Column Data Type
				this.sColumnDataType	= (String) this.listIterator2.next ();
				
				// Get the Column Value
				this.sColumnValue	= (String) this.listIterator2.next ();
				
				// Get the Column Serial Code - Hex to Integer Conversion is done as well
				this.iColumnSerialCode	= Integer.decode ((String) this.listIterator2.next ());
				
				// Get the Column Data Type Length in Bytes - Value is converted to Integer
				this.iColumnDataTypeLength	= Integer.parseInt ((String) this.listIterator2.next ());
				
				if (this.sColumnName.equals ("ROWID"))
				{
					iTreeKey	= Integer.parseInt (this.sColumnValue);
				}
				
				// Write to the File here
				writeColumns (super.oRAF2);
			}
			
			// Seek to the start of the Temporary File
			super.oRAF2.seek (0);
			
			// Read all the data into a Byte Array from this file
			this.bByteArray	= new byte [this.iRecordSize];
			
			super.oRAF2.readFully (bByteArray);
			
			// Clear the contents from the file
			super.oRAF2.setLength (0);
			
			alList2	= new ArrayList<byte[]> ();
			
			alList2.add (bByteArray);
			
			// Insert this value into B Plus Tree Array
			this.oBPT.TblUpdate (iTreeKey, alList2);
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
		
		return;
	}
	
	public void update (PyList pyoList, int piNumColumns, int piRecordSize)
	{
		tmpUpdate (pyoList, piNumColumns, piRecordSize);
	}
	
	public void tmpInsertIndex (PyList pyoList, int piNumColumns, int piRecordSize, String psTreeKey)
	{
		int	iTreeKey	= -1;
		
		try
		{
			this.iNumColumns	= piNumColumns;
			this.iRecordSize	= piRecordSize;
			
			super.openTmpFile ();
			
			this.listIterator	= pyoList.iterator ();
			
			// Initialize an Array list
			this.alList		= new ArrayList<PyList> ();
			
			// The following loop parses each column in the record
			while (this.listIterator.hasNext ())
			{
				this.pyoList2		= (PyList) this.listIterator.next ();
				
				this.listIterator2	= this.pyoList2.iterator ();
				
				// Sort the pyoList2 based on Columns Ordinal position
				if (this.listIterator2.hasNext ())
				{
					// Get the Table Column's Ordinal Position
					this.iColumnOrdPos	= Integer.parseInt ((String) this.listIterator2.next ());
					
					if (alList.size () < this.iColumnOrdPos - 1)
					{
						// This loop allows to add into ArrayList out of order
						while (alList.size() < this.iColumnOrdPos - 1)
						{
							alList.add (alList.size(), null);
    						}
					}	
					
					// Remove the spurious null if present so as to maintain consistency in array.
					if (alList.size () > this.iColumnOrdPos - 1)
					{
						alList.remove (this.iColumnOrdPos - 1);
					}
					
					this.alList.add (this.iColumnOrdPos - 1, this.pyoList2);
				}
			}
			
			// Two Byte is added to encode the Size of the Payload
			this.iRecordSize	= this.iRecordSize + 2;
			
			// One Byte is added to encode the Number of Columns
			this.iRecordSize	= this.iRecordSize + 1;
			
			// One Byte is added to encode the Serial Type Code per each Column
			this.iRecordSize	= this.iRecordSize + this.iNumColumns;
			
			/* Following are the Cell Headers */
			// Write a SMALLINT indicating total payload size (Excluding its size)
			super.oRAF2.writeShort (this.iRecordSize - 2);
			
			// Write a TINYINT Indicating the number of columns
			super.oRAF2.writeByte (this.iNumColumns);
			
			// Write the Serial Codes for the column
			for (this.iCtr = 0; this.iCtr < this.alList.size(); this.iCtr++)
			{
				// Re-assign the list and iterators here
				this.pyoList2		= (PyList) alList.get (iCtr);
				
				this.listIterator2	= this.pyoList2.iterator ();
				
				// Get the Table Column's Ordinal Position
				this.iColumnOrdPos	= Integer.parseInt ((String) this.listIterator2.next ());
				
				// Get the Table Column Name
				this.sColumnName	= (String) this.listIterator2.next ();
				
				// Get the Column Data Type
				this.sColumnDataType	= (String) this.listIterator2.next ();
				
				// Get the Column Value
				this.sColumnValue	= (String) this.listIterator2.next ();
				
				// Get the Column Serial Code - Hex to Integer Conversion is done as well
				this.iColumnSerialCode	= Integer.decode ((String) this.listIterator2.next ());
				
				// Get the Column Data Type Length in Bytes - Value is converted to Integer
				this.iColumnDataTypeLength	= Integer.parseInt ((String) this.listIterator2.next ());
				
				writeSerialCodes (super.oRAF2);
			}
			
			// The following loop parses each column's information in the record
			for (this.iCtr = 0; this.iCtr < this.alList.size(); this.iCtr++)
			{
				// Re-assign the list and iterators here
				this.pyoList2		= (PyList) alList.get (iCtr);
				
				this.listIterator2	= this.pyoList2.iterator ();
				
				// Get the Table Column's Ordinal Position
				this.iColumnOrdPos	= Integer.parseInt ((String) this.listIterator2.next ());
				
				// Get the Table Column Name
				this.sColumnName	= (String) this.listIterator2.next ();
				
				// Get the Column Data Type
				this.sColumnDataType	= (String) this.listIterator2.next ();
				
				// Get the Column Value
				this.sColumnValue	= (String) this.listIterator2.next ();
				
				// Get the Column Serial Code - Hex to Integer Conversion is done as well
				this.iColumnSerialCode	= Integer.decode ((String) this.listIterator2.next ());
				
				// Get the Column Data Type Length in Bytes - Value is converted to Integer
				this.iColumnDataTypeLength	= Integer.parseInt ((String) this.listIterator2.next ());
				
				// Write to the File here
				writeColumns (super.oRAF2);
			}
			
			// Seek to the start of the Temporary File
			super.oRAF2.seek (0);
			
			// Read all the data into a Byte Array from this file
			this.bByteArray	= new byte [this.iRecordSize];
			
			super.oRAF2.readFully (bByteArray);
			
			// Clear the contents from the file
			super.oRAF2.setLength (0);
			
			alList2	= new ArrayList<byte[]> ();
			
			alList2.add (bByteArray);
			
			// Insert this value into B Plus Tree Array
			this.oBPT.IdxInsert (psTreeKey, alList2);
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
		
		return;
	}
	
	public void insertIndex (PyList pyoList, int piNumColumns, int piRecordSize, String psTreeKey)
	{
		tmpInsertIndex (pyoList, piNumColumns, piRecordSize, psTreeKey);
	}
	
	public int IdxFind (String psKey)
	{
		return this.oBPT.BPTIdxSearch (psKey);	
	}
		
	public void write ()
	{
		writeLeaf ();
		
		try
		{
			if (super.oRAF.length () > super.liPageSize)
			{
				writeInterior ();
			}
			
			// Root is written alongside writeInterior () function
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
	}
	
	public void writeIndex ()
	{
		writeLeafIndex ();
		
		try
		{
			if (super.oRAF.length () > super.liPageSize)
			{
				writeInteriorIndex ();
			}
			
			// Root is written alongside writeInterior () function
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
	}
	
	public void writeInterior ()
	{
		Tuple	oTuple;
		
		int	iKey		= 0;
		
		int	iRecCount	= 0;
		
		try
		{
			// Build the custom tbl file here
			
			// Reset the variables here
			this.iPageType		= 0;
			this.iNumCellsPP	= 0;
			this.iStartOfCCA	= 0;
			this.iRightPtr		= 0;
			this.iCellsOffset	= 0;
			this.iPageFragment	= 0;
			
			this.iNumColumns	= 0;
			this.iRecordSize	= 0;
			
			this.liCurrentPage		= 0;
			this.liCurrentPosToWrite	= 0;
			
			// Start by pointing to the left leaf which is page 1
			this.iCtr		= 1;
			
			this.iLCPageNum		= 1;
			
			// Seek to end of File
			super.oRAF.seek (super.oRAF.length());
			
			// Extend Page for Internal Nodes
			extendPage ();
			
			pageDesignator (super.pyoBPTPD.getInteriorTbl (), super.oRAF);
			
			while (true)
			{
				oTuple	= this.oBPT.BPTBrowse (0);
				
				if (oTuple != null)
				{
					iKey	= (Integer) oTuple.getKey ();
				}
				else
				{
					break;
				}
				
				for (this.iCtr = 0; this.iCtr < this.alList2.size(); this.iCtr++)
				{
					// Root page starts at Page Number 0
					this.liCurrentPage	= (long) (Math.floor (super.oRAF.getFilePointer () / super.liPageSize));
					
					super.oRAF.seek (this.liCurrentPage * super.liPageSize);
					
					// Get the Binary Plus Tree Page Type
					this.iPageType		= super.oRAF.readByte ();
					
					// Get the Number of Cells (Records) per page
					this.iNumCellsPP	= super.oRAF.readByte ();
					
					// Get the starting offset (from the start of page offset) of the first cell in the page - Cell Content Area
					this.iStartOfCCA	= super.oRAF.readUnsignedShort ();
					
					// Get the value of the Right Pointer
					this.iRightPtr		= super.oRAF.readInt ();
					
					// Till now the Fixed Standard header size is 8 Byte
					// Seek the pointer to point to the byte after the 0x08 Offset, containing the starting offset of each cells
					super.oRAF.seek (super.oRAF.getFilePointer () + (iNumCellsPP * 2));
					
					this.iPageFragment	= (int) (super.oRAF.getFilePointer () - (this.liCurrentPage * super.liPageSize));
					
					if (this.iStartOfCCA > 0)
					{
						this.iPageFragment	= (int) ((super.liPageSize - this.iStartOfCCA) + this.iPageFragment);
						this.iPageFragment	= (int) (super.liPageSize - this.iPageFragment);
					}
					else
					{
						this.iPageFragment	= (int) (super.liPageSize - this.iPageFragment);
					}
					
					super.pyoDebug.writeJava (String.format ("Free Bytes in Leaf page = [%d]", this.iPageFragment));
					
					this.iCtr2	= this.iNumCellsPP + 1;
					
					// Interior Cell is always a constant 8 Byte length
					this.iRecordSize	= 8;
					
					// Determine the number of Interior Keys to be written
					this.iNumIntKeys	= (int) (Math.floor (getBTreeRecCount () / (this.iBranchingFactor - 1)));
					
					// Once all the required values are extracted, determine the page to write data into
					if (this.iNumIntKeys > 0)
					{
						if (this.iCtr != 0)
						{
							iRecCount	+= this.iCtr;
						}
						else
						{
							iRecCount	+= 1;
						}
						
						if (iRecCount % (this.iBranchingFactor - 1) == 0)
						{
							this.pageDeterminator ("INTERIOR");
							
							// Write to the File here
							super.oRAF.writeInt (this.iLCPageNum);		// 4 Byte Page Number - Left Child Pointer
							super.oRAF.writeInt (iKey);			// Integer Key
							
							markPageHeader ();
							
							iRecCount	= 0;
							
							this.iLCPageNum	+= 1;
						}
					}
				}
			}
			
			this.iCtr2	= 0;
			iRecCount	= 0;
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
		
		return;
	}
	
	public void writeLeaf ()
	{
		Tuple	oTuple;
		
		try
		{
			// Build the custom tbl file here
			
			// Reset the variables here
			this.iPageType		= 0;
			this.iNumCellsPP	= 0;
			this.iStartOfCCA	= 0;
			this.iRightPtr		= 0;
			this.iCellsOffset	= 0;
			this.iPageFragment	= 0;
			
			this.iNumColumns	= 0;
			this.iRecordSize	= 0;
			
			this.liCurrentPage		= 0;
			this.liCurrentPosToWrite	= 0;
			
			super.openFile ();
			
			while (true)
			{
				oTuple	= this.oBPT.BPTBrowse (0);
				
				if (oTuple != null)
				{
					this.alList2  = (ArrayList <byte[]>)oTuple.getValue ();
				}
				else
				{
					break;
				}
				
				for (this.iCtr = 0; this.iCtr < this.alList2.size(); this.iCtr++)
				{
					// Root page starts at Page Number 0
					this.liCurrentPage	= (long) (Math.floor (super.oRAF.getFilePointer () / super.liPageSize));
					
					super.oRAF.seek (this.liCurrentPage * super.liPageSize);
					
					// Get the Binary Plus Tree Page Type
					this.iPageType		= super.oRAF.readByte ();
					
					// Get the Number of Cells (Records) per page
					this.iNumCellsPP	= super.oRAF.readByte ();
					
					// Get the starting offset (from the start of page offset) of the first cell in the page - Cell Content Area
					this.iStartOfCCA	= super.oRAF.readUnsignedShort ();
					
					// Get the value of the Right Pointer
					this.iRightPtr		= super.oRAF.readInt ();
					
					// Till now the Fixed Standard header size is 8 Byte
					// Seek the pointer to point to the byte after the 0x08 Offset, containing the starting offset of each cells
					super.oRAF.seek (super.oRAF.getFilePointer () + (iNumCellsPP * 2));
					
					this.iPageFragment	= (int) (super.oRAF.getFilePointer () - (this.liCurrentPage * super.liPageSize));
					
					if (this.iStartOfCCA > 0)
					{
						this.iPageFragment	= (int) ((super.liPageSize - this.iStartOfCCA) + this.iPageFragment);
						this.iPageFragment	= (int) (super.liPageSize - this.iPageFragment);
					}
					else
					{
						this.iPageFragment	= (int) (super.liPageSize - this.iPageFragment);
					}
					
					super.pyoDebug.writeJava (String.format ("Free Bytes in Leaf page = [%d]", this.iPageFragment));
					
					this.iCtr2	= this.iNumCellsPP + 1;
					
					this.iRecordSize	= ((byte []) alList2.get (iCtr)).length;
					
					// Once all the required values are extracted, determine the page to write data into
					this.pageDeterminator ("LEAF");
					
					// Write to the File here
					super.oRAF.write ((byte []) alList2.get (iCtr));
					
					markPageHeader ();
				}
			}
			
			this.iCtr2	= 0;
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
		
		return;
	}
	
	public void writeInteriorIndex ()
	{
		Tuple	oTuple;
		
		String	sKey		= "";
		
		int	iRecCount	= 0;
		
		try
		{
			// Build the custom tbl file here
			
			// Reset the variables here
			this.iPageType		= 0;
			this.iNumCellsPP	= 0;
			this.iStartOfCCA	= 0;
			this.iRightPtr		= 0;
			this.iCellsOffset	= 0;
			this.iPageFragment	= 0;
			
			this.iNumColumns	= 0;
			this.iRecordSize	= 0;
			
			this.liCurrentPage		= 0;
			this.liCurrentPosToWrite	= 0;
			
			// Start by pointing to the left leaf which is page 1
			this.iCtr		= 1;
			
			this.iLCPageNum		= 1;
			
			// Seek to end of File
			super.oRAF.seek (super.oRAF.length());
			
			// Extend Page for Internal Nodes
			extendPage ();
			
			pageDesignator (super.pyoBPTPD.getInteriorIdx (), super.oRAF);
			
			while (true)
			{
				oTuple	= this.oBPT.BPTBrowse (0);
				
				if (oTuple != null)
				{
					sKey	= (String) oTuple.getKey ();
				}
				else
				{
					break;
				}
				
				for (this.iCtr = 0; this.iCtr < this.alList2.size(); this.iCtr++)
				{
					// Root page starts at Page Number 0
					this.liCurrentPage	= (long) (Math.floor (super.oRAF.getFilePointer () / super.liPageSize));
					
					super.oRAF.seek (this.liCurrentPage * super.liPageSize);
					
					// Get the Binary Plus Tree Page Type
					this.iPageType		= super.oRAF.readByte ();
					
					// Get the Number of Cells (Records) per page
					this.iNumCellsPP	= super.oRAF.readByte ();
					
					// Get the starting offset (from the start of page offset) of the first cell in the page - Cell Content Area
					this.iStartOfCCA	= super.oRAF.readUnsignedShort ();
					
					// Get the value of the Right Pointer
					this.iRightPtr		= super.oRAF.readInt ();
					
					// Till now the Fixed Standard header size is 8 Byte
					// Seek the pointer to point to the byte after the 0x08 Offset, containing the starting offset of each cells
					super.oRAF.seek (super.oRAF.getFilePointer () + (iNumCellsPP * 2));
					
					this.iPageFragment	= (int) (super.oRAF.getFilePointer () - (this.liCurrentPage * super.liPageSize));
					
					if (this.iStartOfCCA > 0)
					{
						this.iPageFragment	= (int) ((super.liPageSize - this.iStartOfCCA) + this.iPageFragment);
						this.iPageFragment	= (int) (super.liPageSize - this.iPageFragment);
					}
					else
					{
						this.iPageFragment	= (int) (super.liPageSize - this.iPageFragment);
					}
					
					super.pyoDebug.writeJava (String.format ("Free Bytes in Leaf page = [%d]", this.iPageFragment));
					
					this.iCtr2	= this.iNumCellsPP + 1;
					
					// Interior Cell is always a constant 8 Byte length
					this.iRecordSize	= 8;
					
					// Determine the number of Interior Keys to be written
					this.iNumIntKeys	= (int) (Math.floor (getBTreeRecCount () / (this.iBranchingFactor - 1)));
					
					// Once all the required values are extracted, determine the page to write data into
					if (this.iNumIntKeys > 0)
					{
						if (this.iCtr != 0)
						{
							iRecCount	+= this.iCtr;
						}
						else
						{
							iRecCount	+= 1;
						}
						
						if (iRecCount % (this.iBranchingFactor - 1) == 0)
						{
							this.pageDeterminatorIndex ("INTERIOR");
							
							// Write to the File here
							super.oRAF.writeInt (this.iLCPageNum);		// 4 Byte Page Number - Left Child Pointer
							super.oRAF.writeBytes (sKey);			// Key
							
							markPageHeader ();
							
							iRecCount	= 0;
							
							this.iLCPageNum	+= 1;
						}
					}
				}
			}
			
			this.iCtr2	= 0;
			iRecCount	= 0;
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
		
		return;
	}
	
	public void writeLeafIndex ()
	{
		Tuple	oTuple;
		
		try
		{
			// Build the custom tbl file here
			
			// Reset the variables here
			this.iPageType		= 0;
			this.iNumCellsPP	= 0;
			this.iStartOfCCA	= 0;
			this.iRightPtr		= 0;
			this.iCellsOffset	= 0;
			this.iPageFragment	= 0;
			
			this.iNumColumns	= 0;
			this.iRecordSize	= 0;
			
			this.liCurrentPage		= 0;
			this.liCurrentPosToWrite	= 0;
			
			super.openFile ();
			
			// Overwrite the Page Type value for leaf index 
			pageDesignator (super.pyoBPTPD.getLeafIdx (), super.oRAF);
			
			while (true)
			{
				oTuple	= this.oBPT.BPTBrowse (0);
				
				if (oTuple != null)
				{
					this.alList2  = (ArrayList <byte[]>)oTuple.getValue ();
				}
				else
				{
					break;
				}
				
				for (this.iCtr = 0; this.iCtr < this.alList2.size(); this.iCtr++)
				{
					// Root page starts at Page Number 0
					this.liCurrentPage	= (long) (Math.floor (super.oRAF.getFilePointer () / super.liPageSize));

					super.oRAF.seek (this.liCurrentPage * super.liPageSize);
					
					// Get the Binary Plus Tree Page Type
					this.iPageType		= super.oRAF.readByte ();
					
					// Get the Number of Cells (Records) per page
					this.iNumCellsPP	= super.oRAF.readByte ();
					
					// Get the starting offset (from the start of page offset) of the first cell in the page - Cell Content Area
					this.iStartOfCCA	= super.oRAF.readUnsignedShort ();
					
					// Get the value of the Right Pointer
					this.iRightPtr		= super.oRAF.readInt ();
					
					// Till now the Fixed Standard header size is 8 Byte
					// Seek the pointer to point to the byte after the 0x08 Offset, containing the starting offset of each cells
					super.oRAF.seek (super.oRAF.getFilePointer () + (iNumCellsPP * 2));
					
					this.iPageFragment	= (int) (super.oRAF.getFilePointer () - (this.liCurrentPage * super.liPageSize));
					
					if (this.iStartOfCCA > 0)
					{
						this.iPageFragment	= (int) ((super.liPageSize - this.iStartOfCCA) + this.iPageFragment);
						this.iPageFragment	= (int) (super.liPageSize - this.iPageFragment);
					}
					else
					{
						this.iPageFragment	= (int) (super.liPageSize - this.iPageFragment);
					}
					
					super.pyoDebug.writeJava (String.format ("Free Bytes in Leaf page = [%d]", this.iPageFragment));
					
					this.iCtr2	= this.iNumCellsPP + 1;
					
					this.iRecordSize	= ((byte []) alList2.get (iCtr)).length;
					
					// Once all the required values are extracted, determine the page to write data into
					this.pageDeterminatorIndex ("LEAF");
					
					// Write to the File here
					super.oRAF.write ((byte []) alList2.get (iCtr));
					
					markPageHeader ();
				}
			}
			
			this.iCtr2	= 0;
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
		
		return;
	}
	
	public void printTree ()
	{
		this.oBPT.BPTPrint ();
	}
	
	// Determine to what page the data has to be written
	public void pageDeterminator (String pID)
	{
		try
		{
			if (pID == "LEAF")		// Leaf Nodes
			{
				if (this.iCtr2 == this.iBranchingFactor)
				{
					// Reset the counter
					this.iCtr2	= 0;
					
					// This page now becomes a new leaf page
					super.extendPage ();
					
					pageDesignator (super.pyoBPTPD.getLeafTbl (), super.oRAF);
					
					// Recalculate the values
					this.liCurrentPage	= (long) (Math.floor (super.oRAF.getFilePointer () / super.liPageSize));
					
					super.oRAF.seek (this.liCurrentPage * super.liPageSize);
					
					// Get the Binary Plus Tree Page Type
					this.iPageType		= super.oRAF.readByte ();
					
					// Get the Number of Cells (Records) per page
					this.iNumCellsPP	= super.oRAF.readByte ();
					
					// Get the starting offset (from the start of page offset) of the first cell in the page - Cell Content Area
					this.iStartOfCCA	= super.oRAF.readUnsignedShort ();
					
					// Get the value of the Right Pointer
					this.iRightPtr		= super.oRAF.readInt ();
					
					// Till now the Fixed Standard header size is 8 Byte
					// Seek the pointer to point to the byte after the 0x08 Offset, containing the starting offset of each cells
					super.oRAF.seek (super.oRAF.getFilePointer () + (iNumCellsPP * 2));
					
					this.iPageFragment	= (int) (super.oRAF.getFilePointer () - (this.liCurrentPage * super.liPageSize));
					
					if (this.iStartOfCCA > 0)
					{
						this.iPageFragment	= (int) ((super.liPageSize - this.iStartOfCCA) + this.iPageFragment);
						this.iPageFragment	= (int) (super.liPageSize - this.iPageFragment);
					}
					else
					{
						this.iPageFragment	= (int) (super.liPageSize - this.iPageFragment);
					}

					super.pyoDebug.writeJava (String.format ("Free Bytes in Leaf page = [%d]", this.iPageFragment));
				}
			}
			else if (pID == "INTERIOR")	// Non Leaf Node
			{
				if (this.iCtr2 == this.iBranchingFactor)
				{
					// Increment the Left Page Child Pointer'
					this.iLCPageNum	+= 1;
					
					// Reset the counter
					this.iCtr2	= 0;
					
					// Increment the Number of Internal nodes
					this.iNumIntNodes	+= 1;
					
					// This page now becomes a new leaf page
					super.extendPage ();
					
					pageDesignator (super.pyoBPTPD.getInteriorTbl (), super.oRAF);
					
					// Recalculate the values
					this.liCurrentPage	= (long) (Math.floor (super.oRAF.getFilePointer () / super.liPageSize));
					
					super.oRAF.seek (this.liCurrentPage * super.liPageSize);
					
					// Get the Binary Plus Tree Page Type
					this.iPageType		= super.oRAF.readByte ();
					
					// Get the Number of Cells (Records) per page
					this.iNumCellsPP	= super.oRAF.readByte ();
					
					// Get the starting offset (from the start of page offset) of the first cell in the page - Cell Content Area
					this.iStartOfCCA	= super.oRAF.readUnsignedShort ();
					
					// Get the value of the Right Pointer
					this.iRightPtr		= super.oRAF.readInt ();
					
					// Till now the Fixed Standard header size is 8 Byte
					// Seek the pointer to point to the byte after the 0x08 Offset, containing the starting offset of each cells
					super.oRAF.seek (super.oRAF.getFilePointer () + (iNumCellsPP * 2));

					this.iPageFragment	= (int) (super.oRAF.getFilePointer () - (this.liCurrentPage * super.liPageSize));
					
					if (this.iStartOfCCA > 0)
					{
						this.iPageFragment	= (int) ((super.liPageSize - this.iStartOfCCA) + this.iPageFragment);
						this.iPageFragment	= (int) (super.liPageSize - this.iPageFragment);
					}
					else
					{
						this.iPageFragment	= (int) (super.liPageSize - this.iPageFragment);
					}
					
					super.pyoDebug.writeJava (String.format ("Free Bytes in Leaf page = [%d]", this.iPageFragment));
				}
			}
			
			// Set the pointer to the correct location in the page
			
			this.liCurrentPosToWrite	= super.oRAF.getFilePointer () + (this.iPageFragment - this.iRecordSize);
			
			super.oRAF.seek (this.liCurrentPosToWrite);
		}
		catch (	Exception je)
		{
			je.printStackTrace ();
		}
		
		return;
	}
	
	// Determine to what page the data has to be written - Index Files
	public void pageDeterminatorIndex (String pID)
	{
		super.pyoDebug.writeJava (String.format ("pageDeterminatorIndex pID = [%s]", pID));
		
		try
		{
			super.pyoDebug.writeJava (String.format ("pageDeterminatorIndex iCtr2 = [%d], iBranchingFacotr = [%d]", this.iCtr2, this.iBranchingFactor));
			
			if (pID == "LEAF")		// Leaf Nodes
			{
				if (this.iCtr2 == this.iBranchingFactor)
				{
					// Reset the counter
					this.iCtr2	= 0;
					
					// This page now becomes a new leaf page
					super.extendPage ();
					
					pageDesignator (super.pyoBPTPD.getLeafIdx (), super.oRAF);
					
					// Recalculate the values
					this.liCurrentPage	= (long) (Math.floor (super.oRAF.getFilePointer () / super.liPageSize));
					
					super.oRAF.seek (this.liCurrentPage * super.liPageSize);
					
					// Get the Binary Plus Tree Page Type
					this.iPageType		= super.oRAF.readByte ();
					
					// Get the Number of Cells (Records) per page
					this.iNumCellsPP	= super.oRAF.readByte ();
					
					// Get the starting offset (from the start of page offset) of the first cell in the page - Cell Content Area
					this.iStartOfCCA	= super.oRAF.readUnsignedShort ();
					
					// Get the value of the Right Pointer
					this.iRightPtr		= super.oRAF.readInt ();
					
					// Till now the Fixed Standard header size is 8 Byte
					// Seek the pointer to point to the byte after the 0x08 Offset, containing the starting offset of each cells
					super.oRAF.seek (super.oRAF.getFilePointer () + (iNumCellsPP * 2));
					
					this.iPageFragment	= (int) (super.oRAF.getFilePointer () - (this.liCurrentPage * super.liPageSize));
					
					if (this.iStartOfCCA > 0)
					{
						this.iPageFragment	= (int) ((super.liPageSize - this.iStartOfCCA) + this.iPageFragment);
						this.iPageFragment	= (int) (super.liPageSize - this.iPageFragment);
					}
					else
					{
						this.iPageFragment	= (int) (super.liPageSize - this.iPageFragment);
					}
					
					super.pyoDebug.writeJava (String.format ("Free Bytes in Leaf page = [%d]", this.iPageFragment));
				}
			}
			else if (pID == "INTERIOR")	// Non Leaf Node
			{
				if (this.iCtr2 == this.iBranchingFactor)
				{
					// Increment the Left Page Child Pointer'
					this.iLCPageNum	+= 1;
					
					// Reset the counter
					this.iCtr2	= 0;
					
					// Increment the Number of Internal nodes
					this.iNumIntNodes	+= 1;
					
					// This page now becomes a new leaf page
					super.extendPage ();
					
					pageDesignator (super.pyoBPTPD.getInteriorIdx (), super.oRAF);
					
					// Recalculate the values
					this.liCurrentPage	= (long) (Math.floor (super.oRAF.getFilePointer () / super.liPageSize));

					super.oRAF.seek (this.liCurrentPage * super.liPageSize);

					// Get the Binary Plus Tree Page Type
					this.iPageType		= super.oRAF.readByte ();
					
					// Get the Number of Cells (Records) per page
					this.iNumCellsPP	= super.oRAF.readByte ();

					// Get the starting offset (from the start of page offset) of the first cell in the page - Cell Content Area
					this.iStartOfCCA	= super.oRAF.readUnsignedShort ();
					
					// Get the value of the Right Pointer
					this.iRightPtr		= super.oRAF.readInt ();
					
					// Till now the Fixed Standard header size is 8 Byte
					// Seek the pointer to point to the byte after the 0x08 Offset, containing the starting offset of each cells
					super.oRAF.seek (super.oRAF.getFilePointer () + (iNumCellsPP * 2));
					
					this.iPageFragment	= (int) (super.oRAF.getFilePointer () - (this.liCurrentPage * super.liPageSize));

					if (this.iStartOfCCA > 0)
					{
						this.iPageFragment	= (int) ((super.liPageSize - this.iStartOfCCA) + this.iPageFragment);
						this.iPageFragment	= (int) (super.liPageSize - this.iPageFragment);
					}
					else
					{
						this.iPageFragment	= (int) (super.liPageSize - this.iPageFragment);
					}
					
					super.pyoDebug.writeJava (String.format ("Free Bytes in Leaf page = [%d]", this.iPageFragment));
				}
			}
			
			// Set the pointer to the correct location in the page
			
			this.liCurrentPosToWrite	= super.oRAF.getFilePointer () + (this.iPageFragment - this.iRecordSize);
			
			super.oRAF.seek (this.liCurrentPosToWrite);
		}
		catch (	Exception je)
		{
			je.printStackTrace ();
		}
		
		return;
	}
	
	public void pageDesignator (int piPageType, RandomAccessFile poRAF)
	{
		super.pyoDebug.writeJava (String.format ("Page Designator piPageType = [%d]", piPageType));
		
		try
		{
			if (piPageType == 13)		// Leaf Table
			{
				poRAF.writeByte (piPageType);
				
				// Seek Back to start of page
				poRAF.seek (poRAF.getFilePointer () - 1);
			}
			else if (piPageType == 5)	// Interior Table
			{
				poRAF.writeByte (piPageType);
				
				// Seek Back to start of page
				poRAF.seek (poRAF.getFilePointer () - 1);
			}
			else if (piPageType == 2)	// Interior Index
			{
				poRAF.writeByte (piPageType);
				
				// Seek Back to start of page
				poRAF.seek (poRAF.getFilePointer () - 1);
			}
			else if (piPageType == 10)	// Leaf Index
			{
				poRAF.writeByte (piPageType);
				
				// Seek Back to start of page
				poRAF.seek (poRAF.getFilePointer () - 1);
			}
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
		
		return;
	}
	
	// Following writes the Serial Codes of all the columns
	public void writeSerialCodes (RandomAccessFile poRAF)
	{
		try
		{
			// If the Data Type is Text - Then add the length of the column value with Serial Type code and write
			// this added result as serial code
			if (this.iColumnSerialCode == 12)
			{
				poRAF.writeByte (this.iColumnSerialCode + this.iColumnDataTypeLength);
			}
			else
			{
				poRAF.writeByte (this.iColumnSerialCode);
			}
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
	}
	
	// Following function write the data to file based on its data type and sets the pointer offset
	// correctly after writing to the file
	public void writeColumns (RandomAccessFile poRAF)
	{
		try
		{
			super.pyoDebug.writeJava (String.format ("Write Columns sColumnDataType [%s]", this.sColumnDataType));
			
			if (this.sColumnDataType.equals ("TEXT"))
			{
				// Write Text Here
				poRAF.writeBytes (this.sColumnValue);
			}
			else
			{
				switch (this.iColumnDataTypeLength)
				{
					case 1:
					{
						if (this.sColumnDataType.equals ("TINYINT"))
						{
							poRAF.writeByte (Integer.parseInt (this.sColumnValue));
						}
						else if (this.sColumnDataType.startsWith ("null"))
						{
							this.bByteArray	= new byte [] {0};
							
							poRAF.write (this.bByteArray);
						}
						
						break;
					}
					
					case 2:
					{
						if (this.sColumnDataType.equals ("SMALLINT"))
						{
							poRAF.writeShort (Integer.parseInt (this.sColumnValue));
						}
						else if (this.sColumnDataType.startsWith ("null"))
						{
							this.bByteArray	= new byte [] {0, 0};
							
							poRAF.write (this.bByteArray);
						}
						
						break;
					}
						
					case 4:
					{
						if (this.sColumnDataType.equals ("INT"))
						{
							poRAF.writeInt (Integer.parseInt (this.sColumnValue));
						}
						else if (this.sColumnDataType.equals ("REAL"))
						{
							poRAF.writeFloat (Float.parseFloat (this.sColumnValue));
						}
						else if (this.sColumnDataType.startsWith ("null"))
						{
							this.bByteArray	= new byte [] {0, 0, 0, 0};
							
							poRAF.write (this.bByteArray);
						}
						
						break;
					}
						
					case 8:
					{
						if (this.sColumnDataType.equals ("BIGINT"))
						{
							poRAF.writeLong (Long.parseLong (this.sColumnValue));
						}
						else if (this.sColumnDataType.equals ("DOUBLE"))
						{
							poRAF.writeDouble (Double.parseDouble (this.sColumnValue));
						}
						else if (this.sColumnDataType.startsWith ("DATE"))
						{
							poRAF.writeLong (Long.parseLong (this.sColumnValue));
						}
						else if (this.sColumnDataType.startsWith ("null"))
						{
							this.bByteArray	= new byte [] {0, 0, 0, 0, 0, 0, 0, 0};
							
							poRAF.write (this.bByteArray);
						}
						
						break;
					}
				}
			}
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
		return;
	}
	
	// This function marks appropriate page header once a record is written completely to the
	// page of the file
	public void markPageHeader ()
	{
		int 	iTmp	= 0;
		
		Short	oShort;
		
		try
		{
			super.oRAF.seek (this.liCurrentPage * super.liPageSize);
			
			/* Page header at offset 0x01 */
			super.oRAF.seek (super.oRAF.getFilePointer () + 1);
			
			iTmp	= super.oRAF.read ();
			
			// Increment the number of cells in this page by 1
			iTmp	= iTmp + 1;
			
			// Write back to the page
			super.oRAF.seek (super.oRAF.getFilePointer () - 1);
			super.oRAF.write (iTmp);
			
			/* Page header at offset 0x02 */
			super.oRAF.writeShort ((int) (this.liCurrentPosToWrite - (this.liCurrentPage * super.liPageSize)));
			
			/* Page header at offset 0x04 */
			// To Do - Right and left sibling at offset 0x04 - Nishanth L
			Random	oRand	= new Random ();
			
			if (super.oRAF.length () > super.liPageSize)
			{
				super.oRAF.writeInt (oRand.nextInt ());
			}
			//super.oRAF.seek (super.oRAF.getFilePointer () + 4);
			
			/* Page header at offset 0x08 */
			super.oRAF.seek (super.oRAF.getFilePointer () + (this.iNumCellsPP * 2));
			super.oRAF.writeShort ((int) (this.liCurrentPosToWrite - (this.liCurrentPage * super.liPageSize)));
			
			// Reset the pointer to the start of the current page
			super.oRAF.seek (this.liCurrentPage * super.liPageSize);
		}
		catch (Exception je)
		{
			je.printStackTrace (); 
		}
		
		return;
	}
}
