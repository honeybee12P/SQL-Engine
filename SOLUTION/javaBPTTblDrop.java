package bPlusTree;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Arrays;
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

public class javaBPTTblDrop extends javaBPTMain
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
	
	public javaBPTTblDrop (String psFile, String psTmpFile, String psDBName, String psTblName, PyObject poDebug, PyObject poBPTPD)
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
		
		this.oBPT	= new BPT (super.sDBName, super.sTblName);
		
		return;
	}
	
	public int getBTreeRecCount ()
	{
		return this.oBPT.getNumRecords ();
	}
	
	public void drop (String psTreeKey)
	{
		Tuple	oTuple;
		
		int	iKey;
		int	iTmp;
		int	iTmp1;
		int	iTmp2;
		int	iTmp3;
		
		String	sTblName;
		
		ArrayList<Integer>	alKey	= new ArrayList<Integer> ();
		
		try
		{
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
			
			super.openTmpFile ();
				
			while (true)
			{
				oTuple	= this.oBPT.BPTBrowse (0);
				
				if (oTuple != null && oTuple.getKey () != null)
				{
					iKey		= (Integer) oTuple.getKey ();
					this.alList2 	= (ArrayList <byte[]>)oTuple.getValue ();
				}
				else
				{
					break;
				}
				
				for (this.iCtr = 0; this.iCtr < this.alList2.size(); this.iCtr++)
				{
					// Clear the File
					super.oRAF2.setLength (0);
					
					// Write to a Temporary File
					super.oRAF2.write (alList2.get (this.iCtr));
					
					// Skip the First 2 Bytes
					super.oRAF2.seek (2);			// 2 Byte Payload
					
					iTmp	= super.oRAF2.readByte ();	// Number of Columns
					
					iTmp1	= super.oRAF2.readByte ();	// Serial Type code for ROWID - Constant 4 Byte
					iTmp2	= super.oRAF2.readByte ();	// Serial Type code for SCHEMA_NAME
					iTmp3	= super.oRAF2.readByte ();	// Serial Type code for TABLE_NAME
					
					// Reset the pointer
					super.oRAF2.seek (2);
					
					iTmp1	= 4 + (iTmp2 - 12);
					
					// Seek to the correct position to read table name
					super.oRAF2.seek (super.oRAF2.getFilePointer() + 1 + iTmp + iTmp1);
					
					iTmp3	-= 12;
					
					this.bByteArray	= new byte [iTmp3];
					
					super.oRAF2.read (this.bByteArray);
					
					sTblName	= ByteToString (this.bByteArray);
					
					if (sTblName.contains (psTreeKey))
					{
						alKey.add (iKey);
					}
				}
			}
			
			for (this.iCtr = 0; this.iCtr < alKey.size(); this.iCtr++)
			{
				this.oBPT.TblDrop (alKey.get (this.iCtr));
			}
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
		
		return;
	}
	
	public String ByteToString (byte[] pbByteArray)
	{
		String s = new String(pbByteArray);
		
		return s;
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
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
	}
	
	public void writeRoot ()
	{
		Tuple	oTuple;
		
		int	iKey		= 0;
		int	iRecCount	= 0;
		int	iTmp		= 0;
		
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
			
			while (true)
			{
				oTuple	= this.oBPT.BPTBrowse (0);
				
				if (oTuple != null && oTuple.getKey () != null)
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
					
					if (this.iCtr != 0)
					{
						iRecCount	+= this.iCtr;
					}
					else
					{
						iRecCount	+= 1;
					}
					
					if (iRecCount == (int) (getBTreeRecCount ()/2) + iTmp)
					{
						this.pageDeterminator ("INTERIOR");
						
						// Write to the File here
						super.oRAF.writeInt (this.iLCPageNum);		// 4 Byte Page Number - Left Child Pointer
						super.oRAF.writeInt (iKey);			// Integer Key
						
						markPageHeader ();
						
						iRecCount	= 0;
						
						this.iLCPageNum	+= 1;
						
						iTmp	+= 1;
						
						if (iTmp == (iNumIntNodes - 1))
						{
							break;
						}
					}
					
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
			
			while (true)
			{
				oTuple	= this.oBPT.BPTBrowse (0);
				
				if (oTuple != null && oTuple.getKey () != null)
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
				
				if (oTuple != null && oTuple.getKey () != null)
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
		try
		{
			if (piPageType == 13)
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
			if (this.sColumnDataType == "TEXT")
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
						if (this.sColumnDataType == "TINYINT")
						{
							poRAF.writeByte (Integer.parseInt (this.sColumnValue));
						}
						else if (this.sColumnDataType.startsWith ("NULL"))
						{
							this.bByteArray	= new byte [] {0};
							
							poRAF.write (this.bByteArray);
						}
						
						break;
					}
					
					case 2:
					{
						if (this.sColumnDataType == "SMALLINT")
						{
							poRAF.writeShort (Integer.parseInt (this.sColumnValue));
						}
						else if (this.sColumnDataType.startsWith ("NULL"))
						{
							this.bByteArray	= new byte [] {0, 0};
							
							poRAF.write (this.bByteArray);
						}
						
						break;
					}
						
					case 4:
					{
						if (this.sColumnDataType == "INT")
						{
							poRAF.writeInt (Integer.parseInt (this.sColumnValue));
						}
						else if (this.sColumnDataType == "REAL")
							{
							poRAF.writeFloat (Float.parseFloat (this.sColumnValue));
						}
						else if (this.sColumnDataType.startsWith ("NULL"))
						{
							this.bByteArray	= new byte [] {0, 0, 0, 0};
							
							poRAF.write (this.bByteArray);
						}
						
						break;
					}
						
					case 8:
					{
						if (this.sColumnDataType == "BIGINT")
						{
							poRAF.writeLong (Long.parseLong (this.sColumnValue));
						}
						else if (this.sColumnDataType == "DOUBLE")
						{
							poRAF.writeDouble (Double.parseDouble (this.sColumnValue));
						}
						else if (this.sColumnDataType.startsWith ("DATE"))
						{
							poRAF.writeLong (Long.parseLong (this.sColumnValue));
						}
						else if (this.sColumnDataType.startsWith ("NULL"))
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
			super.oRAF.seek (super.oRAF.getFilePointer () + 4);
			
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
	
	public void splitPage (int piStartOffset)
	{
		return;
	}
	
	public void pageRearrange ()
	{
		// To do - Nishanth L
		// All modifications of rearranging the pages and modifying the page header goes here
		return;
	}
}
