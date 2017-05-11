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

public class javaBPTTblSQLProcessing extends javaBPTMain
{
	private	BPT     oBPT;		// Corresponds to Catalog and Normal Tables 
	private	BPT	oBPTNormal;	// Corresponds to Normal Tables exclusively
	
	// Catalog Table Processing + Normal Table Processing
	public javaBPTTblSQLProcessing (String psTmpFile, String psDBName, String psTblName)
	{
		this.oBPT	= new BPT (psDBName, psTblName);	
		
		super.sTmpFile	= psTmpFile;
			
		return;
	}
	
	// Normal Table Processing - Not Used Currently
	public javaBPTTblSQLProcessing (String psFile, String psTmpFile, String psDBName, String psTblName, PyObject poDebug, PyObject poBPTPD)
	{
		super (psFile, psTmpFile, psDBName, psTblName, poDebug, poBPTPD);
		
		this.oBPTNormal		= new BPT (psDBName, psTblName);
		
		return;
	}
	
	public int getCatalogBTreeRecCount ()
	{
		return this.oBPT.getNumRecords ();
	}
	
	public int getNormalBTreeRecCount ()
	{
		return this.oBPTNormal.getNumRecords ();
	}
	
	public void printTree ()
	{
		this.oBPT.BPTPrint ();
	}
	
	public ArrayList<String> getTableStructureFromCatalog (String psTblName)
	{
		int	iKey	= 0;
		int	iCtr1	= 0;
		int	iCtr2	= 0;
		
		int	iPayloadSize	= 0;
		int	iNumCols	= 0;
		
		int	iTmp1	= 0;
		int	iTmp2	= 0;
		int	iTmp3	= 0;
		
		ArrayList<String>	alList1	= new ArrayList <String> ();
		ArrayList<byte []>	alList2	= new ArrayList <byte []> ();
		ArrayList<Integer>	alList3	= new ArrayList <Integer> ();
		
		Tuple			oTuple	= new Tuple ();
		
		byte[]			baByteArray;
		
		try
		{
			super.openTmpFile ();
			
			while (true)
			{
				oTuple	= this.oBPT.BPTBrowse (0);
				
				if (oTuple != null && oTuple.getKey () != null)
				{
					iKey	= (Integer) oTuple.getKey ();
					alList2	= (ArrayList <byte[]>)oTuple.getValue ();
				}
				else
				{
					break;
				}
				
				for (iCtr1 = 0; iCtr1 < alList2.size(); iCtr1++)
				{
					// Reset the length of the file
					super.oRAF2.setLength (0);
					
					// Write to a Temporary File
					super.oRAF2.write (alList2.get (iCtr1));
					
					// Skip the First 2 Bytes
					super.oRAF2.seek (2);
					
					iNumCols	= super.oRAF2.readByte ();      // Number of Columns
					
                                        iTmp1	= super.oRAF2.readByte ();      // Serial Type code for ROWID - Constant 4 Byte
                                        iTmp2	= super.oRAF2.readByte ();      // Serial Type code for SCHEMA_NAME
                                        iTmp3	= super.oRAF2.readByte ();      // Serial Type code for TABLE_NAME
					
                                        // Reset the pointer
                                        super.oRAF2.seek (2);
					
                                        iTmp1   = 4 + (iTmp2 - 12);
					
                                        // Seek to the correct position to read table name
                                        super.oRAF2.seek (super.oRAF2.getFilePointer() + 1 + iNumCols+ iTmp1);
					
                                        iTmp3   -= 12;
					
                                        baByteArray = new byte [iTmp3];
					
                                        super.oRAF2.read (baByteArray);
					
                                        sTblName        = ByteToString (baByteArray);
					
					if (sTblName.contains (psTblName))
					{
						// Reset the pointer
						super.oRAF2.seek (0);
						
						// Total Byte of Payload
						iPayloadSize	= super.oRAF2.readShort ();
						
						// Number of Columns
						iNumCols		= super.oRAF2.readByte ();
						
						// Add all the Serial Type Codes here
						for (iCtr2 = 0; iCtr2 < iNumCols; iCtr2++)
						{
							alList3.add (super.oRAF2.readUnsignedByte ());	
						}
						
						// Read the Values and store in a ArrayList of String (Format -> Value|DATA_TYPE)
						// iNumCols = alList3.size () -> This assertion must hold good always.
						for (iCtr2 = 0; iCtr2 < iNumCols; iCtr2++)
						{
							iTmp1	= alList3.get (iCtr2);
							
							// If Text Data Type then subtract the offset to get the String Length
							if (iTmp1 >= 12)
							{
								iTmp1	-= 12;
								
								baByteArray	= new byte [iTmp1];
								
								super.oRAF2.read (baByteArray);
								
								alList1.add (ByteToString (baByteArray) + "|TEXT");
							}
							else
							{
								switch (iTmp1)
								{
									case 4:
									{
										alList1.add (Integer.toString (super.oRAF2.read ()) + "|TINYINT");
										
										break;
									}
									
									case 5:
									{
										alList1.add (Short.toString (super.oRAF2.readShort ()) + "|SMALLINT");
										
										break;
									}
									
									case 6:
									{
										alList1.add (Integer.toString (super.oRAF2.readInt ()) + "|INT");
										
										break;
									}	
									
									case 7:
									{
										alList1.add (Long.toString (super.oRAF2.readLong ()) + "|BIGINT");
										
										break;
									}	
									
									case 8:
									{
										alList1.add (Float.toString (super.oRAF2.readFloat ()) + "|REAL");
										
										break;
									}	
									
									case 9:
									{
										alList1.add (Double.toString (super.oRAF2.readDouble ()) + "|DOUBLE");
										
										break;
									}	
									
									case 10:
									{
										alList1.add (Long.toString (super.oRAF2.readLong ()) + "|DATETIME");
										
										break;
									}	
									
									case 11:
									{
										alList1.add (Long.toString (super.oRAF2.readLong ()) + "|DATE");
										
										break;
									}
								}
							}
						}
						
						// Clear the list before next iteration
						alList3.clear();
					}
				}
			}
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
		
		return alList1;	
	}
	
	public ArrayList<String> getTableValuesFromNormal ()
	{
		int	iKey	= 0;
		int	iCtr1	= 0;
		int	iCtr2	= 0;
		
		int	iPayloadSize	= 0;
		int	iNumCols	= 0;
		
		int	iTmp1	= 0;
		int	iTmp2	= 0;
		int	iTmp3	= 0;
		
		String	sDelimString	= "";
		
		ArrayList<String>	alList1	= new ArrayList <String> ();
		ArrayList<byte []>	alList2	= new ArrayList <byte []> ();
		ArrayList<Integer>	alList3	= new ArrayList <Integer> ();
		ArrayList<String>	alList4	= new ArrayList <String> ();
		
		Tuple			oTuple	= new Tuple ();
		
		byte[]			baByteArray;
		
		try
		{
			super.openTmpFile ();
			
			while (true)
			{
				oTuple	= this.oBPT.BPTBrowse (0);
				
				if (oTuple != null && oTuple.getKey () != null)
				{
					iKey	= (Integer) oTuple.getKey ();
					alList2	= (ArrayList <byte[]>)oTuple.getValue ();
				}
				else
				{
					break;
				}
				
				for (iCtr1 = 0; iCtr1 < alList2.size(); iCtr1++)
				{
					// Reset the length of the file
					super.oRAF2.setLength (0);
					
					// Write to a Temporary File
					super.oRAF2.write (alList2.get (iCtr1));
					
					// Reset the pointer
					super.oRAF2.seek (0);
					
					// Total Byte of Payload
					iPayloadSize	= super.oRAF2.readShort ();
					
					// Number of Columns
					iNumCols		= super.oRAF2.readByte ();
					
					// Add all the Serial Type Codes here
					for (iCtr2 = 0; iCtr2 < iNumCols; iCtr2++)
					{
						alList3.add (super.oRAF2.readUnsignedByte ());	
					}
					
					// Read the Values and store in a ArrayList of String (Format -> Value|DATA_TYPE)
					// iNumCols = alList3.size () -> This assertion must hold good always.
					for (iCtr2 = 0; iCtr2 < iNumCols; iCtr2++)
					{
						iTmp1	= alList3.get (iCtr2);
						
						// If Text Data Type then subtract the offset to get the String Length
						if (iTmp1 >= 12)
						{
							iTmp1	-= 12;
							
							baByteArray	= new byte [iTmp1];
							
							super.oRAF2.read (baByteArray);
							
							alList1.add (ByteToString (baByteArray));
						}
						else
						{
							switch (iTmp1)
							{
								case 0:
								{
									alList1.add ("null");
									
									// Read to seek the pointer to correct position
									super.oRAF2.read ();
									
									break;
								}
								
								case 1:
								{
									alList1.add ("null");
									
									// Read to seek the pointer to correct position
									super.oRAF2.readShort ();
									
									break;
								}
								
								case 2:
								{
									alList1.add ("null");
									
									// Read to seek the pointer to correct position
									super.oRAF2.readInt ();
									
									break;
								}
								
								case 3:
								{
									alList1.add ("null");
									
									// Read to seek the pointer to correct position
									super.oRAF2.readLong ();
									
									break;
								}
								
								case 4:
								{
									alList1.add (Integer.toString (super.oRAF2.read ()));
									
									break;
								}
								
								case 5:
								{
									alList1.add (Short.toString (super.oRAF2.readShort ()));
									
									break;
								}
								
								case 6:
								{
									alList1.add (Integer.toString (super.oRAF2.readInt ()));
									
									break;
								}	
								
								case 7:
								{
									alList1.add (Long.toString (super.oRAF2.readLong ()));
									
									break;
								}	
								
								case 8:
								{
									alList1.add (Float.toString (super.oRAF2.readFloat ()));
									
									break;
								}	
								
								case 9:
								{
									alList1.add (Double.toString (super.oRAF2.readDouble ()));
									
									break;
								}	
								
								case 10:
								{
									alList1.add (Long.toString (super.oRAF2.readLong ()));
									
									break;
								}	
								
								case 11:
								{
									alList1.add (Long.toString (super.oRAF2.readLong ()));
									
									break;
								}
							}
						}
					}
						
					// Clear the list before next iteration
					alList3.clear();
					
					// Make a delimited test
					sDelimString	= String.join ("~", alList1);
					
					// Clear the list before next iteration
					alList1.clear();
					
					alList4.add (sDelimString);
				}
			}
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
		
		return alList4;	
	}
	
	public void deleteTableValuesFromNormal (PyList pyoKeyList)
	{
		Iterator listIterator       = pyoKeyList.iterator ();
		
		try
		{
			while(listIterator.hasNext ())
			{
				this.oBPT.TblDrop (Integer.parseInt ((String) listIterator.next ()));
			}
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
		
		return;
	}
	
	public void deleteTableValuesFromIndex (String psKey, String psDataType)
	{
		try
		{
			this.oBPT.IdxDrop (psKey, psDataType);
		}
		catch (Exception je)
		{
			// Do Nothing even though key is not found as Index File
			// Structure is maintained and is valid.
			;
		}
		
		return;
	}
	
	public String ByteToString (byte[] pbaByteArray)
	{
		String s = new String(pbaByteArray);
		
		return s;
	}
}
