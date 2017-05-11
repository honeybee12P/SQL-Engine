package bPlusTree;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;

// Jython - Java Interfacing libraries with Python
import org.python.core.PyException;
import org.python.core.PyInteger;
import org.python.core.PyObject;

// Jython - Custom Java Interfacing libraries with Python
import bPlusTree.interfaces.debugInterface;
import bPlusTree.interfaces.bptpdInterface;

public class javaBPTMain
{
	protected	RandomAccessFile	oRAF;		// Pointer to Page on File
	protected	RandomAccessFile	oRAF2;		// Pointer to Temporary File
	
	protected	debugInterface		pyoDebug;
	protected	bptpdInterface		pyoBPTPD;
	
	protected	long	liPageSize;
	protected	long	liFileSize;
	
	protected	int	iPageLocation;
	protected	int	iCurrentPage;
	protected	int	iRecordLocation;
	protected	int	iNumPages;
	
	protected	String	sFile;
	protected	String	sTmpFile;
	protected	String	sDBName;
	protected	String	sTblName;
	
	protected javaBPTMain ()
	{
		;
	}
	
	public javaBPTMain (String psFile, String psTmpFile, String psDBName, String psTblName, PyObject poDebug, PyObject poBPTPD)
	{
		// Constructor
		
		// Python objects to use in JAVA
		this.pyoDebug	= (debugInterface) poDebug.__tojava__ (debugInterface.class);
		this.pyoBPTPD	= (bptpdInterface) poBPTPD.__tojava__ (bptpdInterface.class);
		
		this.liPageSize		= this.pyoBPTPD.getPageSize ();
		
		this.iPageLocation	= 0;
		this.iCurrentPage	= 0;
		this.iRecordLocation	= 0;
		
		this.liFileSize		= 0;
		this.iNumPages		= 0;
		
		this.sFile		= psFile;
		this.sTmpFile		= psTmpFile;
		
		this.sDBName		= psDBName;
		this.sTblName		= psTblName;
		
		this.pyoDebug.writeJava (String.format ("Table File Name [%s]", this.sFile));
		
		return;
	}
	
	public void openTmpFile ()
	{
		try
		{
			this.oRAF2 	= new RandomAccessFile (this.sTmpFile, "rw");
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
		
		return;
	}
	
	public void openFile ()
	{
		try
		{
			this.oRAF 	= new RandomAccessFile (this.sFile, "rw");
			
			// Clear the File
			this.oRAF.setLength (0);
			this.liFileSize	= this.oRAF.length();
			
			this.iNumPages	= (int) (this.liFileSize / this.liPageSize); 
			
			this.oRAF.setLength (this.liFileSize + this.liPageSize);
			
			this.iPageLocation	= 0;
			this.iCurrentPage	= 0;
			this.iRecordLocation	= 0;
			
			// By Default designate as Table Leaf
			this.oRAF.writeByte (this.pyoBPTPD.getLeafTbl ());
			
			// Locate the pointer to the beginning of the page
			this.oRAF.seek (0);
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
		
		//displayBinaryHex (this.oRAF);
		
		return;
	}
	
	// Increases the size of the Binary File by one more page
	public void extendPage ()
	{
		try
		{
			// Increase the file size by one more page
			this.oRAF.setLength (this.oRAF.length () + this.liPageSize);
			
			// Sets the pointer to point to the start of the newly added page
			this.oRAF.seek (this.oRAF.length () - this.liPageSize);
		}
		catch (Exception je)
		{
			je.printStackTrace ();
		}
		
		return;	
	}
}
