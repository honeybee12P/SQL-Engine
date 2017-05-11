#!/usr/bin/env jython

import	logging		as LOG
import	sys		as SYS
import	os		as OS
import	cmd		as CMD
import	sys		as SYS
import	pyparsing	as PARSER
import	shutil		as SHUTIL
import	tabulate	as TABULATE
import	itertools	as ITER
import	pickle		as PICKLE
import	collections	as COLLECTIONS
import	operator	as OPERATOR
import	datetime	as DT
import	time		as TIME

import	xmltodict	as XML

# Import Java Interfaces here
from	bPlusTree.interfaces	import	debugInterface
from	bPlusTree.interfaces	import	bptpdInterface

giDbgLvl	= LOG.DEBUG

dDataTypeSerialCode	= {
	
	# Value pair is [Byte, Hex Serial Code]
	
	'null_TINYINT'	: '0x00',
	'null_SMALLINT'	: '0x01',
	'null_INT'	: '0x02',
	'null_REAL'	: '0x02',
	'null_DOUBLE'	: '0x03',
	'null_DATETIME'	: '0x03',
	'null_DATE'	: '0x03',
	'TINYINT'	: '0x04',
	'SMALLINT'	: '0x05',
	'INT'		: '0x06',
	'BIGINT'	: '0x07',
	'REAL'		: '0x08',
	'DOUBLE'	: '0x09',
	'DATETIME'	: '0x0A',
	'DATE'		: '0x0B',
	'TEXT'		: '0x0C' 

}

dDataTypeLength	= {
	
	# Value pair is [Byte, Hex Serial Code]
	
	'null_TINYINT'	: '1',
	'null_SMALLINT'	: '2',
	'null_INT'	: '4',
	'null_REAL'	: '4',
	'null_DOUBLE'	: '8',
	'null_DATETIME'	: '8',
	'null_DATE'	: '8',
	'TINYINT'	: '1',
	'SMALLINT'	: '2',
	'INT'		: '4',
	'BIGINT'	: '8',
	'REAL'		: '4',
	'DOUBLE'	: '8',
	'DATETIME'	: '8',
	'DATE'		: '8',
	'TEXT'		: '0' 

}

dConfig = {

	'LAST_ROW_ID_DB_TABLES'		: 0,
	'LAST_ROW_ID_DB_COLUMNS'	: 0,
	'LAST_ROW_ID_NORMAL_TABLES'	: 0
}

dOpFunc	= {
	
	'+'	: OPERATOR.add,
	'-'	: OPERATOR.sub,
	'*'	: OPERATOR.mul,
	'/'	: OPERATOR.div,
	'>'	: OPERATOR.gt,
	'<'	: OPERATOR.lt,
	'>='	: OPERATOR.ge,
	'<='	: OPERATOR.le,
	'gt'	: OPERATOR.gt,
	'lt'	: OPERATOR.lt,
	'ge'	: OPERATOR.ge,
	'le'	: OPERATOR.le,
	'&'	: OPERATOR.and_,
	'AND'	: OPERATOR.and_,
	'|'	: OPERATOR.or_,
	'OR'	: OPERATOR.or_,
	'='	: OPERATOR.eq,
	'eq'	: OPERATOR.eq,
	'ne'	: OPERATOR.ne,
	'!='	: OPERATOR.ne
}

# B-Plus Tree Page Defines
class BPT_PD(bptpdInterface):
	
	def __init__ (self):
		
		self.iInteriorIdx	= 2
		self.iInteriorTbl	= 5
		self.iLeafIdx		= 10
		self.iLeafTbl		= 13
		self.iPageSize		= 512	# Default Page size of 512 Byte. Overwritten from XML
		self.iBranchingFactor	= 6;	# Default Branching Factor of 6. Overwritten from XML
		
		return
	
	def getInteriorIdx (self):
		
		return self.iInteriorIdx
	
	def getInteriorTbl (self):
		
		return self.iInteriorTbl
	
	def getLeafIdx (self):
		
		return self.iLeafIdx
	
	def getLeafTbl (self):
		
		return self.iLeafTbl
	
	def getPageSize (self):
		
		return self.iPageSize
	
	def getDataTypeSerialCode (self, psKey):
		
		return dDataTypeSerialCode [psKey]
	
	def getDataTypeLength (self, psKey):
		
		return dDataTypeLength [psKey]
	
	def getBranchingFactor (self):
		
		return self.iBranchingFactor

class DEBUG(debugInterface):

	def __init__ (self, psPath='/tmp', piPriority=giDbgLvl):
		
		self.sFN	= '/DavisBASE_logdebug-%d.log' %(OS.getpid ())
		
		self.oLogger	= None
		self.oFH	= None
		self.oFrmtr	= None
		self.iPriority	= piPriority;
		
		# Construct `logdebug` File Name Here
		
		if psPath == None:
			
			psPath	= '/tmp'
		
		if psPath == '/tmp':
			
			self.sFN	= psPath + self.sFN
		
		else:
			
			self.sFN	= psPath + '/Users/kruthikavishwanath/Documents/Spring_2017/Database_Design/DavisBASE/LOGS' + self.sFN
		
		self.oLogger	= LOG.getLogger ('Logger')
		
		return
	
	def debugState (self, piFlag=True):
		
		global	giDbgLvl
		
		if piFlag == False:
			
			# Set to a higher value to DISABLE Debugging
			giDbgLvl	= 99999
		
		else:
			
			self.oLogger.setLevel (self.iPriority)
			
			self.oFH	= LOG.FileHandler (self.sFN)
			
			self.oFrmtr	= LOG.Formatter ("%(asctime)s - %(levelname)10s: %(message)s")
			
			self.oFH.setFormatter (self.oFrmtr)
			
			self.oLogger.addHandler (self.oFH)
	
	def write (self, **kwargs):
		
		global	giDbgLvl
		
		self.sMsg	= ''
		
		'''
		Check if the Debug priority is being passed while calling. If so
		over write the same
		'''
		
		if self.iPriority >= giDbgLvl:
		
			for sKey in kwargs:
				
				if sKey == 'piPriority':
					
					self.iPriority	= kwargs [sKey]
				
				elif sKey == 'psMsg':
					
					self.sMsg	= kwargs [sKey]
			
			self.oLogger.log (self.iPriority, self.sMsg)
		
		return
	
	def writeJava (self, psMsg=''):
		
		global	giDbgLvl
		
		'''
		Standard Python Logger for Java Classes
		'''
		
		if self.iPriority >= giDbgLvl and psMsg:
			
			psMsg	= '{Java}: ' + psMsg
			
			self.oLogger.log (self.iPriority, psMsg)
		
		return

'''
printException - Prints Exception to Console without exiting the code
'''
def printException (psCode, psMsg):
	
	if psCode:
		
		if psMsg [-1] != '\n':
		
			psMsg	+= '\n'
			
		print (psCode + ':' + psMsg)
	
	return

'''
Grouper Procedure - Converts List to List of Lists of n elements
'''
def grouper (plFiles=[], piRow=1, piColumn=1):
	
	plTmp	= []
	
	plTmp	= [plFiles [piColumn*i : piColumn*(i+1)] for i in range (piRow)]
	
	return plTmp

'''
Cast Procedure - Converts the value to its corresponding data type
'''
def castValue (psValue, psDataType):
	
	try:
		
		if (psDataType) == 'TINYINT':
			
			return int (psValue)
		
		elif (psDataType) == 'SMALLINT':
			
			return int (psValue)
			
		elif (psDataType) == 'INT':
			
			return int (psValue)
		
		elif (psDataType) == 'BIGINT':
			
			return long (psValue)
		
		elif (psDataType) == 'REAL':
			
			return float (psValue)
		
		elif (psDataType) == 'DOUBLE':
			
			return float (psValue)
		
		elif (psDataType) == 'DATETIME':
		
			return long (psValue)
		
		elif (psDataType) == 'DATE':
			
			return long (psValue)
		
		elif (psDataType) == 'TEXT':
			
			return psValue
		
		else:
		
			# Treat by default as a string
			return psValue
	
	except Exception as e:
		
		return psValue

def convertDT (piFlag, psDT):
	
	# piFlag 0/1	-> To Long Integer / From Long Integer
	
	if psDT.lower () == 'null':
		
		return str (psDT)
	
	try:
		
		if piFlag == 0:
			
			# Pad to 14 Characters if Time part is not given
			psDT = psDT.ljust (14, '1')
			
			oDT	= DT.datetime.strptime (psDT, '%Y%m%d%H%M%S')
			
			lSecs	= TIME.mktime (oDT.timetuple ())
			
			return str (lSecs)[:-2]		# Remove decimal point
		
		else:
			
			oDT	= DT.datetime.fromtimestamp (float (psDT)).strftime ('%Y-%m-%d_%H:%M:%S')
			
			return str (oDT)
	
	except Exception as e:
		
		return str (psDT)

'''
Pickle The config dictionary
'''
def pickle (psPath, piOps, psKey, pValue):
	
	# piOps	0/1	-> Write / Load
	
	global	dConfig
	
	if piOps == 0:
		
		fpWriteHdl	= open (psPath, 'wb')
		
		dConfig [psKey]	= pValue
		
		PICKLE.dump (dConfig, fpWriteHdl)
		
		fpWriteHdl.flush ()
		
		fpWriteHdl.close ()
	
	else:
		
		if OS.path.exists (psPath):
			
			fpReadHdl	= open (psPath, 'rb+')
			
			dConfig	= PICKLE.load (fpReadHdl);
			
			fpReadHdl.close ()
		
		else:
			
			dConfig [psKey]	= 0
		
		return dConfig [psKey]
	
	return

def init ():
	
	global	goDebug
	global	goBPTPD
	
	global	gsInstallPath
	global	gsCurrentSchema
	
	goDebug	= DEBUG (psPath=OS.environ.get ('PROJECTS_HOME'), piPriority=LOG.DEBUG)
	
	goBPTPD	= BPT_PD ()
	
	return

if __name__ == '__main__':
	
	dObj	= DEBUG (psPath='/tmp', piPriority=LOG.DEBUG)
	
	dObj.write (psMsg='[%s] - Inside [%s]. Test Message. Stand-alone Testing of Script.' %(HDR.OS.path.basename(__file__), ROUTINE))
	
	SYS.exit (0)
