#!/usr/bin/env jython

# Custom Libraries goes here
import	customHDR	as HDR

# Jython - Import Java Libraries here
import  bPlusTree.javaBPTTblSQLProcessing

class initQuery:
	
	def __init__ (self):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		'''
		Considering the following Syntax for the grammer construction.
		
		DELETE FROM TABLE_NAME WHERE CONDITION
		
		(TABLE_NAME[S])		-> Formatted as ABC, XYZ (or) ABC (or) SCHEMA1.ABC, SCHEMA2.XYZ		
		(CONDITION)		-> Can be one from <, <=, >, >=, =, !=, eq, ne, lt, le, gt, ge, AND, OR
		
		'''
		
		self.joBPTTBLProcessCtg		= None
		self.joBPTTBLProcessNormal	= None
		self.joBPTTblWrite		= None
		
		self.sStmt	= HDR.PARSER.Forward ()
		self.exprWhere	= HDR.PARSER.Forward ()
		
		self.sKeyword01	= HDR.PARSER.Keyword ('DELETE', caseless=True).setResultsName ('OPERATION')
		self.sKeyword02	= HDR.PARSER.Keyword ('FROM', caseless=True)
		self.sKeyword03	= HDR.PARSER.Keyword ('WHERE', caseless=True)
		self.sTerm	= HDR.PARSER.CaselessLiteral (";")
		
		self.sKeywordAND	= HDR.PARSER.Keyword ('AND', caseless=True)
		self.sKeywordOR		= HDR.PARSER.Keyword ('OR', caseless=True)
		
		self.tokColumns	= HDR.PARSER.delimitedList (HDR.PARSER.Word (HDR.PARSER.alphanums).setName ('COLUMN_NAMES'), delim='.', combine=True)
		
		self.tokTables	= HDR.PARSER.delimitedList (HDR.PARSER.Word (HDR.PARSER.alphanums).setName ('TABLE_NAMES'), delim='.', combine=True)
		
		self.tokTablesList	= HDR.PARSER.Group (HDR.PARSER.delimitedList (self.tokTables, delim=',', combine=True))
		
		self.tokTablesList	= self.tokTablesList.setResultsName ('TABLE')
		
		self.tokArithOP	= HDR.PARSER.oneOf ("= != < > >= <= eq ne lt le gt ge", caseless=True)
		
		self.sArithSign	= HDR.PARSER.Word ("+-", exact=1).setName ('SIGN_QUALIFIER (+/-)')
		
		# Following definition is Not currently used
		#self.sINT	= HDR.PARSER.Combine (HDR.PARSER.Optional (self.sArithSign) + HDR.PARSER.Word (HDR.PARSER.nums))
		
		self.tokValues	= HDR.PARSER.Word (HDR.PARSER.printables, excludeChars=';') | HDR.PARSER.Regex (r"()")
		
		self.tokWhere	= HDR.PARSER.Group ((self.tokColumns + self.tokArithOP + self.tokValues)).setName ('WHERE_CONDITION')
		
		self.exprWhere	<<= self.tokWhere + HDR.PARSER.ZeroOrMore ((self.sKeywordAND | self.sKeywordOR) + self.exprWhere)
		
		# Finally Define the Grammar for the Statement/Query Here
		
		self.sStmt	<<= (self.sKeyword01 + self.sKeyword02 + 	\
				     self.tokTablesList + HDR.PARSER.Optional (HDR.PARSER.Group (self.sKeyword03 + self.exprWhere)).setResultsName ('WHERE_COND') + \
				     self.sTerm)
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def executeQuery (self, psQuery=''):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		tResult		= ()
		dQuery		= {}
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		tResult	= self.tokenizeQuery (psQuery)
		
		if tResult:
			
			try:
				
				dQuery	= self.interpretQuery (tResult)
			
			except Exception as e:
				
				raise Exception (e)
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def tokenizeQuery (self, psQuery=''):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		tResult		= ()
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		tResult	= self.sStmt.runTests (psQuery, parseAll=True, fullDump=True, printResults=False)
		
		if tResult[0]:
		
			tResult	= tResult [1:]
			
			HDR.goDebug.write (psMsg='[%s] - [%s] Tokenized Statement [%s]' %(HDR.OS.path.basename(__file__), ROUTINE, str (tResult)))
		
		else:
			
			print (str (tResult [1:]))
			
			tResult	= ()
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return tResult
	
	def interpretQuery (self, ptTokens=()):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), str (ptTokens)))
		
		dQuery		= {}
		dTmp		= {}
		
		iTmp		= 0
		
		tTmp		= ()
		
		# To delete the following commented lines
		
		try:
			
			if not ptTokens:
				
				raise Exception ('Query Tokens are Empty. Critical!!!. Cannot Interpret')
			
			else:
			
				dTmp	= ptTokens [0][0][1]
				
				for sKey, Value in dTmp.items ():
					
					if sKey == 'OPERATION':
						
						dQuery [sKey]	= str (Value)
					
					elif sKey == 'TABLE':
						
						dQuery [sKey]	= ','.join (Value)
					
					elif sKey == 'WHERE_COND':
						
						while iTmp < len (Value [0][1:]):
							
							tTmp	= tTmp + (Value [0][1:][iTmp],)
							
							iTmp	+= 1
						
						dQuery [sKey]	= tTmp
						
						HDR.goDebug.write (psMsg='[%s] - [%s] Where Condition [%s]' %(HDR.OS.path.basename(__file__),
								   ROUTINE, str (dQuery [sKey])))
					
					else:
						
						raise Exception ('Unexpected Keyword in the Syntax. Critical!!!')
				
				self.processQuery (dQuery)
		
		except Exception as e:
			
			raise Exception (e)
		
		return dQuery
	
	def processQuery (self, pdQuery={}):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), str (pdQuery)))
		
		sTmp    = ''
		
		sVar1	= ''
		sOp	= ''
		sVar2	= ''
		
		sVal1	= ''
		sVal2	= ''
		
		oOpFunc	= None
		
		lTmp		= []
		lTmp2		= []
		lHeaders	= []
		
		iCtr1	= 0
		iCtr2	= 0
		iNumCol	= 0
		iNumRow	= 0
		
		# Create a Ordered Dictionary (COLUMN_NAME (Key) -> DATA_TYPE (Value))
                odTableStructure        = HDR.COLLECTIONS.OrderedDict ()
		
                # Create a Ordered Dictionary (COLUMN_NAME (Key) -> Value)
                odTableValue            = HDR.COLLECTIONS.OrderedDict ()
		
		self.joBPTTBLProcessCtg = bPlusTree.javaBPTTblSQLProcessing (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['TABLE']),
                                                                             HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_columns.tbl'),
                                                                             HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_columns.tbl'))
		
		lTmp    = self.joBPTTBLProcessCtg.getTableStructureFromCatalog (pdQuery ['TABLE']);
		
		for sItem in lTmp:
			
			iCtr1   += 1
			iCtr2   += 1
			
			if ((iCtr2 % 8) == 0):  # 8 -> Fixed Num of colums in Catalog Column Table (davisbase_columns.tbl)
				
				iCtr1   = 0
			
			if (iCtr1 == 4):
				
				sTmp    = sItem.split ('|') [0]
				
				# Add key to dictionary
				odTableStructure [sTmp.decode (encoding='utf-8')]       = ''
				odTableValue [sTmp.decode (encoding='utf-8')]           = ''
				
				lHeaders.append (sTmp.decode (encoding='utf-8'))
				
			elif (iCtr1 == 5):
				
				# Add value corresponding to the key
				odTableStructure [sTmp.decode (encoding='utf-8')]       = sItem.split ('|') [0]
				
				iNumCol += 1
		
		# Reset the variable
		lTmp	= []
		
		self.joBPTTBLProcessNormal	= bPlusTree.javaBPTTblSQLProcessing (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['TABLE']),
										     HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['TABLE'] + '.tbl'),
                                                                      		     HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['TABLE'] + '.tbl'))
		
		for sItem in self.joBPTTBLProcessNormal.getTableValuesFromNormal ():
			
			iNumRow	+= 1
			
			lTmp	= sItem.split ('~')
			
			for sItem2 in lTmp:
				
				lTmp2.append (str (sItem2))
		
		HDR.goDebug.write (psMsg='[%s] - Delimited Records in lTmp2 = [%s]' %(HDR.OS.path.basename(__file__), lTmp2))
		
		# If no WHERE_CONDITION is given, delete all rows
		if not 'WHERE_COND' in pdQuery.keys ():
			
			iCtr	= 0
			
			lTmp	= []
			
			while (iCtr < len (lTmp2)):
			
				for sKey, sValue in odTableValue.items ():
					
					odTableValue [sKey]	= lTmp2 [iCtr]
					
					# ROWID is the Key in the .tbl File, which is deleted later
					if sKey != 'ROWID':
						
						self.deleteFromIndex (sKey, odTableValue [sKey], odTableStructure [sKey], pdQuery)
					
					iCtr	+= 1
				
				lTmp.append (str (odTableValue ['ROWID']))
			
			HDR.goDebug.write (psMsg='[%s] - Records in New List [%s]' %(HDR.OS.path.basename(__file__), lTmp))
			
			self.joBPTTBLProcessNormal.deleteTableValuesFromNormal (lTmp)
		
		else:
			
			iCtr	= 0
			
			lTmp	= []
			
			while (iCtr < len (lTmp2)):
				
				for sKey, sValue in odTableValue.items ():
					
					odTableValue[sKey]	= lTmp2 [iCtr]
					
					iCtr	+= 1
				
				if (len (pdQuery ['WHERE_COND']) == 1):
					
					iCtr2	= 0
					
					while (iCtr2 < len (pdQuery ['WHERE_COND'])):
						
						sVar1	= pdQuery ['WHERE_COND'][0] [iCtr2]
						iCtr2	+= 1
						
						sOp	= pdQuery ['WHERE_COND'][0] [iCtr2]
						iCtr2	+= 1
						
						sVar2	= pdQuery ['WHERE_COND'][0] [iCtr2]
						iCtr2	+= 1
						
					oOpFunc	= HDR.dOpFunc [sOp]
					
					if (odTableValue [sVar1].lower () != 'null'):
						
						sVal1	= HDR.castValue (odTableValue [sVar1], odTableStructure [sVar1])
					
					else:
						
						sVal1   = odTableValue [sVar1].lower ()
					
					if (sVar2.lower () != 'null'):
						
						sVal2	= HDR.castValue (sVar2, odTableStructure [sVar1])	# Try to cast it to same data type as Column
					
					else:
						
						sVal2   = sVar2.lower ()
					
					if (odTableStructure [sVar1] == 'DATETIME') or (odTableStructure [sVar1] == 'DATE'):
						
						sVal2   = HDR.convertDT (0, str (sVal2))
						
						if (odTableStructure [sVar1] == 'DATE'):
							
							sVal2   = sVal2 [:10]
					
					if (sVal2.lower () != 'null'):
						
						# Recast sVal2 Here
						sVal2   = HDR.castValue (sVal2, odTableStructure [sVar1])
					
					bResult	= oOpFunc (sVal1, sVal2)
					
					if bResult == True:
						
						for sKey, sValue in odTableValue.items ():
							
							# ROWID is the Key in the .tbl File, which is deleted later
							if sKey != 'ROWID':
								
								self.deleteFromIndex (sKey, sValue, odTableStructure [sKey], pdQuery)
						
						lTmp.append (str (odTableValue ['ROWID']))
		
		if (lTmp):
			
			HDR.goDebug.write (psMsg='[%s] - Final Records in New List [%s]' %(HDR.OS.path.basename(__file__), lTmp))
			
			self.joBPTTBLProcessNormal.deleteTableValuesFromNormal (lTmp)
		
		# Update the File
		self.joBPTTblWrite	= bPlusTree.javaBPTTblInsert (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], pdQuery ['TABLE'] + '.tbl'),
                                                                      HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['TABLE'] + '.tbl'),
                                                                      HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['TABLE'] + '.tbl'),
                                                                      HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['TABLE'] + '.tbl'),
                                                                      HDR.goDebug, HDR.goBPTPD)
		
		self.joBPTTblWrite.write ()
		
                return

	def deleteFromIndex (self, psKey='', psValue='', psDataType='', pdQuery={}):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), str (psKey)))
		
		oTmp	= bPlusTree.javaBPTTblSQLProcessing (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + psKey),
								        HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + psKey + '.idx'),
									HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + psKey + '.idx'))
		
		oTmp.deleteTableValuesFromIndex (psValue, psDataType)
		
		# Update the File
		oTmpWrite	= bPlusTree.javaBPTTblInsert (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], psKey + '.idx'),
               	                                                      HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + psKey + '.idx'),
                       	                                              HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + psKey + '.idx'),
                               	                                      HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + psKey + '.idx'),
                                       	                              HDR.goDebug, HDR.goBPTPD)
		
		oTmpWrite.write ()
		
		return

def startProcedure (poXML=None):
	
	ROUTINE	= HDR.SYS._getframe().f_code.co_name
	
	oStart	= initQuery ()
	
	return	oStart
	
if __name__ == '__main__':
	
	startProcedure (poXML=None)
