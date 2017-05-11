#!/usr/bin/env jython

# Custom Libraries goes here
import	customHDR	as HDR

# Jython - Import Java Libraries here
import  bPlusTree.javaBPTTblInsert
import  bPlusTree.javaBPTTblSQLProcessing

class initQuery:
	
	def __init__ (self):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		'''
		Considering the following Syntax for the grammer construction.
		
		INSERT INTO (TABLE_NAME) VALUES (v1,v2..)
		
		(TABLE_NAME[S])		-> Formatted as ABC, XYZ (or) ABC (or) SCHEMA1.ABC, SCHEMA2.XYZ		
		(v1,v2..)		-> Values to enter
		
		'''
		self.joBPTTBLProcessCtg		= None
		self.joBPTTBLInsertNormal	= None
		
		self.sStmt	= HDR.PARSER.Forward ()
		self.exprWhere	= HDR.PARSER.Forward ()
		
		self.sKeyword01	= HDR.PARSER.Keyword ('INSERT', caseless=True).setResultsName ('OPERATION')
		self.sKeyword02	= HDR.PARSER.Keyword ('INTO', caseless=True)
		self.sKeyword03	= HDR.PARSER.Keyword ('VALUES', caseless=True)
		self.sTerm	= HDR.PARSER.CaselessLiteral (";")
		
		self.sKeywordAND	= HDR.PARSER.Keyword ('AND', caseless=True)
		self.sKeywordOR		= HDR.PARSER.Keyword ('OR', caseless=True)
		
		self.tokValues	= HDR.PARSER.Word (HDR.PARSER.printables, excludeChars='(),') | HDR.PARSER.Regex (r"()")
		self.tokTables	= HDR.PARSER.Word (HDR.PARSER.alphanums).setName ('TABLE_NAME')
		
		self.tokTablesList	= HDR.PARSER.Group (HDR.PARSER.delimitedList (self.tokTables))
		self.tokValuesList	= HDR.PARSER.Group (HDR.PARSER.delimitedList (self.tokValues, delim=',', combine=False))
		
		self.tokValuesList	= self.tokValuesList.setResultsName ('VALUES')
		self.tokTablesList	= self.tokTablesList.setResultsName ('TABLE')
		
		# Finally Define the Grammar for the Statement/Query Here
		
		self.sStmt	<<= (self.sKeyword01 + self.sKeyword02 + self.tokTablesList + self.sKeyword03 + '(' + self.tokValuesList + ')' + self.sTerm)
		
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
				
				self.processQuery (dQuery)
			
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
		
		try:
			
			if not ptTokens:
				
				raise Exception ('Query Tokens are Empty. Critical!!!. Cannot Interpret')
			
			else:
			
				dTmp	= ptTokens [0][0][1]
				
				for sKey, Value in dTmp.items ():
					
					if sKey == 'OPERATION':
						
						dQuery [sKey]	= str (Value)
					
					elif sKey == 'VALUES':
						
						dQuery [sKey]	= ','.join (Value)
					
					elif sKey == 'TABLE':
						
						dQuery [sKey]	= ','.join (Value)
						
						HDR.goDebug.write (psMsg='[%s] - [%s] Where Condition [%s]' %(HDR.OS.path.basename(__file__),
								   ROUTINE, str (dQuery [sKey])))
					
					else:
						
						raise Exception ('Unexpected Keyword in the Syntax. Critical!!!')
		
		except Exception as e:
			
			raise Exception (e)
		
		return dQuery
	
	def processQuery (self, pdQuery={}):
		
		ROUTINE = HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), str (pdQuery)))
		
		# Create a Ordered Dictionary (COLUMN_NAME (Key) -> DATA_TYPE (Value))
		odTableStructure	= HDR.COLLECTIONS.OrderedDict ()
		
		# Create a Ordered Dictionary (COLUMN_NAME (Key) -> Value)
		odTableValue		= HDR.COLLECTIONS.OrderedDict ()
		odTableValueNullable	= HDR.COLLECTIONS.OrderedDict ()
		odTableValueConstraint	= HDR.COLLECTIONS.OrderedDict ()
		
		sTmp		= ''
		
		lTmp		= []
		
		iNumCol	= 0
		iCtr1	= 0
		iCtr2	= 0
		
		HDR.goDebug.write (psMsg='[%s] - HDR.OS.path.join (HDR.OS.getcwd (), pdQuery [TABLE]) = [%s]'\
				   %(HDR.OS.path.basename(__file__), HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'])))
		
		if not HDR.OS.path.exists (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'])):
			
			HDR.printException ('DB006', 'Cannot find %s. No such Table exists.' %(pdQuery ['TABLE']))
		
		else:
			
			sTmp	= '.' + pdQuery ['TABLE']
			
			self.joBPTTBLProcessCtg	= bPlusTree.javaBPTTblSQLProcessing (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], sTmp),
										     HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_columns.tbl'),
										     HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_columns.tbl'))
			
			# Get the Table structure
			lTmp	= self.joBPTTBLProcessCtg.getTableStructureFromCatalog (pdQuery ['TABLE']);
			
			for sItem in lTmp:
				
				iCtr1	+= 1
				iCtr2	+= 1
				
				if ((iCtr2 % 8) == 0):	# 8 -> Fixed Num of colums in Catalog Column Table (davisbase_columns.tbl)
					
					odTableValueConstraint [sTmp.decode (encoding='utf-8')]	= sItem.split ('|')[0]
					
					iCtr1	= 0
				
				if (iCtr1 == 4):
					
					sTmp	= sItem.split ('|') [0]
					
					# Add key to dictionary
					odTableStructure [sTmp.decode (encoding='utf-8')]	= ''
					odTableValue [sTmp.decode (encoding='utf-8')] 		= ''
					odTableValueNullable [sTmp.decode (encoding='utf-8')]	= ''
					odTableValueConstraint [sTmp.decode (encoding='utf-8')]	= ''
					
				elif (iCtr1 == 5):
					
					# Add value corresponding to the key
					odTableStructure [sTmp.decode (encoding='utf-8')]	= sItem.split ('|') [0]
					
					iNumCol	+= 1
				
				elif (iCtr1 == 7):
					
					odTableValueNullable [sTmp.decode (encoding='utf-8')]	= sItem.split ('|')[0]
		
		iCtr1	= 0
		
		lTmp	= pdQuery ['VALUES'].split (',')
		
		if (iNumCol == 1):
			
			iNumCol	+= 1
		
		if (len (lTmp) == (iNumCol - 1)):		# Exclude ROWID as its internally populated
		
			for sKey, sValue in odTableStructure.items ():
				
				if sKey != 'ROWID':		# Do not populate ROWID Column
				
					odTableValue [sKey]	= lTmp [iCtr1]
					
					iCtr1	+= 1
			
			self.insertIntoTable (odTableStructure, odTableValue, odTableValueNullable, odTableValueConstraint, pdQuery)
		
		else:
			
			HDR.printException ('DB007', 'Cannot Insert. Table structure and Insert Column Count Mismatch')
		
		return
	
	def insertIntoTable (self, pdTableStructure, pdTableValue, pdTableValueNullable, pdTableValueConstraint, pdQuery={}):
		
		ROUTINE = HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), str (pdQuery)))
		
		lFlat	= []
		lFlat2	= []
		
		iRowID	= 0
		iCtr	= 0
		
		iRecordSize	= 0
		bFlag		= False
		
		# Format the data to insert into table
		# Get the Last Row ID
                iRowID  = int (HDR.pickle (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.config'),
                                           1, 'LAST_ROW_ID_NORMAL_TABLES', 0))
		
                if iRowID < 0:
			
                        raise Exception ('Database Table Corrupted.. Critical. Aborting!!!')
			
                else:
			
                        iRowID  += 1
		
		pdTableValue ['ROWID']	= str (iRowID)
		
		# Flatten the query here
		for sKey, sValue in pdTableValue.items ():
			
			iCtr	+= 1
			
			# Handle for DATETIME and DATE here
			if (pdTableStructure [sKey] == 'DATETIME') or (pdTableStructure [sKey] == 'DATE'):
				
				sValue	= HDR.convertDT (0, sValue)
			
			if (sValue.lower () == 'null') and (pdTableStructure [sKey] != 'TEXT'):
				
				pdTableStructure [sKey]	= sValue.lower () + '_' + pdTableStructure [sKey]
			
			lFlat.append ([str (iCtr), sKey, pdTableStructure [sKey].decode (encoding='utf-8'), sValue])
		
		# Insert into table here
		for item in lFlat:
			
                        item.append (HDR.dDataTypeSerialCode [item [2]])
			
                        # For DATA_TYPE as TEXT, calculate the length of the actual text and append to list
                        if (HDR.dDataTypeSerialCode [item [2]] == '0x0C'):
				
                                item.append (str (len (item [3])))
				
                                iRecordSize     = iRecordSize + len (item [3])
                        else:
				
                                item.append (HDR.dDataTypeLength [item [2]])
				
                                iRecordSize     = iRecordSize   + int (HDR.dDataTypeLength [item [2]])
			
                        lFlat2.append (item)
		
                if lFlat2:
			
			# Check for Constraint and Nullables here. If everything passes, then insert.
			# If any condition fails, then do not insert
			
			for sKey, sValue in pdTableValueNullable.items ():
				
				if pdTableValueNullable [sKey] == 'N':
					
					if pdTableValue [sKey].lower ()	== 'null':
						
						HDR.printException ('DB008', 'Field [%s] is Not Nullable. Cannot Nullify the same' %(str (sKey)))
						
						bFlag	= True
			
			if bFlag == False:
				
				for sKey, sValue in pdTableValue.items ():
					
					if sKey != 'ROWID' and pdTableValueConstraint [sKey] == 'PK':
						
						self.joBPTTBLInsertNormal = bPlusTree.javaBPTTblInsert (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], sKey + '.idx'),
                               	                                            HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + sKey + '.idx' + '.insert'),
                               	                                            HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + sKey + '.idx'),
                                              	        	            HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + sKey + '.idx'),
                                                	                    HDR.goDebug, HDR.goBPTPD)
						
						
						if (self.joBPTTBLInsertNormal.IdxFind (sValue) == 1):
							
							HDR.printException ('DB009', 'Field [%s] is a [%s]. Cannot have duplicate values for this field'
										     %(str(sKey), str (pdTableValueConstraint [sKey])))
							
							bFlag	= True
							
							break
			
			if bFlag == False:
				
				self.joBPTTBLInsertNormal	= bPlusTree.javaBPTTblInsert (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], pdQuery ['TABLE'] + '.tbl'),
                                                                      HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['TABLE'] + '.tbl' + '.insert'),
                                                                      HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['TABLE'] + '.tbl'),
                                                                      HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['TABLE'] + '.tbl'),
                                                                      HDR.goDebug, HDR.goBPTPD)
				
				self.joBPTTBLInsertNormal.insert (lFlat2, len (lFlat2), iRecordSize)
				
				self.joBPTTBLInsertNormal.write ()
				
				# Write the indexes for every column here
				self.writeIndex (pdTableValue, pdTableStructure, pdQuery)
				
				lFlat2  = []
				
				iRecordSize     = 0
		
				# Write the last ROW ID to the config file if everything is successful
                		HDR.pickle (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.config'),
                            				      0, 'LAST_ROW_ID_NORMAL_TABLES', iRowID)
		
		return
	
	def writeIndex (self, pdTableValue={}, pdTableStructure={}, pdQuery={}):
		
		ROUTINE = HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), str (pdQuery)))
		
		for sKey, sValue in pdTableValue.items ():
			
			lFlat	= []
			lFlat2	= []
			
			iCtr		= 0
			iRecordSize	= 0
			
			if sKey != 'ROWID':
				
				self.joBPTTBLInsertNormal = bPlusTree.javaBPTTblInsert (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], sKey + '.idx'),
                               	                                    HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + sKey + '.idx' + '.insert'),
                               	                                    HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + sKey + '.idx'),
                                      	        	            HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + sKey + '.idx'),
                                               	                    HDR.goDebug, HDR.goBPTPD)
				
				iCtr	+= 1
				
				# Handle for DATETIME and DATE here
				if (pdTableStructure [sKey] == 'DATETIME') or (pdTableStructure [sKey] == 'DATE'):
					
					sValue	= HDR.convertDT (0, sValue)
				
				lFlat.append ([str (iCtr), sKey, pdTableStructure [sKey].decode (encoding='utf-8'), sValue])
				
				iCtr	+= 1
				
				lFlat.append ([str (iCtr), 'ROWID', pdTableStructure ['ROWID'].decode (encoding='utf-8'), pdTableValue ['ROWID']])
				
				# Insert into table here
				for item in lFlat:
					
					item.append (HDR.dDataTypeSerialCode [item [2]])
					
					# For DATA_TYPE as TEXT, calculate the length of the actual text and append to list
					if (HDR.dDataTypeSerialCode [item [2]] == '0x0C'):
						
						item.append (str (len (item [3])))
						
						iRecordSize     = iRecordSize + len (item [3])
					else:
						
                                		item.append (HDR.dDataTypeLength [item [2]])
						
                                		iRecordSize     = iRecordSize   + int (HDR.dDataTypeLength [item [2]])
					
                        		lFlat2.append (item)
				
				self.joBPTTBLInsertNormal.insertIndex (lFlat2, len (lFlat2), iRecordSize, str (sValue))
				
				self.joBPTTBLInsertNormal.writeIndex ()
				
		return

def startProcedure (poXML=None):
	
	ROUTINE	= HDR.SYS._getframe().f_code.co_name
	
	oStart	= initQuery ()
	
	return	oStart
	
if __name__ == '__main__':
	
	startProcedure (poXML=None)
