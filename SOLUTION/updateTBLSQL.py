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
		
		UPDATE (TABLE_NAME) SET (COLUMN_NAME)=(TOK_VALUE) [WHERE CONDITION]
		
		(TABLE_NAME)		-> Formatted as ABC, XYZ (or) ABC (or) SCHEMA1.ABC, SCHEMA2.XYZ		
		(COLUMN_NAME)		-> Formatted as ABC, XYZ (or) ABC (or) SCHEMA1.ABC, SCHEMA2.XYZ		
		(CONDITION)		-> Can be one from <, <=, >, >=, =, !=, eq, ne, lt, le, gt, ge, AND, OR
		
		'''
		
		self.joBPTTBLProcessCtg		= None
		self.joBPTTBLProcessNormal	= None
		self.joBPTTblUpdateNormal	= None
		
		self.sStmt	= HDR.PARSER.Forward ()
		self.exprWhere	= HDR.PARSER.Forward ()
		
		self.sKeyword01	= HDR.PARSER.Keyword ('UPDATE', caseless=True).setResultsName ('OPERATION')
		self.sKeyword02	= HDR.PARSER.Keyword ('SET', caseless=True)
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
		
		self.tokValues	= HDR.PARSER.Word (HDR.PARSER.printables, excludeChars=';') | HDR.PARSER.Regex (r"()")
		
		self.tokWhere	= HDR.PARSER.Group ((self.tokColumns + self.tokArithOP + self.tokValues)).setName ('WHERE_CONDITION')
		
		self.exprWhere	<<= self.tokWhere + HDR.PARSER.ZeroOrMore ((self.sKeywordAND | self.sKeywordOR) + self.exprWhere)
		
		# Finally Define the Grammar for the Statement/Query Here
		
		self.sStmt	<<= (self.sKeyword01 + self.tokTablesList + self.sKeyword02 + 							\
				     self.tokColumns.setResultsName ('COLUMN') + '=' + self.tokValues.setResultsName ('VALUE') +		\
				     HDR.PARSER.Optional (HDR.PARSER.Group (self.sKeyword03 + self.exprWhere)).setResultsName ('WHERE_COND') +  \
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
					
					elif sKey == 'COLUMN':
						
						dQuery [sKey]	= Value
					
					elif sKey == 'VALUE':
						
						dQuery [sKey]	= Value
					
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
		lFlat		= []
		lFlat2		= []
		
		iCtr1	= 0
		iCtr2	= 0
		iNumCol	= 0
		iNumRow	= 0
		
		iRecordSize	= 0
		
		bFlag	= False
		
		if not HDR.OS.path.exists (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'])):
			
			HDR.printException ('DB010', 'Table [%s] does not exist' %pdQuery ['TABLE'])
			
			return
		
		# Create a Ordered Dictionary (COLUMN_NAME (Key) -> DATA_TYPE (Value))
                odTableStructure        = HDR.COLLECTIONS.OrderedDict ()
		
                # Create a Ordered Dictionary (COLUMN_NAME (Key) -> Value)
                odTableValue            = HDR.COLLECTIONS.OrderedDict ()
		odTableValueConstraint	= HDR.COLLECTIONS.OrderedDict ()
		odTableValueNullable	= HDR.COLLECTIONS.OrderedDict ()
		
		self.joBPTTBLProcessCtg = bPlusTree.javaBPTTblSQLProcessing (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['TABLE']),
                                                                             HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_columns.tbl'),
                                                                             HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_columns.tbl'))
		
		lTmp    = self.joBPTTBLProcessCtg.getTableStructureFromCatalog (pdQuery ['TABLE']);
		
		for sItem in lTmp:
			
			iCtr1   += 1
			iCtr2   += 1
			
			if ((iCtr2 % 8) == 0):  # 8 -> Fixed Num of colums in Catalog Column Table (davisbase_columns.tbl)
				
				odTableValueConstraint [sTmp.decode (encoding='utf-8')]	= sItem.split ('|') [0]
				
				iCtr1   = 0
			
			if (iCtr1 == 4):
				
				sTmp    = sItem.split ('|') [0]
				
				# Add key to dictionary
				odTableStructure [sTmp.decode (encoding='utf-8')]       = ''
				odTableValue [sTmp.decode (encoding='utf-8')]           = ''
				odTableValueNullable [sTmp.decode (encoding='utf-8')]	= ''
				odTableValueConstraint [sTmp.decode (encoding='utf-8')]	= ''
				
				lHeaders.append (sTmp.decode (encoding='utf-8'))
				
			elif (iCtr1 == 5):
				
				# Add value corresponding to the key
				odTableStructure [sTmp.decode (encoding='utf-8')]       = sItem.split ('|') [0]
				
				iNumCol += 1
			
			elif (iCtr1 == 7):
				
				odTableValueNullable [sTmp.decode (encoding='utf-8')]	= sItem.split ('|') [0]
		
		# Reset the variables
		lTmp	= []
		
		# Check for constraints here
		if odTableValueNullable [pdQuery ['COLUMN']] == 'N':
					
			if pdQuery ['VALUE'].lower () == 'null':
				
				HDR.printException ('DB008', 'Field [%s] is Not Nullable. Cannot Nullify the same' %(str (pdQuery ['COLUMN'])))
				
				return
		
		self.joBPTTBLProcessNormal	= bPlusTree.javaBPTTblSQLProcessing (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['TABLE']),
										     HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['TABLE'] + '.tbl'),
                                                                      		     HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['TABLE'] + '.tbl'))
		
		for sItem in self.joBPTTBLProcessNormal.getTableValuesFromNormal ():
			
			iNumRow	+= 1
			
			lTmp	= sItem.split ('~')
			
			for sItem2 in lTmp:
				
				lTmp2.append (str (sItem2))
		
		iCtr1	= 0
		
		# Update to the File
		self.joBPTTBLUpdateNormal = bPlusTree.javaBPTTblInsert (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], pdQuery ['TABLE'] + '.tbl'),
									HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['TABLE'] + '.tbl' + '.update'),
									HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['TABLE'] + '.tbl'),
									HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['TABLE'] + '.tbl'),
									HDR.goDebug, HDR.goBPTPD)
				
		
		# If no WHERE_CONDITION is given, update all rows
		if not 'WHERE_COND' in pdQuery.keys ():
			
			lTmp	= []
			
			while (iCtr1 < len (lTmp2)):
			
				for sKey, sValue in odTableValue.items ():
					
					odTableValue [sKey]	= lTmp2 [iCtr1]
					
					iCtr1	+= 1
			
				if pdQuery ['COLUMN'] != 'ROWID' and odTableValueConstraint [pdQuery ['COLUMN']] == 'PK':
				
					oTmp1	= bPlusTree.javaBPTTblInsert (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], pdQuery ['COLUMN'] + '.idx'),
									HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['COLUMN'] + '.idx' + '.update'),
									HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['COLUMN'] + '.idx'),
									HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['COLUMN'] + '.idx'),
									HDR.goDebug, HDR.goBPTPD)
					
					if (oTmp1.IdxFind (pdQuery ['VALUE']) == 1):
							
						HDR.printException ('DB009', 'Field [%s] is a [%s]. Cannot have duplicate values for this field'
									     %(str(pdQuery ['COLUMN']), str (odTableValueConstraint [pdQuery ['COLUMN']])))
						
						return
				
				odTableValue [pdQuery ['COLUMN']]	= pdQuery ['VALUE']
				
				lFlat	= []
				lFlat2	= []
				
				iCtr2		= 0
				iRecordSize	= 0
				
				# Flatten the query here
				for sKey, sValue in odTableValue.items ():
					
					iCtr2	+= 1
					
					# Handle for DATETIME and DATE here
					if sKey == pdQuery ['COLUMN']:
						
						if (odTableStructure [sKey] == 'DATETIME') or (odTableStructure [sKey] == 'DATE'):
							
							sValue  = HDR.convertDT (0, sValue)
						
						if (sValue.lower () == 'null') and (odTableStructure [sKey] != 'TEXT') and	\
						   (odTableStructure [sKey] [:4] != 'null'):
							
							odTableStructure [sKey] = sValue.lower () + '_' + odTableStructure [sKey]
					
					else:
						
						if (sValue.lower () == 'null') and (odTableStructure [sKey] != 'TEXT') and	\
						   (odTableStructure [sKey] [:4] != 'null'):
							
							odTableStructure [sKey] = sValue.lower () + '_' + odTableStructure [sKey]
					
					lFlat.append ([str (iCtr2), sKey, odTableStructure [sKey].decode (encoding='utf-8'), sValue])
				
				# Update into table here
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
				
				if (lFlat2):
					
					self.updateToIndex (odTableValue [pdQuery ['COLUMN']], pdQuery ['VALUE'], odTableValue, odTableStructure, pdQuery)
					
					self.joBPTTBLUpdateNormal.update (lFlat2, len (lFlat2), iRecordSize)
					
					bFlag	= True
					
		else:
		
			lTmp	= []
			
			while (iCtr1 < len (lTmp2)):
			
				for sKey, sValue in odTableValue.items ():
					
					odTableValue [sKey]	= lTmp2 [iCtr1]
					
					iCtr1	+= 1
				
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
					
					if (sVal2 != 'null'):
						
						# Recast sVal2 Here
						sVal2   = HDR.castValue (sVal2, odTableStructure [sVar1])
					
					bResult	= oOpFunc (sVal1, sVal2)
					
					# Reset the variable
					iCtr2	= 0
					
					if bResult == True:
						
						if pdQuery ['COLUMN'] != 'ROWID' and odTableValueConstraint [pdQuery ['COLUMN']] == 'PK':
							
							oTmp1 = bPlusTree.javaBPTTblInsert(HDR.OS.path.join(HDR.OS.getcwd (), pdQuery ['TABLE'], pdQuery ['COLUMN'] + '.idx'),
									HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['COLUMN'] + '.idx' + '.update'),
									HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['COLUMN'] + '.idx'),
									HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['COLUMN'] + '.idx'),
									HDR.goDebug, HDR.goBPTPD)
							
							if (oTmp1.IdxFind (pdQuery ['VALUE']) == 1):
									
								HDR.printException ('DB009', 'Field [%s] is a [%s]. Cannot have duplicate values for this field'
											     %(str(pdQuery ['COLUMN']), str (odTableValueConstraint [pdQuery ['COLUMN']])))
								
								return
						
						odTableValue [pdQuery ['COLUMN']]	= pdQuery ['VALUE']
						
						lFlat	= []
						lFlat2	= []
						
						iCtr2		= 0
						iRecordSize	= 0
						
						# Flatten the query here
						for sKey, sValue in odTableValue.items ():
							
							iCtr2	+= 1
							
							HDR.goDebug.write (psMsg='[%s] - Where Condition sKey [%s], sValue [%s]' 	\
									   %(HDR.OS.path.basename(__file__), sKey, sValue))
							
							# Handle for DATETIME and DATE here
							if sKey == pdQuery ['COLUMN']:
								
								HDR.goDebug.write (psMsg='[%s] - If part sKey = pdQuery [COLUMN], Constraint [%s]'	\
										 %(HDR.OS.path.basename(__file__), odTableStructure [sKey]))
								
								if (odTableStructure [sKey] == 'DATETIME') or (odTableStructure [sKey] == 'DATE'):
									
									sValue  = HDR.convertDT (0, sValue)
								
								HDR.goDebug.write (psMsg='[%s] - If sValue [%s]' %(HDR.OS.path.basename(__file__), sValue))
								
								if (sValue.lower () == 'null') and (odTableStructure [sKey] != 'TEXT') and	\
								   (odTableStructure [sKey] [:4] != 'null'):
									
									odTableStructure [sKey] = sValue.lower () + '_' + odTableStructure [sKey]
							
							else:
								
								HDR.goDebug.write (psMsg='[%s] - Else part sKey = pdQuery [COLUMN], Constraint [%s]'	\
										  %(HDR.OS.path.basename(__file__), odTableStructure [sKey])) 
								
								HDR.goDebug.write (psMsg='[%s] - Else sValue [%s]' %(HDR.OS.path.basename(__file__), sValue))
								
								if (sValue.lower () == 'null') and (odTableStructure [sKey] != 'TEXT') and	\
								   (odTableStructure [sKey] [:4] != 'null'):
									
									odTableStructure [sKey] = sValue.lower () + '_' + odTableStructure [sKey]
							
							lFlat.append ([str (iCtr2), sKey, odTableStructure [sKey].decode (encoding='utf-8'), sValue])
						
						# Update into table here
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
						
						if (lFlat2):
							
							self.updateToIndex (odTableValue [pdQuery ['COLUMN']], pdQuery ['VALUE'], odTableValue, odTableStructure, pdQuery)
							
							self.joBPTTBLUpdateNormal.update (lFlat2, len (lFlat2), iRecordSize)
							
							bFlag	= True
		
		if bFlag == True:
			
			self.joBPTTBLUpdateNormal.write ()
		
                return

	def updateToIndex (self, psOldKey, psNewKey, pdTableValue, pdTableStructure, pdQuery):
		
		ROUTINE = HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), str (psOldKey)))
		
		oTmp   = bPlusTree.javaBPTTblSQLProcessing (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['COLUMN']),
									HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['COLUMN'] + '.idx'),
									HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['COLUMN'] + '.idx'))
		
		oTmp.deleteTableValuesFromIndex (psOldKey, pdTableStructure [pdQuery ['COLUMN']])
		
		# Flatten the Record with new key for Insertion
		lFlat	= []
		lFlat2	= []
		
		sValue	= ''
		
		iCtr		= 0
		iRecordSize	= 0
		
		iCtr	+= 1
		
		# Handle for DATETIME and DATE here
		if (pdTableStructure [pdQuery ['COLUMN']] == 'DATETIME') or (pdTableStructure [pdQuery ['COLUMN']] == 'DATE'):
			
			sValue  = HDR.convertDT (0, sValue)
		
		lFlat.append ([str (iCtr), pdQuery ['COLUMN'].decode (encoding='utf-8'), pdTableStructure [pdQuery ['COLUMN']].decode (encoding='utf-8'), psNewKey])
		
		iCtr    += 1
		
		lFlat.append ([str (iCtr), 'ROWID', pdTableStructure ['ROWID'].decode (encoding='utf-8'), pdTableValue ['ROWID']])
		
		HDR.goDebug.write (psMsg='[%s] - Update Index lFlat [%s]' %(HDR.OS.path.basename(__file__), lFlat))
		
		# Insert into table here
		for item in lFlat:
			
			HDR.goDebug.write (psMsg='[%s] - Update Index item [%s]' %(HDR.OS.path.basename(__file__), item))
			
			item.append (HDR.dDataTypeSerialCode [item [2]])
			
			# For DATA_TYPE as TEXT, calculate the length of the actual text and append to list
			if (HDR.dDataTypeSerialCode [item [2]] == '0x0C'):
				
				item.append (str (len (item [3])))
				
				iRecordSize     = iRecordSize + len (item [3])
			
			else:
				
				item.append (HDR.dDataTypeLength [item [2]])
				
				iRecordSize     = iRecordSize   + int (HDR.dDataTypeLength [item [2]])
				
			lFlat2.append (item)
		
		HDR.goDebug.write (psMsg='[%s] - Update Index lFlat2 appended [%s]' %(HDR.OS.path.basename(__file__), lFlat2))
		
		# Update the File
		oTmpWrite	= bPlusTree.javaBPTTblInsert (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], pdQuery ['COLUMN'] + '.idx'),
									HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['COLUMN'] + '.idx' + '.update'),
									HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['COLUMN'] + '.idx'),
									HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'], '.' + pdQuery ['COLUMN'] + '.idx'),
									HDR.goDebug, HDR.goBPTPD)
		
		oTmpWrite.insertIndex (lFlat2, len (lFlat2), iRecordSize, str (psNewKey))
		
      		oTmpWrite.writeIndex ()
		
		return

def startProcedure (poXML=None):
	
	ROUTINE	= HDR.SYS._getframe().f_code.co_name
	
	oStart	= initQuery ()
	
	return	oStart
	
if __name__ == '__main__':
	
	startProcedure (poXML=None)
