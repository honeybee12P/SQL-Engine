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
		
		SELECT (COLUMN_NAME[S]) FROM (TABLE_NAME[S]) WHERE (CONDITION)
		
		(COLUMN_NAME[S])	-> Formatted as ABC, XYZ (or) ABC (or) TABLE2.ABC, TABLE2.XYZ		
		(TABLE_NAME[S])		-> Formatted as ABC, XYZ (or) ABC (or) SCHEMA1.ABC, SCHEMA2.XYZ		
		(CONDITION)		-> Can be one from <, <=, >, >=, =, !=, eq, ne, lt, le, gt, ge, AND, OR, IN (NESTED SELECT STATEMENT)
		
		'''
		
		self.joBPTTBLProcessCtg		= None
		self.joBPTTBLProcessNormal	= None
		
		self.sStmt	= HDR.PARSER.Forward ()
		self.exprWhere	= HDR.PARSER.Forward ()
		
		self.sKeyword01	= HDR.PARSER.Keyword ('SELECT', caseless=True).setResultsName ('OPERATION')
		self.sKeyword02	= HDR.PARSER.Keyword ('FROM', caseless=True)
		self.sKeyword03	= HDR.PARSER.Keyword ('WHERE', caseless=True)
		self.sTerm	= HDR.PARSER.CaselessLiteral (";")
		
		self.sKeywordAND	= HDR.PARSER.Keyword ('AND', caseless=True)
		self.sKeywordOR		= HDR.PARSER.Keyword ('OR', caseless=True)
		
		self.tokColumns	= HDR.PARSER.delimitedList (HDR.PARSER.Word (HDR.PARSER.alphanums).setName ('COLUMN_NAMES'), delim='.', combine=True)
		self.tokColumns	= self.tokColumns | '*'
		
		self.tokTables	= HDR.PARSER.delimitedList (HDR.PARSER.Word (HDR.PARSER.alphanums).setName ('TABLE_NAMES'), delim='.', combine=True)
		self.tokTables	= HDR.PARSER.Word (HDR.PARSER.printables, excludeChars=';') | self.tokTables
		
		self.tokColumnsList	= HDR.PARSER.Group (HDR.PARSER.delimitedList (self.tokColumns))
		self.tokTablesList	= HDR.PARSER.Group (HDR.PARSER.delimitedList (self.tokTables))
		
		self.tokColumnsList	= self.tokColumnsList.setResultsName ('COLUMNS')
		self.tokTablesList	= self.tokTablesList.setResultsName ('TABLES')
		
		self.tokArithOP	= HDR.PARSER.oneOf ("= != < > >= <= eq ne lt le gt ge", caseless=True)
		
		self.sArithSign	= HDR.PARSER.Word ("+-", exact=1).setName ('SIGN_QUALIFIER (+/-)')
		
		# Following Definition is Not currently used
		#self.sINT	= HDR.PARSER.Combine (HDR.PARSER.Optional (self.sArithSign) + HDR.PARSER.Word (HDR.PARSER.nums))
		
		self.tokValues	= HDR.PARSER.Word (HDR.PARSER.printables, excludeChars=';') | HDR.PARSER.Regex (r"()")
		self.tokWhere	= HDR.PARSER.Group ((self.tokColumns + self.tokArithOP + self.tokValues)).setName ('WHERE_CONDITION')
		
		self.exprWhere	<<= self.tokWhere + HDR.PARSER.ZeroOrMore ((self.sKeywordAND | self.sKeywordOR) + self.exprWhere)
		
		# Finally Define the Grammar for the Statement/Query Here
		
		self.sStmt	<<= (self.sKeyword01 + self.tokColumnsList + self.sKeyword02 + 	\
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
				
				if dQuery:
					
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
		
		# To delete the following commented lines
		
		try:
			
			if not ptTokens:
				
				raise Exception ('Query Tokens are Empty. Critical!!!. Cannot Interpret')
			
			else:
			
				dTmp	= ptTokens [0][0][1]
				
				for sKey, Value in dTmp.items ():
					
					if sKey == 'OPERATION':
						
						dQuery [sKey]	= str (Value)
					
					elif sKey == 'COLUMNS':
						
						dQuery [sKey]	= ','.join (Value)
					
					elif sKey == 'TABLES':
						
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
		
		except Exception as e:
			
			raise Exception (e)
		
		return dQuery
	
	def processQuery (self, pdQuery={}):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), str (pdQuery)))
		
		sTmp    = '' 
		
		lTmp		= []
		lTmp2		= []
		lHeaders	= []
		
		iCtr1	= 0
		iCtr2	= 0
		iNumCol	= 0
		iNumRow	= 0
		
		bResult	= False
		
		# Create a Ordered Dictionary (COLUMN_NAME (Key) -> DATA_TYPE (Value))
		odTableStructure        = HDR.COLLECTIONS.OrderedDict ()
		odTableValue		= HDR.COLLECTIONS.OrderedDict ()
		odTableValueConstraint	= HDR.COLLECTIONS.OrderedDict ()
		odTableValueNullable	= HDR.COLLECTIONS.OrderedDict ()
		
		# Special handling for viewing catalog tables - START ---->
		if pdQuery ['TABLES'].lower () == 'davisbase_columns':
			
			self.joBPTTBLProcessCtg = bPlusTree.javaBPTTblSQLProcessing (HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.' + pdQuery ['TABLES']),
                        	                                                     HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_columns.tbl'),
                                	                                             HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_columns.tbl'))
			
			lTmp    = self.joBPTTBLProcessCtg.getTableStructureFromCatalog (pdQuery ['TABLES'])
			
			for sItem in lTmp:
				
				iCtr1   += 1
				iCtr2   += 1
				
				if ((iCtr2 % 8) == 0):  # 8 -> Fixed Num of colums in Catalog Column Table (davisbase_columns.tbl)
					
					odTableValueConstraint [sTmp.decode (encoding='utf-8')] = sItem.split ('|')[0]
					
					iCtr1   = 0
				
				if (iCtr1 == 4):
					
					sTmp    = sItem.split ('|') [0]
					
					# Add key to dictionary
					odTableStructure [sTmp.decode (encoding='utf-8')]       = ''
					odTableValue [sTmp.decode (encoding='utf-8')]		= ''
					odTableValueConstraint [sTmp.decode (encoding='utf-8')]	= ''
					odTableValueNullable [sTmp.decode (encoding='utf-8')]	= ''
					
					lHeaders.append (sTmp.decode (encoding='utf-8'))
				
				elif (iCtr1 == 5):
					
					# Add value corresponding to the key
					odTableStructure [sTmp.decode (encoding='utf-8')]       = sItem.split ('|') [0]
					
					iNumCol += 1
				
				elif (iCtr1 == 7):
					
					odTableValueNullable [sTmp.decode (encoding='utf-8')]   = sItem.split ('|')[0]
			
			# Reset the variable
			lTmp	= []
			
			self.joBPTTBLProcessNormal = bPlusTree.javaBPTTblSQLProcessing (HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.' + pdQuery ['TABLES']),
								   			HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_columns.tbl'),
								   			HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_columns.tbl'))
			
			for sItem in self.joBPTTBLProcessNormal.getTableValuesFromNormal ():
				
				iNumRow	+= 1
				
				lTmp	= sItem.split ('~')
				
				for sItem2 in lTmp:
					
					lTmp2.append (str (sItem2))
			
			# Reset the variable
			iCtr1	= 0
			
			lTmp	= []
			
			if 'WHERE_COND' in pdQuery.keys ():
				
				iNumRow	= 0
				
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
								
							sVal1	= odTableValue [sVar1].lower ()
						
						if (sVar2.lower () != 'null'):
							
							sVal2	= HDR.castValue (sVar2, odTableStructure [sVar1])	# Try to cast it to same data type as Column
						
						else:
							
							sVal2	= sVar2.lower ()
						
						if (odTableStructure [sVar1] == 'DATETIME') or (odTableStructure [sVar1] == 'DATE'):
							
							sVal2	= HDR.convertDT (0, str (sVal2))
							
							if (odTableStructure [sVar1] == 'DATE'):
								
								sVal2	= sVal2	[:10]
						
						if (sVal2.lower () != 'null'):
							
							# Recast sVal2 Here
							sVal2	= HDR.castValue (sVal2, odTableStructure [sVar1])
						
						bResult	= oOpFunc (sVal1, sVal2)
						
						# Reset the variable
						iCtr2	= 0
						
						if bResult == True:
							
							iNumRow	+= 1
							
							for sKey, sValue in odTableValue.items ():
								
								if (odTableStructure [sKey] == 'DATETIME') or (odTableStructure [sKey] == 'DATE'):
									
									odTableValue [sKey]	= HDR.convertDT (1, str (odTableValue [sKey]))
									
									if (odTableStructure [sKey] == 'DATE'):
										
										odTableValue [sKey]	= odTableValue [sKey][:10]
								
								lTmp.append (str(odTableValue [sKey]))
			
			else:
				
				while (iCtr1 < len (lTmp2)):
					
					for sKey, sValue in odTableValue.items ():
						
						odTableValue [sKey]	= lTmp2 [iCtr1]
						
						iCtr1	+= 1
						
						if (odTableStructure [sKey] == 'DATETIME') or (odTableStructure [sKey] == 'DATE'):
							
							odTableValue [sKey]	= HDR.convertDT (1, str (odTableValue [sKey]))
							
							if (odTableStructure [sKey] == 'DATE'):
								
								odTableValue [sKey]	= odTableValue [sKey][:10]
					
					for sKey, sValue in odTableValue.items ():
						
						lTmp.append (str (odTableValue [sKey]))
			
			lTmp2	= lTmp
			
			if pdQuery ['COLUMNS'] == '*':
			
				# Group List on Per-Row Basis
				lTmp   = (HDR.grouper (lTmp2, piRow=iNumRow, piColumn=iNumCol))
				
				HDR.goDebug.write (psMsg='[%s] - Files New List [%s]' %(HDR.OS.path.basename(__file__), lTmp))
			
			else:
				
				lHeaders	= pdQuery ['COLUMNS'].split (',')
				
				iNumCol		= len (lHeaders)
				
				iCtr	= 0
				
				lTmp	= []
				
				while (iCtr < len (lTmp2)):
				
					for sKey, sValue in odTableStructure.items ():
						
						odTableStructure [sKey]	= lTmp2 [iCtr]
						
						iCtr	+= 1
					
					for sKey in lHeaders:
						
						if sKey in odTableStructure.keys():
						
							lTmp.append (str (odTableStructure [sKey]))
				
				lTmp2	= lTmp
				
				# Group List on Per-Row Basis
				lTmp   = (HDR.grouper (lTmp2, piRow=iNumRow, piColumn=iNumCol))
				
				HDR.goDebug.write (psMsg='[%s] - Files New List [%s]' %(HDR.OS.path.basename(__file__), lTmp))
			
			print ('\n' + HDR.TABULATE.tabulate (lTmp, headers=lHeaders, tablefmt='psql') + '\n')
			
			return
		
		elif pdQuery ['TABLES'].lower () == 'davisbase_tables':
			
			self.joBPTTBLProcessCtg = bPlusTree.javaBPTTblSQLProcessing (HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.' + pdQuery ['TABLES']),
                        	                                                     HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_columns.tbl'),
                                	                                             HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_columns.tbl'))
			
			lTmp    = self.joBPTTBLProcessCtg.getTableStructureFromCatalog (pdQuery ['TABLES'])
			
			for sItem in lTmp:
				
				iCtr1   += 1
				iCtr2   += 1
				
				if ((iCtr2 % 8) == 0):  # 8 -> Fixed Num of colums in Catalog Column Table (davisbase_columns.tbl)
					
					odTableValueConstraint [sTmp.decode (encoding='utf-8')] = sItem.split ('|')[0]
					
					iCtr1   = 0
				
				if (iCtr1 == 4):
					
					sTmp    = sItem.split ('|') [0]
					
					# Add key to dictionary
					odTableStructure [sTmp.decode (encoding='utf-8')]       = ''
					odTableValue [sTmp.decode (encoding='utf-8')]		= ''
					odTableValueConstraint [sTmp.decode (encoding='utf-8')]	= ''
					odTableValueNullable [sTmp.decode (encoding='utf-8')]	= ''
					
					lHeaders.append (sTmp.decode (encoding='utf-8'))
				
				elif (iCtr1 == 5):
					
					# Add value corresponding to the key
					odTableStructure [sTmp.decode (encoding='utf-8')]       = sItem.split ('|') [0]
					
					iNumCol += 1
				
				elif (iCtr1 == 7):
					
					odTableValueNullable [sTmp.decode (encoding='utf-8')]   = sItem.split ('|')[0]
			
			# Reset the variable
			lTmp	= []
			
			self.joBPTTBLProcessNormal = bPlusTree.javaBPTTblSQLProcessing (HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.' + pdQuery ['TABLES']),
								   			HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_tables.tbl'),
								   			HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_tables.tbl'))
			
			for sItem in self.joBPTTBLProcessNormal.getTableValuesFromNormal ():
				
				iNumRow	+= 1
				
				lTmp	= sItem.split ('~')
				
				for sItem2 in lTmp:
					
					lTmp2.append (str (sItem2))
			
			# Reset the variable
			iCtr1	= 0
			
			lTmp	= []
			
			if 'WHERE_COND' in pdQuery.keys ():
				
				iNumRow	= 0
				
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
								
							sVal1	= odTableValue [sVar1].lower ()
						
						if (sVar2.lower () != 'null'):
							
							sVal2	= HDR.castValue (sVar2, odTableStructure [sVar1])	# Try to cast it to same data type as Column
						
						else:
							
							sVal2	= sVar2.lower ()
						
						if (odTableStructure [sVar1] == 'DATETIME') or (odTableStructure [sVar1] == 'DATE'):
							
							sVal2	= HDR.convertDT (0, str (sVal2))
							
							if (odTableStructure [sVar1] == 'DATE'):
								
								sVal2	= sVal2	[:10]
						
						if (sVal2.lower () != 'null'):
							
							# Recast sVal2 Here
							sVal2	= HDR.castValue (sVal2, odTableStructure [sVar1])
						
						bResult	= oOpFunc (sVal1, sVal2)
						
						# Reset the variable
						iCtr2	= 0
						
						if bResult == True:
							
							iNumRow	+= 1
							
							for sKey, sValue in odTableValue.items ():
								
								if (odTableStructure [sKey] == 'DATETIME') or (odTableStructure [sKey] == 'DATE'):
									
									odTableValue [sKey]	= HDR.convertDT (1, str (odTableValue [sKey]))
									
									if (odTableStructure [sKey] == 'DATE'):
										
										odTableValue [sKey]	= odTableValue [sKey][:10]
								
								lTmp.append (str(odTableValue [sKey]))
			
			else:
				
				while (iCtr1 < len (lTmp2)):
					
					for sKey, sValue in odTableValue.items ():
						
						odTableValue [sKey]	= lTmp2 [iCtr1]
						
						iCtr1	+= 1
						
						if (odTableStructure [sKey] == 'DATETIME') or (odTableStructure [sKey] == 'DATE'):
							
							odTableValue [sKey]	= HDR.convertDT (1, str (odTableValue [sKey]))
							
							if (odTableStructure [sKey] == 'DATE'):
								
								odTableValue [sKey]	= odTableValue [sKey][:10]
					
					for sKey, sValue in odTableValue.items ():
						
						lTmp.append (str (odTableValue [sKey]))
			
			lTmp2	= lTmp
			
			if pdQuery ['COLUMNS'] == '*':
			
				# Group List on Per-Row Basis
				lTmp   = (HDR.grouper (lTmp2, piRow=iNumRow, piColumn=iNumCol))
				
				HDR.goDebug.write (psMsg='[%s] - Files New List [%s]' %(HDR.OS.path.basename(__file__), lTmp))
			
			else:
				
				lHeaders	= pdQuery ['COLUMNS'].split (',')
				
				iNumCol		= len (lHeaders)
				
				iCtr	= 0
				
				lTmp	= []
				
				while (iCtr < len (lTmp2)):
				
					for sKey, sValue in odTableStructure.items ():
						
						odTableStructure [sKey]	= lTmp2 [iCtr]
						
						iCtr	+= 1
					
					for sKey in lHeaders:
						
						if sKey in odTableStructure.keys():
						
							lTmp.append (str (odTableStructure [sKey]))
				
				lTmp2	= lTmp
				
				# Group List on Per-Row Basis
				lTmp   = (HDR.grouper (lTmp2, piRow=iNumRow, piColumn=iNumCol))
				
				HDR.goDebug.write (psMsg='[%s] - Files New List [%s]' %(HDR.OS.path.basename(__file__), lTmp))
			
			print ('\n' + HDR.TABULATE.tabulate (lTmp, headers=lHeaders, tablefmt='psql') + '\n')
			
			return
		
		# End of Special handling for viewing Catalog Tables
		
		if not HDR.OS.path.exists (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLES'])):
			
			HDR.printException ('DB010', 'Table [%s] does not exist' %pdQuery ['TABLES'])
			
			return
		
		self.joBPTTBLProcessCtg = bPlusTree.javaBPTTblSQLProcessing (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLES'], '.' + pdQuery ['TABLES']),
               	  			 		                     HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_columns.tbl'),
                       	                                                     HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_columns.tbl'))
			
		lTmp    = self.joBPTTBLProcessCtg.getTableStructureFromCatalog (pdQuery ['TABLES'])
		
		for sItem in lTmp:
			
			iCtr1   += 1
			iCtr2   += 1
			
			if ((iCtr2 % 8) == 0):  # 8 -> Fixed Num of colums in Catalog Column Table (davisbase_columns.tbl)
				
				odTableValueConstraint [sTmp.decode (encoding='utf-8')] = sItem.split ('|')[0]
				
				iCtr1   = 0
			
			if (iCtr1 == 4):
				
				sTmp    = sItem.split ('|') [0]
				
				# Add key to dictionary
				odTableStructure [sTmp.decode (encoding='utf-8')]       = ''
				odTableValue [sTmp.decode (encoding='utf-8')]		= ''
				odTableValueConstraint [sTmp.decode (encoding='utf-8')]	= ''
				odTableValueNullable [sTmp.decode (encoding='utf-8')]	= ''
				
				lHeaders.append (sTmp.decode (encoding='utf-8'))
			
			elif (iCtr1 == 5):
				
				# Add value corresponding to the key
				odTableStructure [sTmp.decode (encoding='utf-8')]       = sItem.split ('|') [0]
				
				iNumCol += 1
			
			elif (iCtr1 == 7):
				
				odTableValueNullable [sTmp.decode (encoding='utf-8')]   = sItem.split ('|')[0]
		
		# Reset the variable
		lTmp	= []
		
		self.joBPTTBLProcessNormal	= bPlusTree.javaBPTTblSQLProcessing (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLES'], '.' + pdQuery ['TABLES']),
										   HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLES'], '.' + pdQuery ['TABLES'] + '.tbl'),
                                                                      		   HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLES'], '.' + pdQuery ['TABLES'] + '.tbl'))
		
		for sItem in self.joBPTTBLProcessNormal.getTableValuesFromNormal ():
			
			iNumRow	+= 1
			
			lTmp	= sItem.split ('~')
			
			for sItem2 in lTmp:
				
				lTmp2.append (str (sItem2))
		
		# Reset the variable
		iCtr1	= 0
		
		lTmp	= []
		
		if 'WHERE_COND' in pdQuery.keys ():
			
			iNumRow	= 0
			
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
						
						sVal1	= odTableValue [sVar1].lower ()
					
					if (sVar2.lower () != 'null'):
						
						sVal2	= HDR.castValue (sVar2, odTableStructure [sVar1])	# Try to cast it to same data type as Column
					
					else:
						
						sVal2	= sVar2.lower ()
					
					if (odTableStructure [sVar1] == 'DATETIME') or (odTableStructure [sVar1] == 'DATE'):
						
						sVal2	= HDR.convertDT (0, str (sVal2))
						
						if (odTableStructure [sVar1] == 'DATE'):
							
							sVal2	= sVal2	[:10]
					
					if (sVal2.lower () != 'null'):
						
						# Recast sVal2 Here
						sVal2	= HDR.castValue (sVal2, odTableStructure [sVar1])
					
					bResult	= oOpFunc (sVal1, sVal2)
					
					# Reset the variable
					iCtr2	= 0
					
					if bResult == True:
						
						iNumRow	+= 1
						
						for sKey, sValue in odTableValue.items ():
							
							if (odTableStructure [sKey] == 'DATETIME') or (odTableStructure [sKey] == 'DATE'):
								
								odTableValue [sKey]	= HDR.convertDT (1, str (odTableValue [sKey]))
								
								if (odTableStructure [sKey] == 'DATE'):
									
									odTableValue [sKey]	= odTableValue [sKey][:10]
							
							lTmp.append (str(odTableValue [sKey]))
		
		else:
			
			while (iCtr1 < len (lTmp2)):
				
				for sKey, sValue in odTableValue.items ():
					
					odTableValue [sKey]	= lTmp2 [iCtr1]
					
					iCtr1	+= 1
					
					if (odTableStructure [sKey] == 'DATETIME') or (odTableStructure [sKey] == 'DATE'):
						
						odTableValue [sKey]	= HDR.convertDT (1, str (odTableValue [sKey]))
						
						if (odTableStructure [sKey] == 'DATE'):
							
							odTableValue [sKey]	= odTableValue [sKey][:10]
				
				for sKey, sValue in odTableValue.items ():
					
					lTmp.append (str (odTableValue [sKey]))
		
		lTmp2	= lTmp
		
		if pdQuery ['COLUMNS'] == '*':
		
			# Group List on Per-Row Basis
			lTmp   = (HDR.grouper (lTmp2, piRow=iNumRow, piColumn=iNumCol))
			
			HDR.goDebug.write (psMsg='[%s] - Files New List [%s]' %(HDR.OS.path.basename(__file__), lTmp))
		
		else:
			
			lHeaders	= pdQuery ['COLUMNS'].split (',')
			
			iNumCol		= len (lHeaders)
			
			iCtr	= 0
			
			lTmp	= []
			
			while (iCtr < len (lTmp2)):
			
				for sKey, sValue in odTableStructure.items ():
					
					odTableStructure [sKey]	= lTmp2 [iCtr]
					
					iCtr	+= 1
				
				for sKey in lHeaders:
					
					if sKey in odTableStructure.keys():
					
						lTmp.append (str (odTableStructure [sKey]))
			
			lTmp2	= lTmp
			
			# Group List on Per-Row Basis
			lTmp   = (HDR.grouper (lTmp2, piRow=iNumRow, piColumn=iNumCol))
			
			HDR.goDebug.write (psMsg='[%s] - Files New List [%s]' %(HDR.OS.path.basename(__file__), lTmp))
		
		print ('\n' + HDR.TABULATE.tabulate (lTmp, headers=lHeaders, tablefmt='psql') + '\n')
		
                return

def startProcedure (poXML=None):
	
	ROUTINE	= HDR.SYS._getframe().f_code.co_name
	
	oStart	= initQuery ()
	
	return	oStart
	
if __name__ == '__main__':
	
	startProcedure (poXML=None)
