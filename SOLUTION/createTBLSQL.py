#!/usr/bin/env jython

# Custom Libraries goes here
import	customHDR	as HDR

# Jython - Import Java Libraries here
import	bPlusTree.javaBPTTblInsert

class initQuery:
	
	def __init__ (self):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		'''
		Considering the following Syntax for the grammer construction.
		
		CREATE TABLE (TABLE_NAME) (COLUMN_NAME[S] DATA_TYPE CONSTRAINT)
		
		(TABLE_NAME)		-> Formatted as ABC
		(COLUMN_NAME[S])	-> Formatted as COL1 <VALID_DT> <CONSTRAINTS>
		
		'''
		
		if not (HDR.OS.path.exists (HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', 'davisbase_tables.tbl'))):
			
			self.bCtgTblExists	= False
		
		else:
			
			self.bCtgTblExists	= True
		
		if not (HDR.OS.path.exists (HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', 'davisbase_columns.tbl'))):
			
			self.bCtgColExists	= False
		
		else:
			
			self.bCtgColExists	= True
		
		# Create Java Objects here
		self.joCtgTblInsert	= bPlusTree.javaBPTTblInsert (HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', 'davisbase_tables.tbl'),
								      HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_tables.tbl'),
								      HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_tables.tbl'),
								      HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_tables.tbl'),
								      HDR.goDebug, HDR.goBPTPD)
		
		self.joCtgColInsert	= bPlusTree.javaBPTTblInsert (HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', 'davisbase_columns.tbl'),
								      HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_columns.tbl'),
								      HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_columns.tbl'),
								      HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.davisbase_columns.tbl'),
								      HDR.goDebug, HDR.goBPTPD)
		
		self.sStmt	= HDR.PARSER.Forward ()
		self.exprWhere	= HDR.PARSER.Forward ()
		
		self.sKeyword01	= HDR.PARSER.Keyword ('CREATE', caseless=True).setResultsName ('OPERATION')
		self.sKeyword02	= HDR.PARSER.Keyword ('TABLE', caseless=True)
		self.sTerm	= HDR.PARSER.CaselessLiteral (";")
		
		self.tokTable	= HDR.PARSER.delimitedList (HDR.PARSER.Word (HDR.PARSER.alphanums).setName ('TABLE_NAME'), delim='.', combine=True)
		
		self.tokColName	= HDR.PARSER.Word (HDR.PARSER.alphanums).setName ('COLUMN_NAME')
		self.tokColDT	= HDR.PARSER.Word (HDR.PARSER.alphanums).setName ('COLUMN_DATATYPE')
		self.tokColConstraint	= HDR.PARSER.Regex (r"[0-9A-Za-z\ ]+").setName ('COLUMN_CONSTRAINT')
		
		self.tokColName		= self.tokColName.setResultsName ('COLUMN')
		self.tokColDT		= self.tokColDT.setResultsName ('DATA_TYPE')
		self.tokColConstraint	= self.tokColConstraint.setResultsName ('CONSTRAINT')
		
		self.tokColDefines	= HDR.PARSER.Group (self.tokColName + self.tokColDT + HDR.PARSER.Optional (self.tokColConstraint))
		
		self.tokTableList	= HDR.PARSER.Group (HDR.PARSER.delimitedList (self.tokTable))
		self.tokColDefinesList	= HDR.PARSER.Group (HDR.PARSER.delimitedList (self.tokColDefines, delim=',', combine=False))
		
		self.tokTableList	= self.tokTableList.setResultsName ('TABLE')
		self.tokColDefinesList	= self.tokColDefinesList.setResultsName ('COLUMN_DEFINES')
		
		# Finally Define the Grammar for the Statement/Query Here
		
		self.sStmt	<<= (self.sKeyword01 + self.sKeyword02 + self.tokTableList + '(' + self.tokColDefinesList + ')' + self.sTerm)
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def executeQuery (self, psQuery=''):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		tResult		= ()
		dQuery		= {}
		
		iFlag		= False
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		tResult	= self.tokenizeQuery (psQuery)
		
		if tResult:
			
			try:
				
				dQuery	= self.interpretQuery (tResult)
				
				if dQuery:
					
					iFlag	= self.processQuery (dQuery)
					
					if (iFlag == True):
						
						self.checkToSelfDescribeCtgTbl ()
						
						self.checkToSelfDescribeCtgCol ()
						
						self.loadToCtgTbl (dQuery)
						
						self.loadToCtgCol (dQuery)
			
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
			
			HDR.printException ('DB001', 'Error in SQL Syntax.' + str (tResult [1:]))
			
			tResult	= ()
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return tResult
	
	def interpretQuery (self, ptTokens=()):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), str (ptTokens)))
		
		dQuery		= {}
		dTmp		= {}
		
		sTmp		= ''
		
		iTmp		= 0
		
		lTmp		= []
		lTmp2		= []
		
		lValidDT		= ['NULL', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'REAL', 'DOUBLE', 'DATETIME', 'DATE', 'TEXT']
		lValidConstraints	= ['PRIMARY KEY', 'NOT NULL', 'DEFAULT']
		
		try:
			
			if not ptTokens:
				
				raise Exception ('Query Tokens are Empty. Critical!!!. Cannot Interpret')
			
			else:
			
				dTmp	= ptTokens [0][0][1]
				
				for sKey, Value in dTmp.items ():
					
					if sKey == 'OPERATION':
						
						dQuery [sKey]	= str (Value)
					
					elif sKey == 'COLUMN_DEFINES':
						
						# Validate the COLUMN Definitions here
						
						iTmp	= 0;
						
						while (iTmp < len (Value)):
							
							for sSubKey, SubValue in Value [iTmp].items ():
								
								if sSubKey == 'DATA_TYPE':
					 				
									if str (SubValue) not in lValidDT:
										
										HDR.printException ('DB002', 'Unexpected Data Type in the Syntax')
										
										dQuery	= {}
										
										return
									
									else:
										
										HDR.goDebug.write (psMsg='[%s] - [%s] DataType [%s] Valid'
												  %(HDR.OS.path.basename(__file__), ROUTINE, str (SubValue)))
								
								elif sSubKey == 'CONSTRAINT':
									
									if str (SubValue) not in lValidConstraints:
										
										HDR.printException ('DB003', 'Unexpected Constraint in the Syntax')
										
										dQuery	= {}
										
										return
									
									else:
										
										HDR.goDebug.write (psMsg='[%s] - [%s] Constraint [%s] Valid'
												  %(HDR.OS.path.basename(__file__), ROUTINE, str (SubValue)))
							
							iTmp	+= 1
						
						dQuery [sKey]	= Value 
					
					elif sKey == 'TABLE':
						
						dQuery [sKey]	= ','.join (Value)
					
					else:
						
						raise Exception ('Unexpected Keyword in the Syntax. Critical!!!')
		
		except Exception as e:
			
			raise Exception (e)
		
		return dQuery
	
	def processQuery (self, pdQuery={}):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		iFlag	= False
		
		if HDR.OS.path.exists (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE'])):
			
			HDR.printException ('DB005', 'Cannot create %s. Already Exists. Drop it First before recreation.' %(pdQuery ['TABLE']))
			
		else:
			
			try:
				
				HDR.OS.makedirs (HDR.OS.path.join (HDR.OS.getcwd (), pdQuery ['TABLE']))
			
			except Exception as e:
				
				raise Exception ('Cannot Create Table. Critical!!!. Aborting')
			
			else:
				
				print ('Table [%s] Created' %(pdQuery ['TABLE']))
				
				iFlag	= True
		
		return iFlag
	
	def flattenQueryForCtgTbl (self, pdQuery={}): 
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		lFlat	= []
		
		# Get the Last Row ID
		iRowID	= int (HDR.pickle (HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.config'),
					   1, 'LAST_ROW_ID_DB_TABLES', 0))
		
		if iRowID < 0:
			
			raise Exception ('Database Table Corrupted.. Critical. Aborting!!!')
		
		else:
			
			iRowID	+= 1
		
		'''
		davisbase_columns.tbl File has the following constant structure
		IN THE SAME ORDER
		
		ROWID			INT
		SCHEMA_NAME		STRING
		TABLE_NAME		STRING
		'''
		
		# First Column is always internal ROW_ID
		lFlat	= [['1', 'ROWID', 'INT', str (iRowID)]]
		
		# Second Column is always Schema Name
		lFlat.append (['2', 'SCHEMA_NAME', 'TEXT', HDR.gsCurrentSchema]) 
		
		# Third Column is always Table Name
		lFlat.append (['3', 'TABLE_NAME', 'TEXT', pdQuery ['TABLE']])
		
		# Write the last ROW ID to the config file
		HDR.pickle (HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.config'),
			    0, 'LAST_ROW_ID_DB_TABLES', iRowID)
		
		return lFlat
	
	def flattenQueryForCtgCol (self, pdQuery={}): 
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		'''
		This procedure flattens the query into a list of following structure
		[<COLUMN_NAME>, <COLUMN_DATA-TYPE>, <COLUMN_VALUE>, <COLUMN_CONSTRAINTS>, <IS_NULLABLE (Y/N)>]
		
		E.g : ['rowid', 'INT', '1', 'PRIMARY KEY', 'N']
		
		All values in this list are of String type which is type-cased appropriately in java
		'''
		
		lFlat	= []
		
		iTmp	= 1
		
		sNullable	= ''
		sKeyConstraint	= ''
		
		# Get the Last Row ID
		iRowID	= int (HDR.pickle (HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.config'),
					   1, 'LAST_ROW_ID_DB_COLUMNS', 0))
		
		if iRowID < 0:
			
			raise Exception ('Database Table Corrupted.. Critical. Aborting!!!')
		
		else:
			
			iRowID	+= 1
		
		# First Column is always internal ROW_ID
		lFlat	= [['1', 'ROWID', 'INT', str (iRowID)]]
		
		# Second Column is always Schema Name
		lFlat.append (['2', 'SCHEMA_NAME', 'TEXT', HDR.gsCurrentSchema]) 
		
		# Third Column is always Table Name
		lFlat.append (['3', 'TABLE_NAME', 'TEXT', pdQuery ['TABLE']])
		
		# Fourth Column is always Column Name
		lFlat.append (['4', 'COLUMN_NAME', 'TEXT', 'ROWID'])
		
		# Fifth Column is always Data Type
		lFlat.append (['5', 'DATA_TYPE', 'TEXT', 'INT'])
		
		# Sixth Column is always Ordinal Position
		lFlat.append (['6', 'ORDINAL_POSITION', 'TINYINT', str (iTmp)])
		
		# Seventh Column is always IS_NULLABLE
		lFlat.append (['7', 'IS_NULLABLE', 'TEXT', 'N'])
		
		# Eighth Column is always the Constraint
		lFlat.append (['8', 'CONSTRAINT', 'TEXT', 'PK'])
		
		'''
		Now follow the rest other columns as per the query
		davisbase_tbl.tbl File has the following constant structure
		IN THE SAME ORDER
		
		ROWID			INT
		SCHEMA_NAME		TEXT
		TABLE_NAME		TEXT
		COLUMN_NAME		TEXT
		DATA_TYPE		TEXT
		ORDINAL_POSITION	TINYINT
		IS_NULLABLE		TEXT
		CONSTRAINT		TEXT
		'''
		
		while (iTmp <= len (pdQuery ['COLUMN_DEFINES'])):
			
			sNullable	= 'Y'
			sKeyConstraint	= ''
			
			bConstraintAdded	= False
			
			iRowID	+= 1;
			
			# First Column is always internal ROW_ID
			lFlat.append (['1', 'ROWID', 'INT', str (iRowID)])
			
			# Second Column is always Schema Name
			lFlat.append (['2', 'SCHEMA_NAME', 'TEXT', HDR.gsCurrentSchema]) 
			
			# Third Column is always Table Name
			lFlat.append (['3', 'TABLE_NAME', 'TEXT', pdQuery ['TABLE']])
			
			# Sixth Column is always Ordinal position 
			lFlat.append (['6', 'ORDINAL_POSITION', 'TINYINT', str (iTmp + 1)])
			
			for sKey, Value in pdQuery ['COLUMN_DEFINES'][iTmp-1].items ():
				
				if sKey == 'COLUMN':
					
					# Fourth Column is always Column Name
					lFlat.append (['4', 'COLUMN_NAME', 'TEXT', Value])
					
					continue
				
				if sKey == 'DATA_TYPE':
					
					# Fifth Column is always Data Type
					lFlat.append (['5', 'DATA_TYPE', 'TEXT', Value])
					
					continue
				
				if sKey == 'CONSTRAINT': 
					
					if Value == 'NOT NULL':
						
						if not sKeyConstraint:
							
							sNullable	= 'N'
					
					if Value == 'PRIMARY KEY':
						
						sKeyConstraint	= 'PK'
						sNullable	= 'N'
				
					bConstraintAdded	= True
					
					# Seventh Column is always IS_NULLABLE
					lFlat.append (['7', 'IS_NULLABLE', 'TEXT', sNullable])
					
					# Eighth Column is always Constraint 
					lFlat.append (['8', 'CONSTRAINT', 'TEXT', sKeyConstraint])
				
			iTmp	+= 1
			
			if bConstraintAdded == False:
				
				bConstraintAdded	= True
				
				# Seventh Column is always IS_NULLABLE
				lFlat.append (['7', 'IS_NULLABLE', 'TEXT', sNullable])
				
				# Eighth Column is always Constraint 
				lFlat.append (['8', 'CONSTRAINT', 'TEXT', sKeyConstraint])
		
		# Write the last ROW ID to the config file
		HDR.pickle (HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.config'),
			    0, 'LAST_ROW_ID_DB_COLUMNS', iRowID);
		
		return lFlat
	
	def checkToSelfDescribeCtgTbl (self):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		# If its the first run, then insert the details about the davisbase_tables.tbl and davisbase_columns.tbl
		# into the catalog file
		
		if self.bCtgTblExists == False:
			
			iRowID	= 0
			
			lFlat	= []
			lFlat2	= []
			
			iRecordSize	= 0
			
			# Get the Last Row ID
			iRowID	= int (HDR.pickle (HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.config'),
						   1, 'LAST_ROW_ID_DB_TABLES', 0))
		
			if iRowID < 0:
				
				raise Exception ('Database Table Corrupted.. Critical. Aborting!!!')
			
			else:
				
				iRowID	+= 1
			
			'''
			davisbase_columns.tbl File has the following constant structure
			IN THE SAME ORDER
			
			ROWID			INT
			SCHEMA_NAME		STRING
			TABLE_NAME		STRING
			'''
			
			# First Column is always internal ROW_ID
			lFlat	= [['1', 'ROWID', 'INT', str (iRowID)]]
			
			# Second Column is always Schema Name
			lFlat.append (['2', 'SCHEMA_NAME', 'TEXT', 'CATALOG']) 
			
			# Third Column is always Table Name
			lFlat.append (['3', 'TABLE_NAME', 'TEXT', 'DAVISBASE_TABLES'])
			
			# Repeat for davisbase_columns.tbl entry into davisbase_tables.tbl
			iRowID	+= 1
			
			# First Column is always internal ROW_ID
			lFlat.append (['1', 'ROWID', 'INT', str (iRowID)])
			
			# Second Column is always Schema Name
			lFlat.append (['2', 'SCHEMA_NAME', 'TEXT', 'CATALOG']) 
			
			# Third Column is always Table Name
			lFlat.append (['3', 'TABLE_NAME', 'TEXT', 'DAVISBASE_COLUMNS'])
			
			# Write the last ROW ID to the config file
			HDR.pickle (HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.config'),
				    0, 'LAST_ROW_ID_DB_TABLES', iRowID)
			
			for item in lFlat:
				
				# If a record is written close it and write the next record
				if item[1] == 'ROWID':
					
					# Close the record here
					if lFlat2:
						
						# This loads data to davisbase_columns table and then to davisbase_tables table
						self.joCtgTblInsert.insert (lFlat2, len (lFlat2), iRecordSize)
						
						lFlat2	= []
						
						iRecordSize	= 0
				
				item.append (HDR.dDataTypeSerialCode [item [2]])
				
				# For DATA_TYPE as TEXT, calculate the length of the actual text and append to list
				if (HDR.dDataTypeSerialCode [item [2]] == '0x0C'):
					
					item.append (str (len (item [3])))
					
					iRecordSize	= iRecordSize + len (item [3])
				else:
					
					item.append (HDR.dDataTypeLength [item [2]])
					
					iRecordSize	= iRecordSize	+ int (HDR.dDataTypeLength [item [2]])
				
				lFlat2.append (item)
				
			if lFlat2:
					
				self.joCtgTblInsert.insert (lFlat2, len (lFlat2), iRecordSize)
				
				self.joCtgTblInsert.write ()
				
				lFlat2	= []
				
				iRecordSize	= 0
			
		return

	def checkToSelfDescribeCtgCol (self):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		# If its the first run, then insert the details about the davisbase_tables.tbl and davisbase_columns.tbl
		# into the catalog file
		
		if self.bCtgColExists == False:
			
			iTmp	= 1
			
			iRowID	= 0
			
			lFlat	= []
			
			# Get the Last Row ID
			iRowID	= int (HDR.pickle (HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.config'),
						   1, 'LAST_ROW_ID_DB_COLUMNS', 0))
			
			if iRowID < 0:
				
				raise Exception ('Database Table Corrupted.. Critical. Aborting!!!')
			
			else:
				
				iRowID	+= 1
			
			'''
			Now follow the rest other columns as per the query
			davisbase_tbl.tbl File has the following constant structure
			IN THE SAME ORDER
			
			ROWID			INT
			SCHEMA_NAME		TEXT
			TABLE_NAME		TEXT
			COLUMN_NAME		TEXT
			DATA_TYPE		TEXT
			ORDINAL_POSITION	TINYINT
			IS_NULLABLE		TEXT
			CONSTRAINT		TEXT
			'''
			
			# Insert first the catalog files information within itself ------>
			
			# First Column is always internal ROW_ID
			lFlat	= [['1', 'ROWID', 'INT', str (iRowID)]]
			
			# Second Column is always Schema Name
			lFlat.append (['2', 'SCHEMA_NAME', 'TEXT', 'CATALOG']) 
			
			# Third Column is always Table Name
			lFlat.append (['3', 'TABLE_NAME', 'TEXT', 'DAVISBASE_COLUMNS'])
			
			# Fourth Column is always Column Name
			lFlat.append (['4', 'COLUMN_NAME', 'TEXT', 'ROWID'])
			
			# Fifth Column is always Data Type
			lFlat.append (['5', 'DATA_TYPE', 'TEXT', 'INT'])
			
			# Sixth Column is always Ordinal Position
			lFlat.append (['6', 'ORDINAL_POSITION', 'TINYINT', str (iTmp)])
			
			# Seventh Column is always IS_NULLABLE
			lFlat.append (['7', 'IS_NULLABLE', 'TEXT', 'N'])
			
			# Eighth Column is always the Constraint
			lFlat.append (['8', 'CONSTRAINT', 'TEXT', 'PK'])
			
			# Repeat for further fields
			
			iRowID	+= 1
			iTmp	+= 1
			
			# First Column is always internal ROW_ID
			lFlat.append (['1', 'ROWID', 'INT', str (iRowID)])
			
			# Second Column is always Schema Name
			lFlat.append (['2', 'SCHEMA_NAME', 'TEXT', 'CATALOG']) 
			
			# Third Column is always Table Name
			lFlat.append (['3', 'TABLE_NAME', 'TEXT', 'DAVISBASE_COLUMNS'])
			
			# Fourth Column is always Column Name
			lFlat.append (['4', 'COLUMN_NAME', 'TEXT', 'SCHEMA_NAME'])
			
			# Fifth Column is always Data Type
			lFlat.append (['5', 'DATA_TYPE', 'TEXT', 'TEXT'])
			
			# Sixth Column is always Ordinal Position
			lFlat.append (['6', 'ORDINAL_POSITION', 'TINYINT', str (iTmp)])
			
			# Seventh Column is always IS_NULLABLE
			lFlat.append (['7', 'IS_NULLABLE', 'TEXT', 'N'])
			
			# Eighth Column is always the Constraint
			lFlat.append (['8', 'CONSTRAINT', 'TEXT', ''])
			
			iRowID	+= 1
			iTmp	+= 1
			
			# First Column is always internal ROW_ID
			lFlat.append (['1', 'ROWID', 'INT', str (iRowID)])
			
			# Second Column is always Schema Name
			lFlat.append (['2', 'SCHEMA_NAME', 'TEXT', 'CATALOG']) 
			
			# Third Column is always Table Name
			lFlat.append (['3', 'TABLE_NAME', 'TEXT', 'DAVISBASE_COLUMNS'])
			
			# Fourth Column is always Column Name
			lFlat.append (['4', 'COLUMN_NAME', 'TEXT', 'TABLE_NAME'])
			
			# Fifth Column is always Data Type
			lFlat.append (['5', 'DATA_TYPE', 'TEXT', 'TEXT'])
			
			# Sixth Column is always Ordinal Position
			lFlat.append (['6', 'ORDINAL_POSITION', 'TINYINT', str (iTmp)])
			
			# Seventh Column is always IS_NULLABLE
			lFlat.append (['7', 'IS_NULLABLE', 'TEXT', 'N'])
			
			# Eighth Column is always the Constraint
			lFlat.append (['8', 'CONSTRAINT', 'TEXT', ''])
			
			iRowID	+= 1
			iTmp	+= 1
			
			# First Column is always internal ROW_ID
			lFlat.append (['1', 'ROWID', 'INT', str (iRowID)])
			
			# Second Column is always Schema Name
			lFlat.append (['2', 'SCHEMA_NAME', 'TEXT', 'CATALOG']) 
			
			# Third Column is always Table Name
			lFlat.append (['3', 'TABLE_NAME', 'TEXT', 'DAVISBASE_COLUMNS'])
			
			# Fourth Column is always Column Name
			lFlat.append (['4', 'COLUMN_NAME', 'TEXT', 'COLUMN_NAME'])
			
			# Fifth Column is always Data Type
			lFlat.append (['5', 'DATA_TYPE', 'TEXT', 'TEXT'])
			
			# Sixth Column is always Ordinal Position
			lFlat.append (['6', 'ORDINAL_POSITION', 'TINYINT', str (iTmp)])
			
			# Seventh Column is always IS_NULLABLE
			lFlat.append (['7', 'IS_NULLABLE', 'TEXT', 'N'])
			
			# Eighth Column is always the Constraint
			lFlat.append (['8', 'CONSTRAINT', 'TEXT', ''])
			
			iRowID	+= 1
			iTmp	+= 1
			
			# First Column is always internal ROW_ID
			lFlat.append (['1', 'ROWID', 'INT', str (iRowID)])
			
			# Second Column is always Schema Name
			lFlat.append (['2', 'SCHEMA_NAME', 'TEXT', 'CATALOG']) 
			
			# Third Column is always Table Name
			lFlat.append (['3', 'TABLE_NAME', 'TEXT', 'DAVISBASE_COLUMNS'])
			
			# Fourth Column is always Column Name
			lFlat.append (['4', 'COLUMN_NAME', 'TEXT', 'DATA_TYPE'])
			
			# Fifth Column is always Data Type
			lFlat.append (['5', 'DATA_TYPE', 'TEXT', 'TEXT'])
			
			# Sixth Column is always Ordinal Position
			lFlat.append (['6', 'ORDINAL_POSITION', 'TINYINT', str (iTmp)])
			
			# Seventh Column is always IS_NULLABLE
			lFlat.append (['7', 'IS_NULLABLE', 'TEXT', 'N'])
			
			# Eighth Column is always the Constraint
			lFlat.append (['8', 'CONSTRAINT', 'TEXT', ''])
			
			iRowID	+= 1
			iTmp	+= 1
			
			# First Column is always internal ROW_ID
			lFlat.append (['1', 'ROWID', 'INT', str (iRowID)])
			
			# Second Column is always Schema Name
			lFlat.append (['2', 'SCHEMA_NAME', 'TEXT', 'CATALOG']) 
			
			# Third Column is always Table Name
			lFlat.append (['3', 'TABLE_NAME', 'TEXT', 'DAVISBASE_COLUMNS'])
			
			# Fourth Column is always Column Name
			lFlat.append (['4', 'COLUMN_NAME', 'TEXT', 'ORDINAL_POSITION'])
			
			# Fifth Column is always Data Type
			lFlat.append (['5', 'DATA_TYPE', 'TEXT', 'TINYINT'])
			
			# Sixth Column is always Ordinal Position
			lFlat.append (['6', 'ORDINAL_POSITION', 'TINYINT', str (iTmp)])
			
			# Seventh Column is always IS_NULLABLE
			lFlat.append (['7', 'IS_NULLABLE', 'TEXT', 'N'])
			
			# Eighth Column is always the Constraint
			lFlat.append (['8', 'CONSTRAINT', 'TEXT', ''])
			
			iRowID	+= 1
			iTmp	+= 1
			
			# First Column is always internal ROW_ID
			lFlat.append (['1', 'ROWID', 'INT', str (iRowID)])
			
			# Second Column is always Schema Name
			lFlat.append (['2', 'SCHEMA_NAME', 'TEXT', 'CATALOG']) 
			
			# Third Column is always Table Name
			lFlat.append (['3', 'TABLE_NAME', 'TEXT', 'DAVISBASE_COLUMNS'])
			
			# Fourth Column is always Column Name
			lFlat.append (['4', 'COLUMN_NAME', 'TEXT', 'IS_NULLABLE'])
			
			# Fifth Column is always Data Type
			lFlat.append (['5', 'DATA_TYPE', 'TEXT', 'TEXT'])
			
			# Sixth Column is always Ordinal Position
			lFlat.append (['6', 'ORDINAL_POSITION', 'TINYINT', str (iTmp)])
			
			# Seventh Column is always IS_NULLABLE
			lFlat.append (['7', 'IS_NULLABLE', 'TEXT', 'N'])
			
			# Eighth Column is always the Constraint
			lFlat.append (['8', 'CONSTRAINT', 'TEXT', ''])
			
			iRowID	+= 1
			iTmp	+= 1
			
			# First Column is always internal ROW_ID
			lFlat.append (['1', 'ROWID', 'INT', str (iRowID)])
			
			# Second Column is always Schema Name
			lFlat.append (['2', 'SCHEMA_NAME', 'TEXT', 'CATALOG']) 
			
			# Third Column is always Table Name
			lFlat.append (['3', 'TABLE_NAME', 'TEXT', 'DAVISBASE_COLUMNS'])
			
			# Fourth Column is always Column Name
			lFlat.append (['4', 'COLUMN_NAME', 'TEXT', 'CONSTRAINT'])
			
			# Fifth Column is always Data Type
			lFlat.append (['5', 'DATA_TYPE', 'TEXT', 'TEXT'])
			
			# Sixth Column is always Ordinal Position
			lFlat.append (['6', 'ORDINAL_POSITION', 'TINYINT', str (iTmp)])
			
			# Seventh Column is always IS_NULLABLE
			lFlat.append (['7', 'IS_NULLABLE', 'TEXT', 'Y'])
			
			# Eighth Column is always the Constraint
			lFlat.append (['8', 'CONSTRAINT', 'TEXT', ''])
			
			# Handle for DAVISBASE_TABLES here
			
			iRowID	+= 1
			iTmp	= 1	# Reset the variable
			
			# First Column is always internal ROW_ID
			lFlat.append (['1', 'ROWID', 'INT', str (iRowID)])
			
			# Second Column is always Schema Name
			lFlat.append (['2', 'SCHEMA_NAME', 'TEXT', 'CATALOG']) 
			
			# Third Column is always Table Name
			lFlat.append (['3', 'TABLE_NAME', 'TEXT', 'DAVISBASE_TABLES'])
			
			# Fourth Column is always Column Name
			lFlat.append (['4', 'COLUMN_NAME', 'TEXT', 'ROWID'])
			
			# Fifth Column is always Data Type
			lFlat.append (['5', 'DATA_TYPE', 'TEXT', 'INT'])
			
			# Sixth Column is always Ordinal Position
			lFlat.append (['6', 'ORDINAL_POSITION', 'TINYINT', str (iTmp)])
			
			# Seventh Column is always IS_NULLABLE
			lFlat.append (['7', 'IS_NULLABLE', 'TEXT', 'N'])
			
			# Eighth Column is always the Constraint
			lFlat.append (['8', 'CONSTRAINT', 'TEXT', 'PK'])
			
			
			iRowID	+= 1
			iTmp	+= 1
			
			# First Column is always internal ROW_ID
			lFlat.append (['1', 'ROWID', 'INT', str (iRowID)])
			
			# Second Column is always Schema Name
			lFlat.append (['2', 'SCHEMA_NAME', 'TEXT', 'CATALOG']) 
			
			# Third Column is always Table Name
			lFlat.append (['3', 'TABLE_NAME', 'TEXT', 'DAVISBASE_TABLES'])
			
			# Fourth Column is always Column Name
			lFlat.append (['4', 'COLUMN_NAME', 'TEXT', 'SCHEMA_NAME'])
			
			# Fifth Column is always Data Type
			lFlat.append (['5', 'DATA_TYPE', 'TEXT', 'TEXT'])
			
			# Sixth Column is always Ordinal Position
			lFlat.append (['6', 'ORDINAL_POSITION', 'TINYINT', str (iTmp)])
			
			# Seventh Column is always IS_NULLABLE
			lFlat.append (['7', 'IS_NULLABLE', 'TEXT', 'N'])
			
			# Eighth Column is always the Constraint
			lFlat.append (['8', 'CONSTRAINT', 'TEXT', ''])
			
			
			iRowID	+= 1
			iTmp	+= 1
			
			# First Column is always internal ROW_ID
			lFlat.append (['1', 'ROWID', 'INT', str (iRowID)])
			
			# Second Column is always Schema Name
			lFlat.append (['2', 'SCHEMA_NAME', 'TEXT', 'CATALOG']) 
			
			# Third Column is always Table Name
			lFlat.append (['3', 'TABLE_NAME', 'TEXT', 'DAVISBASE_TABLES'])
			
			# Fourth Column is always Column Name
			lFlat.append (['4', 'COLUMN_NAME', 'TEXT', 'TABLE_NAME'])
			
			# Fifth Column is always Data Type
			lFlat.append (['5', 'DATA_TYPE', 'TEXT', 'TEXT'])
			
			# Sixth Column is always Ordinal Position
			lFlat.append (['6', 'ORDINAL_POSITION', 'TINYINT', str (iTmp)])
			
			# Seventh Column is always IS_NULLABLE
			lFlat.append (['7', 'IS_NULLABLE', 'TEXT', 'N'])
			
			# Eighth Column is always the Constraint
			lFlat.append (['8', 'CONSTRAINT', 'TEXT', ''])
			
			# End of handling for Catalog Tables ----->
			
			# Write the last ROW ID to the config file
			HDR.pickle (HDR.OS.path.join (HDR.gsInstallPath, 'data', 'catalog', '.config'),
				    0, 'LAST_ROW_ID_DB_COLUMNS', iRowID);
			
			iRecordSize	= 0
			
			lFlat2		= []
			
			for item in lFlat:
				
				# If a record is written close it and write the next record
				if item[1] == 'ROWID':
					
					# Close the record here
					if lFlat2:
						
						# This loads data to davisbase_columns table and then to davisbase_tables table
						self.joCtgColInsert.insert (lFlat2, len (lFlat2), iRecordSize)
						
						lFlat2	= []
						
						iRecordSize	= 0
				
				item.append (HDR.dDataTypeSerialCode [item [2]])
				
				# For DATA_TYPE as TEXT, calculate the length of the actual text and append to list
				if (HDR.dDataTypeSerialCode [item [2]] == '0x0C'):
					
					item.append (str (len (item [3])))
					
					iRecordSize	= iRecordSize + len (item [3])
				else:
					
					item.append (HDR.dDataTypeLength [item [2]])
					
					iRecordSize	= iRecordSize	+ int (HDR.dDataTypeLength [item [2]])
				
				lFlat2.append (item)
				
			if lFlat2:
				
				self.joCtgColInsert.insert (lFlat2, len (lFlat2), iRecordSize)
				
				self.joCtgColInsert.write ()
				
				lFlat2	= []
				
				iRecordSize	= 0
		
		return
	
	def loadToCtgTbl (self, pdQuery={}):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		lFlat	= []
		lFlat2	= []
		
		try:
			
			lFlat	= self.flattenQueryForCtgTbl (pdQuery)
		
		except Exception as e:
			
			raise Exception (e)
		
		iRecordSize	= 0
		
		for item in lFlat:
			
			# If a record is written close it and write the next record
			if item[1] == 'ROWID':
				
				# Close the record here
				if lFlat2:
					
					# This loads data to davisbase_columns table and then to davisbase_tables table
					self.joCtgTblInsert.insert (lFlat2, len (lFlat2), iRecordSize)
					
					lFlat2	= []
					
					iRecordSize	= 0
			
			item.append (HDR.dDataTypeSerialCode [item [2]])
			
			# For DATA_TYPE as TEXT, calculate the length of the actual text and append to list
			if (HDR.dDataTypeSerialCode [item [2]] == '0x0C'):
				
				item.append (str (len (item [3])))
				
				iRecordSize	= iRecordSize + len (item [3])
			else:
				
				item.append (HDR.dDataTypeLength [item [2]])
				
				iRecordSize	= iRecordSize	+ int (HDR.dDataTypeLength [item [2]])
			
			lFlat2.append (item)
			
		if lFlat2:
			
			self.joCtgTblInsert.insert (lFlat2, len (lFlat2), iRecordSize)
			
			self.joCtgTblInsert.write ()
			
			lFlat2	= []
			
			iRecordSize	= 0
		
		return
	
	def loadToCtgCol (self, pdQuery={}):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		lFlat	= []
		lFlat2	= []
		
		try:
			
			lFlat	= self.flattenQueryForCtgCol (pdQuery)
		
		except Exception as e:
			
			raise Exception (e)
		
		iRecordSize	= 0
		
		for item in lFlat:
			
			# If a record is written close it and write the next record
			if item[1] == 'ROWID':
				
				# Close the record here
				if lFlat2:
					
					# This loads data to davisbase_columns table and then to davisbase_tables table
					self.joCtgColInsert.insert (lFlat2, len (lFlat2), iRecordSize)
					
					lFlat2	= []
					
					iRecordSize	= 0
			
			item.append (HDR.dDataTypeSerialCode [item [2]])
			
			# For DATA_TYPE as TEXT, calculate the length of the actual text and append to list
			if (HDR.dDataTypeSerialCode [item [2]] == '0x0C'):
				
				item.append (str (len (item [3])))
				
				iRecordSize	= iRecordSize + len (item [3])
			else:
				
				item.append (HDR.dDataTypeLength [item [2]])
				
				iRecordSize	= iRecordSize	+ int (HDR.dDataTypeLength [item [2]])
			
			lFlat2.append (item)
			
		if lFlat2:
			
			self.joCtgColInsert.insert (lFlat2, len (lFlat2), iRecordSize)
			
			self.joCtgColInsert.write ()
			
			lFlat2	= []
			
			iRecordSize	= 0
		
		return
	
	def printResult (self):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		# Prints the BPlus Tree	- DavisBase Tables
		self.joCtgTblInsert.printTree ()
		
		# Prints the BPlus Tree - DavisBase Columns
		self.joCtgColInsert.printTree ()
		
		return

def startProcedure (poXML=None):
	
	ROUTINE	= HDR.SYS._getframe().f_code.co_name
	
	oStart	= initQuery ()
	
	return	oStart
	
if __name__ == '__main__':
	
	startProcedure (poXML=None)
