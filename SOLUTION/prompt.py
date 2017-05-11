#!/usr/bin/env jython

# Custom Libraries goes here
import	customHDR	as HDR

import	selectSQL	as SEL_SQL
import	insertSQL	as INS_SQL
import	useDBSQL	as USEDB_SQL
import	createDBSQL	as CRTDB_SQL
import	createTBLSQL	as CRTTBL_SQL
import	dropTBLSQL	as DRPTBL_SQL
import	dropDBSQL	as DRPDB_SQL
import	deleteTBLSQL	as DELTBL_SQL
import	updateTBLSQL	as UPDTBL_SQL

import	showDBSQL	as SHOWDB_SQL
import	showTBLSQL	as SHOWTBL_SQL

class initPrompt (HDR.CMD.Cmd, object):
	
	def __init__ (self):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		# USE DB SQL Syntax Object
		self.USEDB_SQL	= None
		
		# SELECT SQL Syntax Object
		self.SEL_SQL	= None
		
		# INSERT SQL Syntax Object
		self.INS_SQL	= None
		
		# CREATE SQL Syntax Object
		self.CRTTBL_SQL	= None
		
		# CREATE DATABASE SQL Syntax Object
		self.CRTDB_SQL	= None
		
		# DROP DATABASE SQL Syntax Object
		self.DRPDB_SQL	= None
		
		# DROP TABLE SQL Syntax Object
		self.DRPTBL_SQL	= None
		
		# SHOW DATABASES Syntax Object
		self.SHOWDB_SQL	= None
		
		# SHOW TABLES Syntax Object
		self.SHOWTBL_SQL= None
		
		# DELETE FROM TABLE Syntax Object
		self.DELTBL_SQL	= None
		
		# UPDATE TABLE Syntax Object
		self.UPDTBL_SQL	= None
		
		# Flags for different Operations
		self.bUSEFlag	= False		# Flag to indicate whether USE command has been issued atleast once
		
		# Call the constructor of the parent class
		super (initPrompt, self).__init__()
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def precmd (self, psQuery):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		# Don't Upper-case in-built help command syntax
		if not psQuery.find ('help', 0, 4):
			
			HDR.goDebug.write (psMsg='[%s] - [%s] Help Syntax [%s]' %(HDR.OS.path.basename(__file__), ROUTINE, psQuery))
			
			if len (psQuery) > len ('help'):
				
				# Upper-case the command from help syntax without changing the case of `help` command
				psQuery	= psQuery[0:5] + psQuery[4:].upper ().rstrip (';')
		
		else:
			
			# Upper-case other commands
			psQuery	= psQuery.upper ()
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return psQuery
	
	#
	# SELECT Query Methods - START
	# <----------------------------------------------------------------------------------------------------------------------------------------------------------------
	
	def do_SELECT (self, psQuery):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		if self.bUSEFlag == True:
			
			self.SEL_SQL	= SEL_SQL.startProcedure ()
			
			self.SEL_SQL.executeQuery ('SELECT ' + psQuery)
		
		else:
			
			print ('Create and/or Select Database First to use.')
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def complete_SELECT (self, psQuery, psLine, piBeginIdx, piEndIdx):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		sCompletion	= ['SELECT [COLUMN_LIST|*] FROM [COMMA DELIMITED TABLE_NAME_LIST];']
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return	sCompletion
	
	def help_SELECT (self):
	
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		print ('\n'.join (['<!--',
				  'Selects Tuples from Relations.',
				  '-->']))
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	#
	# SELECT Query Methods - END
	# ----------------------------------------------------------------------------------------------------------------------------------------------------------------->
	
	#
	# USE DB Query Methods - START
	# <----------------------------------------------------------------------------------------------------------------------------------------------------------------
	
	def do_USE (self, psQuery):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		self.bUSEFlag	= True
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		self.USEDB_SQL	= USEDB_SQL.startProcedure ()
		
		self.USEDB_SQL.executeQuery ('USE ' + psQuery)
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def complete_USE (self, psQuery, psLine, piBeginIdx, piEndIdx):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		sCompletion	= ['USE [SCHEMA_NAME];']
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return	sCompletion
	
	def help_USE (self):
	
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		print ('\n'.join (['<!--',
				  'Uses an available Schema/Database.',
				  '-->']))
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	#
	# USE DB Query Methods - END
	# ----------------------------------------------------------------------------------------------------------------------------------------------------------------->
	
	#
	# CREATE TABLE and CREATE DATABASE Query Methods - START
	# <----------------------------------------------------------------------------------------------------------------------------------------------------------------
	
	def do_CREATE (self, psQuery):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		HDR.goDebug.write (psMsg='[%s] - psQuery [:8] [%s]' %(HDR.OS.path.basename(__file__), psQuery [:8]))
		
		if psQuery [:8] == 'DATABASE':
			
			self.proc_CREATEDB (psQuery)
		
		elif psQuery [:5] == 'TABLE':
			
			if self.bUSEFlag == True:
				
				self.CRTTBL_SQL	= CRTTBL_SQL.startProcedure ()
				
				self.CRTTBL_SQL.executeQuery ('CREATE ' + psQuery)
			
			else:
				
				print ('Create and/or Select Database First to use.')
		
		else:
			
			HDR.printException ('DB001', 'Error in SQL Syntax.')
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def help_CREATE (self):
	
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		print ('\n'.join (['<!--',
				  'Creats a New Table.',
				  '-->']))
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def proc_CREATEDB (self, psQuery):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		self.CRTDB_SQL	= CRTDB_SQL.startProcedure ()
		
		self.CRTDB_SQL.executeQuery ('CREATE ' + psQuery)
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def help_CREATEDB (self):
	
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		print ('\n'.join (['<!--',
				  'Creats New Database.',
				  '-->']))
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	#
	# CREATE TABLE and CREATE DATABASE Query Methods - END
	# ----------------------------------------------------------------------------------------------------------------------------------------------------------------->
	
	#
	# INSERT DB Query Methods - START
	# <----------------------------------------------------------------------------------------------------------------------------------------------------------------
	
	def do_INSERT (self, psQuery):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		if self.bUSEFlag == True:
			
			self.INS_SQL	= INS_SQL.startProcedure ()
			
			self.INS_SQL.executeQuery ('INSERT ' + psQuery)
		
		else:
			
			print ('Create and/or Select Database First to use.')
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def complete_INSERT (self, psQuery, psLine, piBeginIdx, piEndIdx):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		sCompletion	= ['INSERT INTO [TABLE_NAME] VALUES (v1,v2..);']
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return	sCompletion
	
	def help_INSERT(self):
	
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		print ('\n'.join (['<!--',
				  'Insert Data into Table.',
				  '-->']))
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	#
	# INSERT DB Query Methods - END
	# ----------------------------------------------------------------------------------------------------------------------------------------------------------------->
	
	#
	# UPDATE TABLE Query Methods - START
	# <----------------------------------------------------------------------------------------------------------------------------------------------------------------
	
	def do_UPDATE (self, psQuery):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		if self.bUSEFlag == True:
			
			self.UPDTBL_SQL	= UPDTBL_SQL.startProcedure ()
			
			self.UPDTBL_SQL.executeQuery ('UPDATE ' + psQuery)
		
		else:
			
			print ('Create and/or Select Database First to use.')
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def complete_UPDATE (self, psQuery, psLine, piBeginIdx, piEndIdx):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		sCompletion	= ['UPDATE [TABLE_NAME] SET COLUMN=VALUE [WHERE_CONDITION]']
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return	sCompletion
	
	def help_UPDATE (self):
	
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		print ('\n'.join (['<!--',
				  'Update Data In The Table.',
				  '-->']))
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	#
	# UPDATE TABLE Query Methods - END
	# ----------------------------------------------------------------------------------------------------------------------------------------------------------------->
	
	#
	# DELETE FROM TABLE Query Methods - START
	# <----------------------------------------------------------------------------------------------------------------------------------------------------------------
	
	def do_DELETE (self, psQuery):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		if self.bUSEFlag == True:
			
			self.DELTBL_SQL	= DELTBL_SQL.startProcedure ()
			
			self.DELTBL_SQL.executeQuery ('DELETE ' + psQuery)
		
		else:
			
			print ('Create and/or Select Database First to use.')
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def complete_DELETE (self, psQuery, psLine, piBeginIdx, piEndIdx):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		sCompletion	= ['DELETE FROM [TABLE_NAME] WHERE CONDITION;']
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return	sCompletion
	
	def help_DELETE (self):
	
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		print ('\n'.join (['<!--',
				  'Delete Data From Table.',
				  '-->']))
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	#
	# DELETE FROM TABLE Query Methods - END
	# ----------------------------------------------------------------------------------------------------------------------------------------------------------------->
	
	#
	# DROP TABLE and DROP DATABASE Query Methods - START
	# <----------------------------------------------------------------------------------------------------------------------------------------------------------------
	
	def do_DROP (self, psQuery):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		HDR.goDebug.write (psMsg='[%s] - psQuery [:8] [%s]' %(HDR.OS.path.basename(__file__), psQuery [:8]))
		
		if psQuery [:8] == 'DATABASE':
			
			self.proc_DROPDB (psQuery)
		else:
			
			if self.bUSEFlag == True:
				
				self.DRPTBL_SQL	= DRPTBL_SQL.startProcedure ()
				
				self.DRPTBL_SQL.executeQuery ('DROP ' + psQuery)
			
			else:
				
				print ('Create and/or Select Database First to use.')
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def help_DROP (self):
	
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		print ('\n'.join (['<!--',
				  'Drop Existing Table.',
				  '-->']))
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def proc_DROPDB (self, psQuery):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		self.DRPDB_SQL	= DRPDB_SQL.startProcedure ()
		
		self.DRPDB_SQL.executeQuery ('DROP ' + psQuery)
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def help_DROPDB (self):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		print ('\n'.join (['<!--',
				  'Drop Existing Database.',
				  '-->']))
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	#
	# DROP TABLE and DROP DATABASE Query Methods - END
	# ----------------------------------------------------------------------------------------------------------------------------------------------------------------->
	
	#
	# SHOW TABLE and SHOW DATABASES Query Methods - START
	# <----------------------------------------------------------------------------------------------------------------------------------------------------------------
	
	def do_SHOW (self, psQuery):
	
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		HDR.goDebug.write (psMsg='[%s] - psQuery [:8] [%s]' %(HDR.OS.path.basename(__file__), psQuery [:9]))
		
		if psQuery [:8] == 'DATABASE':
			
			self.proc_SHOWDB (psQuery)
		
		elif psQuery[:5] == 'TABLE':
			
			if self.bUSEFlag == True:
			
				self.SHOWTBL_SQL	= SHOWTBL_SQL.startProcedure ()
				
				self.SHOWTBL_SQL.executeQuery ('SHOW ' + psQuery)
			
			else:
				
				print ('Create and/or Select Database First to use.')
		
		else:
			
			HDR.printException ('DB001', 'Error in SQL Syntax.')
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def help_SHOW (self):
	
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		print ('\n'.join (['<!--',
				  'Show Existing Table[s] List.',
				  '-->']))
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def proc_SHOWDB (self, psQuery):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), psQuery))
		
		self.SHOWDB_SQL	= SHOWDB_SQL.startProcedure ()
		
		self.SHOWDB_SQL.executeQuery ('SHOW ' + psQuery)
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def help_SHOWDB (self):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		print ('\n'.join (['<!--',
				  'Show Existing Database[s] List.',
				  '-->']))
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	#
	# SHOW TABLE and SHOW DATABASES Query Methods - END
	# ----------------------------------------------------------------------------------------------------------------------------------------------------------------->
	
	#
	# QUIT and EXIT Query Methods - START
	# <----------------------------------------------------------------------------------------------------------------------------------------------------------------
	
	def do_EXIT (self, psQuery):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		raise (SystemExit)
	
	def help_EXIT (self):
	
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		print ('\n'.join (['<!--',
				  'Quits the monitor.',
				  '-->']))
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def do_QUIT (self, psQuery):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		raise (SystemExit)
	
	def help_QUIT (self):
	
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		print ('\n'.join (['<!--',
				  'Quits the monitor.',
				  '-->']))
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	#
	# QUIT and EXIT Query Methods - END
	# ----------------------------------------------------------------------------------------------------------------------------------------------------------------->

def startProcedure (poXML=None): 
	
	ROUTINE	= HDR.SYS._getframe().f_code.co_name
	
	oStart	= initPrompt ()
	
	try:
		if poXML:
			
			try:
				
				oStart.prompt	= poXML ['PROPERTY']['PROMPT']
				
				if oStart.prompt:
					
					if (oStart.prompt [-1:]) != '>':
						
						oStart.prompt	= poXML ['PROPERTY']['PROMPT'] + '>'
				
				else:
					
					oStart.prompt	= 'davisql>'
			
			except KeyError as e:
				
				oStart.prompt	= 'davisql>'
				
				HDR.goDebug.write (psMsg='[%s] - [%s]: PROMPT Key absent. Defaults to [%s]' %(HDR.OS.path.basename(__file__), ROUTINE, oStart.prompt))
			
			else:
				
				HDR.goDebug.write (psMsg='[%s] - [%s]: Prompt Value [%s]' %(HDR.OS.path.basename(__file__), ROUTINE, oStart.prompt))
		
		else:
			
			raise Exception ('User Config XML File Object not accessible')
	
	except Exception as e:
		
		HDR.goDebug.write (psMsg='[%s] - [%s]: Exception Raised on XML Parsing [%s]' %(HDR.OS.path.basename(__file__), ROUTINE, str (e)))
		
		raise Exception (e)
	
	oStart.cmdloop ("\n* Starting prompt... Welcome to the DavisBASE monitor. \n")

if __name__ == '__main__':
	
	startProcedure ()
