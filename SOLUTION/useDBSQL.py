#!/usr/bin/env jython

# Custom Libraries goes here
import	customHDR	as HDR

class initQuery:
	
	def __init__ (self):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		'''
		Considering the following Syntax for the grammer construction.
		
		USE (SCHEMA_NAME/DATABASE_NAME)
		
		(SCHEMA_NAME)		-> Database/Schema to select and use
		
		'''
		
		self.sStmt	= HDR.PARSER.Forward ()
		
		self.sKeyword01	= HDR.PARSER.Keyword ('USE', caseless=True).setResultsName ('OPERATION')
		self.sTerm	= HDR.PARSER.CaselessLiteral (";")
		
		self.tokSchema	= HDR.PARSER.delimitedList (HDR.PARSER.Word (HDR.PARSER.alphanums).setName ('SCHEMA/DATABASE NAME'), delim=' ', combine=True)
		
		self.tokSchemaList	= HDR.PARSER.Group (HDR.PARSER.delimitedList (self.tokSchema))
		self.tokSchemaList	= self.tokSchemaList.setResultsName ('SCHEMA')
		
		# Finally Define the Grammar for the Statement/Query Here
		
		self.sStmt	<<= (self.sKeyword01 + self.tokSchemaList + self.sTerm)
		
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
			
                        tResult = tResult [1:]
			
                        HDR.goDebug.write (psMsg='[%s] - [%s] Tokenized Statement [%s]' %(HDR.OS.path.basename(__file__), ROUTINE, str (tResult)))
		
                else:
			
                        print (str (tResult [1:]))
			
                        tResult = ()
		
		HDR.goDebug.write (psMsg='[%s] - [%s] Tokenized Statement [%s]' %(HDR.OS.path.basename(__file__), ROUTINE, str (tResult)))
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return tResult
	
	def interpretQuery (self, ptTokens=()):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), str (ptTokens)))
		
		dQuery	= {}
		dTmp	= {}
		
		try:
			
			if not ptTokens:
				
				raise Exception ('Query Tokens are Empty. Critical!!!. Cannot Interpret')
			
			else:
				
				dTmp	= ptTokens [0][0][1]
				
				for sKey, Value in dTmp.items ():
					
					if sKey == 'OPERATION':
						
						dQuery [sKey]	= str (Value)
					
					elif sKey == 'SCHEMA':
						
						dQuery [sKey]	= Value
						
						HDR.goDebug.write (psMsg='[%s] - [%s] Schema Name [%s]' %(HDR.OS.path.basename(__file__),
								   ROUTINE, str (dQuery [sKey])))
					
					else:
						
						raise Exception ('Unexpected Keyword in the Syntax. Critical!!!')
		
		except Exception as e:
			
			raise Exception (e)
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return dQuery
	
	def processQuery (self, dQuery={}):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), str (dQuery)))
		
		for sKey, Value in dQuery.items ():
			
			if sKey == 'SCHEMA':
				
				lTmp	= Value
				
				try:
					
					self.useSchema (lTmp)
				
				except Exception as e:
					
					raise Exception (e)
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
	
	def useSchema (self, plSchemaName=[]):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), str (plSchemaName)))
		
		iTmp	= 0
		
		while (iTmp < len (plSchemaName)):
			
			try:
				
				if not HDR.OS.path.isdir (HDR.OS.path.join (HDR.gsInstallPath, 'data', plSchemaName [iTmp])):
				
					print ('Cannot Use Schema %s. Probably No such Schema exists.' %(plSchemaName [iTmp]))
				
				else:
					
					try:
						
						HDR.OS.chdir (HDR.OS.path.join (HDR.gsInstallPath, 'data', plSchemaName [iTmp]))
					
					except Exception as e:
						
						raise Exception ('Cannot Use Schema. Critical!!!. Aborting')
					
					else:
						
						print ('Using Database/Schema [%s]' %(plSchemaName [iTmp]))
						
						HDR.gsCurrentSchema	= plSchemaName [iTmp]
			
			except Exception as e:
				
				raise Exception (e)
			
			iTmp	+= 1;
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return
		
def startProcedure (poXML=None):
	
	ROUTINE	= HDR.SYS._getframe().f_code.co_name
	
	oStart	= initQuery ()
	
	return	oStart
	
if __name__ == '__main__':
	
	startProcedure (poXML=None)
