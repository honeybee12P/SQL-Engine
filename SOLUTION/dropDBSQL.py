#!/usr/bin/env jython

# Custom Libraries goes here
import	customHDR	as HDR

class initQuery:
	
	def __init__ (self):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		'''
		Considering the following Syntax for the grammer construction.
		
		CREATE DATABASE (DB_NAME[S])
		
		DB_NAME[S]		-> Database names to delete
		'''
		
		self.sStmt	= HDR.PARSER.Forward ()
		self.exprWhere	= HDR.PARSER.Forward ()
		
		self.sKeyword01	= HDR.PARSER.Keyword ('DROP DATABASE', caseless=True).setResultsName ('OPERATION')
		self.sTerm	= HDR.PARSER.CaselessLiteral (";")
		
		self.tokDBs	= HDR.PARSER.delimitedList (HDR.PARSER.Word (HDR.PARSER.alphanums).setName ('DB_NAMES'), delim=' ', combine=True)
		
		self.tokDBsList	= HDR.PARSER.Group (HDR.PARSER.delimitedList (self.tokDBs))
		self.tokDBsList	= self.tokDBsList.setResultsName ('DATABASE')
		
		# Finally Define the Grammar for the Statement/Query Here
		
		self.sStmt	<<= (self.sKeyword01 + self.tokDBsList + self.sTerm)
		
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
		
		# To delete the following commented lines
		
		try:
			
			if not ptTokens:
				
				raise Exception ('Query Tokens are Empty. Critical!!!. Cannot Interpret')
			
			else:
			
				dTmp	= ptTokens [0][0][1]
				
				for sKey, Value in dTmp.items ():
					
					if sKey == 'OPERATION':
						
						dQuery [sKey]	= str (Value)
					
					elif sKey == 'DATABASE':
						
						dQuery [sKey]	= Value
						
						HDR.goDebug.write (psMsg='[%s] - [%s] Database Names [%s]' %(HDR.OS.path.basename(__file__),
								   ROUTINE, str (dQuery [sKey])))
					
					else:
						
						raise Exception ('Unexpected Keyword in the Syntax. Critical!!!')
		
		except Exception as e:
			
			raise Exception (e)
		
		return dQuery
	
	def processQuery (self, dQuery={}):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), str (dQuery)))
		
		for sKey, Value in dQuery.items ():
			
			if sKey == 'DATABASE':
				
				iLen	= len (dQuery [sKey])
				
				HDR.goDebug.write (psMsg='[%s] - [%s] DB Names List Length [%d]' %(HDR.OS.path.basename(__file__), ROUTINE, iLen))
				
				#lTmp	= Value.split (',')
				lTmp	= Value
				
				try:
					
					self.dropDatabase (lTmp)
				
				except Exception as e:
					
					raise Exception (e)
		
		return
	
	def dropDatabase (self, plDBNames=[]):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		HDR.goDebug.write (psMsg='[%s] - Syntax [%s]' %(HDR.OS.path.basename(__file__), str (plDBNames)))
		
		iTmp	= 0
		
		while (iTmp < len (plDBNames)):
			
			try:
				
				if not HDR.OS.path.isdir (HDR.OS.path.join (HDR.gsInstallPath, 'data', plDBNames [iTmp])):
				
					print ('Cannot drop %s. Probably No such Database exists.' %(plDBNames [iTmp]))
				
				else:
					
					try:
						
						HDR.SHUTIL.rmtree (HDR.OS.path.join (HDR.gsInstallPath, 'data', plDBNames [iTmp]), ignore_errors=True)
					
					except Exception as e:
						
						raise Exception ('Cannot Drop Database. Critical!!!. Aborting')
			
			except Exception as e:
				
				raise Exception (e)		
			
			iTmp	+= 1;
		
		return

def startProcedure (poXML=None):
	
	ROUTINE	= HDR.SYS._getframe().f_code.co_name
	
	oStart	= initQuery ()
	
	return	oStart
	
if __name__ == '__main__':
	
	startProcedure (poXML=None)
