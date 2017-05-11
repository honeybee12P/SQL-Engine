#!/usr/bin/env jython

# Custom Libraries goes here
import	customHDR	as HDR

class initXML:
	
	def __init__ (self):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		self.sCfgDir	= HDR.OS.environ.get ('PWD') + '/dbase_Config/'
		self.fUserCfg	= self.sCfgDir + 'userConfig.xml'
		
		# Call the XML Parser
		with open (self.fUserCfg) as oUserCfgFD:
			
			self.oXML	= HDR.XML.parse (oUserCfgFD.read ())
		
		return
	
	def _parse (self):
		
		ROUTINE	= HDR.SYS._getframe().f_code.co_name
		
		HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		# Check the Debug Flag
		sTmp	= self.oXML ['PROPERTY']['DEBUG']
		
		if sTmp == "1":
			
			# Enable Debugging
			
			HDR.goDebug.debugState (piFlag=True)
			
			HDR.goDebug.write (psMsg='[%s] - Inside [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
			
		else:
			
			# Disable Debugging
			
			HDR.goDebug.debugState (piFlag=False)
		
		# Check Install path
		sTmp	= self.oXML ['PROPERTY']['INSTALL_PATH']
		
		HDR.goDebug.write (psMsg='[%s] - [%s] INSTALL_PATH [%s]' %(HDR.OS.path.basename(__file__), ROUTINE, sTmp))
		
		try:
			
			if HDR.OS.path.exists (sTmp):
				
				HDR.gsInstallPath	= sTmp
		
		except Exception as e:
				
			raise Exception ('Install Path Not available. Reconfigure in XML. Aborting')
		
		# Check Page Size
		sTmp	= self.oXML ['PROPERTY']['PAGE_SIZE']
		
		HDR.goDebug.write (psMsg='[%s] - [%s] PAGE_SIZE [%s]' %(HDR.OS.path.basename(__file__), ROUTINE, sTmp))
		
		try:
			
			# Page Size ranges between 512 Byte to 65536 Byte only
			if int (sTmp) < 512 or int (sTmp) > 65536:
				
				sTmp	= 512
		
		except Exception as e:
			
			sTmp	= 512
				
			printException ('Page Size is Invalid or Not Available. Reconfigure in XML. Defaulting to 512 Byte')
		
		# Check BranchingFactor
		sTmp	= self.oXML ['PROPERTY']['BRANCHING_FACTOR']
		
		HDR.goDebug.write (psMsg='[%s] - [%s] BRANCHING_FACTOR [%s]' %(HDR.OS.path.basename(__file__), ROUTINE, sTmp))
		
		try:
			
			# Branching Factor ranges between 2 Byte to 100 only
			if int (sTmp) < 2 or int (sTmp) > 100:
				
				sTmp	= 6
		
		except Exception as e:
			
			sTmp	= 6
				
			printException ('Branching Factor is Invalid or Not Available. Reconfigure in XML. Defaulting to Order 6')
		
		HDR.goBPTPD.iBranchingFactor	= int (sTmp)
		
		HDR.goDebug.write (psMsg='[%s] - [%s] Final Branching Factor Considered [%s]' %(HDR.OS.path.basename(__file__), ROUTINE, str (HDR.goBPTPD.iBranchingFactor)))
		
		HDR.goDebug.write (psMsg='[%s] - Returning from [%s]' %(HDR.OS.path.basename(__file__), ROUTINE))
		
		return

def startProcedure (): 
	
	ROUTINE	= HDR.SYS._getframe().f_code.co_name
	
	oStart	= initXML ()
	
	oStart._parse ()
	
	return	oStart.oXML

if __name__ == '__main__':
	
	startProcedure ()
