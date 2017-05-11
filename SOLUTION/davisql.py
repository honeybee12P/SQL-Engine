#!/usr/bin/env jython

# Custom Libraries goes here
import	customHDR	as	HDR

import	xmlParser	as	XML
import	prompt		as 	PROMPT

def startProcedure (): 
	
	ROUTINE	= HDR.SYS._getframe().f_code.co_name
	
	# Initialize standard `Extern` varialbes
	HDR.init ()
	
	try:
		
		oXML	= XML.startProcedure ()
		
		oPrompt	= PROMPT.startProcedure (poXML=oXML)
	
	except Exception as e:
		
		raise Exception (e)
		
	return

if __name__ == '__main__':
	
	startProcedure ()
	
	HDR.SYS.exit (0)
