Public Methods:

Name: loadJson
Arguments: 
    inputfilename
    deleteExistingConfig
        Default=True

Name: resetConfig
Arguments: None

Name: runAllTests
Arguments: None

Name: runTest
Arguments:
    testname
    testdict

Name: runFixedDurationTest(self, testname, testdict): 
Arguments: None

Name: runPingTest(self, testname, testdict):        
Arguments: None

Name: trafficStart(self): 
Arguments: None

Name: trafficStop(self):
Arguments: None

Name: trafficLearn(self, learningmode):
Arguments: None

Name: trafficWaitUntilDone(self):
Arguments: None

Name: resultsClear(self):
Arguments: None

Name: createStreamBlock(self, port, name, dictconfig=""):
Arguments: None

Name: createDevice(self, port, name, dictconfig=""):
Arguments: None

Name: createModifier(self, streamblock, modifiertype, dictconfig=""): 
Arguments: None

Name: connectAndApply(self, revokeowner=False): 
Arguments: None

Name: relocatePort(self, portname, location): 
Arguments: None

Name: isTrafficRunning(self): 
Arguments: None

Name: saveResultsDb(self, filename): 
Arguments: None

Name: generateCsv(self, resultsdb, prefix=""):        
Arguments: None

