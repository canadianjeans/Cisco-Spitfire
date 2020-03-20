###############################################################################
#
#           Spirent TestCenter Stc_Gen Traffic Generator Library
#                         by Spirent Communications
#
#   Date: November 28, 2017
# Author: Matthew Jefferson - matt.jefferson@spirent.com
#
# Description: This library provides the user with functions that converts
#              JSON-formatted input into Spirent TestCenter configuration.
#
# NOTE:
#   To build the HTML documentation, use the command "pydoc -w stc_gen", executed
#   in the same directory as the source code.
#
###############################################################################
#
# Modification History
# Version  Modified
# 1.5.0    03/20/2020 by Matthew Jefferson
#           -Added support for RFC 2544 testing.
#
# 1.4.0    03/12/2020 by Matthew Jefferson
#           -Added the methods "breakLinks", "restoreLinks", "bgpWithdrawRoutes"
#            and "bgpReadvertiseRouters".
#
# 1.3.5    01/16/2019 by Matthew Jefferson
#           -Increased the delay between traffic stopping and retrieving the results
#            database for the runFixedDurationTest method. This should prevent any
#            problems with results not being up-to-date.
#
# 1.3.4    12/13/2018 by Matthew Jefferson
#           -Fixed a bug with the Sqlite table creation.
#
# 1.3.3    12/12/2018 by Matthew Jefferson
#           -Fixed an issue with IPv6 modifiers.
#           -Numerous small changes and fixes.
#
# 1.3.2    09/11/2018 by Matthew Jefferson
#           -Made some small modifications to the results processing code. The changes
#            should result in some massive speed improvements over the original code.
#
# 1.3.1    09/06/2018 by Matthew Jefferson
#           -Using a new internal download mechanism. This should improve the speed
#            of downloading DB files when there is a large number of them.
#
# 1.3.0    09/06/2018 by Matthew Jefferson
#           -New objects with the same name as an existing object now re-use the exiting
#            object. This prevents duplicates from being created.
#           -When using the runFixedDurationTest() method, result DB files are now
#            either zipped or deleted after they are no longer needed.
#
# 1.2.2    08/28/2018 by Matthew Jefferson
#           -Changed the python method used to move files. This should fix an issue
#            moving files between file systems.
#           -Now displaying the error message when downloading the results database.
#
# 1.2.1    08/22/2018 by Matthew Jefferson
#           -Added an stc.apply() to the loadJson method.
#           -runFixedDurationTest now defaults to "None" for learning.
#
# 1.2.0    08/22/2018 by Matthew Jefferson
#           -This is a substantial revision.
#           -Added linkUp, arpNdSuccess, archiveDirectory, setFrameLength and getObject.
#           -Modified the connectAndApply command to allow it to block until
#            links are up and ARP/ND is successful.
#           -runFixedDurationTest can now be run without learning.
#           -Default log path is now /tmp/StcGen_logs.
#           -The library can now connect to an existing session and reuse the 
#            existing configuration.
#           -Removed the findObjectsByName method. This has been replaced by getObject.
#
# 1.1.0    08/17/2018 by Matthew Jefferson
#           -I've added this modification history.
#           -Fixed an issue with the getResultsDictFromDb. Tx frame counts were
#            not being calculated correctly.
#           -Added the waitForLinkUp method.
#           -Added the waitForArpNdSuccess method.
#
# 1.0.0    01/01/2018 by Matthew Jefferson
#           -The initial release of the code.
#
###############################################################################
# Copyright (c) 2017 SPIRENT COMMUNICATIONS OF CALABASAS, INC.
# All Rights Reserved
#
#                SPIRENT COMMUNICATIONS OF CALABASAS, INC.
#                            LICENSE AGREEMENT
#
#  By accessing or executing this software, you agree to be bound by the terms
#  of this agreement.
#
# Redistribution and use of this software in source and binary forms, with or
# without modification, are permitted provided that the following conditions
# are met:
#  1. Redistribution of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#  2. Redistribution's in binary form must reproduce the above copyright notice.
#     This list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#  3. Neither the name SPIRENT, SPIRENT COMMUNICATIONS, SMARTBITS, nor the names
#     of its contributors may be used to endorse or promote products derived
#     from this software without specific prior written permission.
#
# This software is provided by the copyright holders and contributors [as is]
# and any express or implied warranties, including, but not limited to, the
# implied warranties of merchantability and fitness for a particular purpose
# are disclaimed. In no event shall the Spirent Communications of Calabasas,
# Inc. Or its contributors be liable for any direct, indirect, incidental,
# special, exemplary, or consequential damages (including, but not limited to,
# procurement of substitute goods or services; loss of use, data, or profits;
# or business interruption) however caused and on any theory of liability,
# whether in contract, strict liability, or tort (including negligence or
# otherwise) arising in any way out of the use of this software, even if
# advised of the possibility of such damage.
#
###############################################################################

# For Python 2.x compatibility (use Python3 syntax in Python2.7).
from __future__ import print_function

import sys
import platform
import os
import time
import datetime
import re
import getpass
import json
import sqlite3
import logging
import shutil
import zipfile
import ast

import atexit

from collections import defaultdict
# netaddr is required for converting IP/MAC addresses.
from netaddr import *


#print("DEBUG: Using pprint!!")
#import pprint
   
###############################################################################
####
####    Class Definition
####
###############################################################################

class StcGen:
    #==============================================================================
    #
    #   Constructor/Destructor
    #
    #==============================================================================
    def __init__(self, userest=False, labserverip=None, username=None, sessionname="StcGen", existingsession=None, verbose=False, logpath=None, loglevel="INFO", cleanuponexit=True):
        """Initializes the object and loads the Spirent TestCenter API (either the ReST or native version).

        Parameters
        ----------
        userest : bool
            Set to True to use the ReST API.
        labserverip : str
            The IP address of the Lab Server. Required if using the ReST API.
        username: str
            The username for the Lab Server session. Only applicable if using the Lab Server. Defaults to the current user.
        sessionname: str
            The name of the Lab Server session. Only applicable if using the Lab Server.
        existingsession: str
            The action to take if there is an existing session: join or kill. If not set, an exception will be raised if there
            is an existing session.
        verbose: bool
            All log messages will be sent to STDOUT if set to True.
        logpath: str
            The parent directory where the temporary session directory will be created. Using this argument will override the
            STC_LOG_OUTPUT_DIRECTORY environment variable. The default is './StcGen_logs'.
        loglevel: str
            The level of detail to include in the logs. These levels can be: ERROR, WARNING, INFO, DEBUG.
        cleanuponexit: bool
            Deletes the temporary session (logs) directory on exit.

        """

        # Create the session temporary directory. This is where all logs and results will be stored during the session.
        # All log files are saved in ./StcGen_logs/<YYY-MM-DD-HH-MM-SS_PIDXXXX>, unless the environment variable STC_LOG_OUTPUT_DIRECTORY is specified.

        arguments = locals()

        if cleanuponexit:
            atexit.register(self.cleanupTempDirectory)

        self.verbose = verbose
        self.cleanuponexit = cleanuponexit
        self.labserverip = labserverip

        # Construct the log path. The default location is in /tmp.    
        if sys.platform == "win32":
            defaultlogpath = "./StcGen_logs"
        else:
            defaultlogpath = "/tmp/StcGen_logs"

        now = datetime.datetime.now()
        tempdir = now.strftime("%Y-%m-%d-%H-%M-%S")
        tempdir += "_PID"
        tempdir += str(os.getpid())
        defaultlogpath = os.path.join(defaultlogpath, tempdir)
        defaultlogpath = os.path.expanduser(defaultlogpath)
        
        # The STC_LOG_OUTPUT_DIRECTORY will override the default path.
        self.logpath = os.getenv("STC_LOG_OUTPUT_DIRECTORY", defaultlogpath)

        # The logpath argument will override everything.
        if logpath:
            self.logpath = logpath

        self.logpath = os.path.abspath(self.logpath)
        self.logfile = os.path.join(self.logpath, "stc_gen.log")        

        if not os.path.exists(self.logpath):
            os.makedirs(self.logpath)

        # NOTE: Consider limiting the number of log directories that are created.
        #       It would mean deleting older directories.
        if loglevel.upper() == "ERROR":
            self.loglevel = logging.ERROR
        elif loglevel.upper() == "WARNING":
            self.loglevel = logging.WARNING
        elif loglevel.upper() == "INFO":
            self.loglevel = logging.INFO
        elif loglevel.upper() == "DEBUG":
            self.loglevel = logging.DEBUG
        else:
            self.loglevel = logging.INFO

        logging.basicConfig(filename=self.logfile, filemode="w", level=self.loglevel, format="%(asctime)s %(message)s")

        # The logger is now ready.            
        #self.pp = pprint.PrettyPrinter(indent=2)

        logging.info("Executing __init__: " + str(arguments))

        logging.info("Python Version: " + str(sys.version))
        logging.info("Platform: " + platform.platform())
        logging.info("System: " + platform.system())
        logging.info("Release: " + platform.release())
        logging.info("Version: " + platform.version())

        if not username:
            username = getpass.getuser()
                
        self.usingrestapi = False
        if userest:
            logging.info("Using the Python ReST adapter")
            from stcrestclient import stcpythonrest
            self.stc = stcpythonrest.StcPythonRest()

            logging.info("Using the Lab Server (" + labserverip + ") session " + sessionname + " - " + username)
        
            # This is the REST-only method for connecting to the server.
            self.stc.new_session(labserverip, session_name=sessionname, user_name=username, existing_session=existingsession)

            self.usingrestapi = True

        else:            
            logging.info("Using the native Python API")
            from StcPython import StcPython
            self.stc = StcPython()  

            if labserverip:       

                if self.__doesSessionExist(testsessionname=sessionname, ownerid=username, action=existingsession) and existingsession.lower() == "join":
                    logging.info("Joining the Lab Server (" + labserverip + ") session " + sessionname + " - " + username)
                    self.stc.perform("CSTestSessionConnect", host=labserverip,
                                                             TestSessionName=sessionname,
                                                             OwnerId=username,
                                                             CreateNewTestSession=False)    
                else:
                    logging.info("Creating the Lab Server (" + labserverip + ") session " + sessionname + " - " + username)
                    self.stc.perform("CSTestSessionConnect", host=labserverip,
                                                             TestSessionName=sessionname,
                                                             OwnerId=username,
                                                             CreateNewTestSession=True)    

                # This instructs the Lab Server to terminate the session when the last client disconnects.
                self.stc.perform("TerminateBll", TerminateType="ON_LAST_DISCONNECT")                

        logging.info("Spirent TestCenter Version: " + self.stc.get("system1", "Version"))
        
        # This dictionary keeps track of relations that we need to resolve after all
        # objects have been created. This is necessary to prevent race conditions.
        self.relations = {}

        # This dictionary keeps track of modifiers. We need to delay creation of modifiers until
        # after all relations have been resolved. This prevents a race condition involving bound IPv6 streamblocks.
        self.modifiers = {}

        # This dictionary keeps track of all objects that we have created. 
        # The key is the object name so multiple objects with the same name is a problem.
        self.objects = {}
        self.__populateObjectsDict()

        #self.pp.pprint(self.objects)

        self.project = self.stc.get("system1", "children-project")

        logging.info("The StcGen object has been initialized.")
                

        return

    #==============================================================================
    #
    #   Public Methods
    #
    #==============================================================================
    def loadJson(self, inputfilename, deleteExistingConfig=False, autoDeleteDevices=False, autoDeleteStreamBlocks=True):
        """Parses the specifed JSON file and generates the corresponding Spirent TestCenter objects.

        Parameters
        ----------
        inputfilename : str
            The name of the JSON input configuration file.
        deleteExistingConfig : bool
            Set to True to delete the existing project and create a new configuration.
        autoDeleteDevices : bool
            Set to False prevent the deletion of all existing EmulatedDevices.
        autoDeleteStreamBlocks : bool
            Set to False prevent the deletion of all existing StreamBlocks.

        """

        logging.info("Executing loadJson: " + str(locals()))

        # The configuration should be specified in the JSON.
        if deleteExistingConfig:
            self.resetConfig()

        inputdict = self.__convertJsonToDict(inputfilename)

        # Start with the configuration section of the input file.
        if "Configuration" in inputdict.keys():
            # Loading a new configuration will reset the existing config.
            configdict = inputdict["Configuration"]

            if (autoDeleteDevices or autoDeleteStreamBlocks) and "DeleteExisting" not in configdict.keys():
                configdict["DeleteExisting"] = []

            if autoDeleteDevices and "EmulatedDevice" not in configdict["DeleteExisting"]:
                configdict["DeleteExisting"].append("EmulatedDevice")

            if autoDeleteStreamBlocks and "StreamBlock" not in configdict["DeleteExisting"]:
                configdict["DeleteExisting"].append("StreamBlock")                

            # Determine if the configuration is specified in a TCC file.
            if "ConfigFileName" in configdict.keys():
                # The configuration is defined in a TCC/DB/XML file.
                filename = configdict["ConfigFileName"]

                if os.path.isfile(filename): 
                    extension = filename.split(".")[-1].lower()

                    if extension == "xml":
                        self.stc.perform("LoadFromXml", filename=filename)
                    elif extension == "tcc" or extension == "db":
                        self.stc.perform("LoadFromDatabase", DatabaseConnectionString=filename)
                    else:
                        errmsg = "The file '" + filename + "' needs to be either an XML, TCC or DB file."
                        logging.error(errmsg)
                        raise Exception(errmsg)

                    logging.info("Successfully loaded the configuration file " + filename)
                else:
                    errmsg = "The configuration file '" + filename + "' does not exist."
                    logging.error(errmsg)
                    raise Exception(errmsg)
            else:
                self.project = self.stc.get("system1", "children-project")

                # Parse the configuration dict and create all objects.
                self.__addObject(configdict)

                # Resolve all object references from the configuration.
                # We need to do this after all of the objects have been created
                # in order to prevent any race conditions.
                self.__resolveRelations()

                self.__addModifiers()

        # Now deal with the Tests section of the input file.
        if "Tests" in inputdict.keys():
            # NOTE: The order of keys for a JSON file is NOT maintain when it gets 
            #       loaded into Python. The order of the tests is determined by 
            #       the name of the tests!!!
            self.testsdict = inputdict["Tests"]

        logging.info("Successfully loaded the JSON configuration.")

        self.stc.apply()

        return

    #==============================================================================
    def resetConfig(self):
        """Delete the existing project and return the configuration to the default state.

        """

        logging.info("Executing resetConfig:")

        # Reset the configuration.
        self.stc.perform("ResetConfig", config="system1")        
        self.relations = {}
        self.modifiers = {}
        self.objects = {}
        self.testsdict = {}

        return

    #==============================================================================
    def runAllTests(self, revokeowner=False, waitforlinkup=60, waitforarpndsuccess=60):
        """Run all tests defined by the JSON configuration.

        Returns
        -------
        dict
            Returns a dictionary containing test status, statistics and results database filename information.         
        
        """

        logging.info("Executing runAllTests:")

        self.connectAndApply(revokeowner=revokeowner, waitforlinkup=waitforlinkup, waitforarpndsuccess=waitforarpndsuccess)   

        # An exception will be generated if the links do not come up.
        #self.waitForLinkUp()

        results = {}

        # First, make sure none of the tests are "continuous"
        for testname in sorted(self.testsdict.keys()):
            #testtype = self.testsdict[testname].get("Type","FixedDuration")
            results[testname] = self.runTest(testname, parametersdict=self.testsdict[testname].copy())

        #self.__lprint("Disconnecting from hardware...")
        #self.stc.perform("ChassisDisconnectAll")

        return(results)

    #==============================================================================
    def runTest(self, testname, testtype="FixedDuration", parametersdict=None, **kwargs): 
        """Run the specified test type.
        
        Parameters
        ----------
        testname : str
            The name of the test. For reporting purposes only!
        testtype: str
            The type of test to run. This argument is overridden by the "Type" setting
            for the parameterdict.
        parametersdict : dict
            A dictionary of test parameters. Users can use either the parametersdict or
            keyword arguments. parameterdict settings take precedence over keyword arguments.
        **kwargs
            Arbitrary keyword arguments for the test. These are test type dependent 
            and are overriden by parametersdict settings.
        
        Returns
        -------
        dict
            Returns a dictionary containing test status, statistics and results database filename information. 

        """         

        logging.info("Executing runTest: " + str(locals()))

        # Options are: Ping, FixedDuration. Consider adding RFC 2544 and 2889 (as well as others).
        if not parametersdict:
            parametersdict = {}

        testtype = str(parametersdict.get("Type", testtype))

        del parametersdict["Type"]

        results = {}
        if testtype.lower() == "fixedduration":
            results = self.runFixedDurationTest(testname, parametersdict=parametersdict, **kwargs)
        elif testtype.lower() == "ping":
            results = self.runPingTest(testname, parametersdict=parametersdict, **kwargs)
        elif testtype.lower() == "rfc2544":
            results = self.runRfc2544Test(testname, parametersdict=parametersdict, **kwargs)
        else:
            errmsg = "Unknown test type '" + testtype + "' for test '" + testname + "'."
            logging.error(errmsg)
            raise Exception(errmsg)

        return(results)

    #==============================================================================
    def runFixedDurationTest(self, testname, 
                                   Duration       = 60,
                                   DurationMode   = "SECONDS",
                                   LearningMode   = "None",
                                   FrameLengths   = None,
                                   Loads          = None,
                                   LoadUnit       = "PERCENT_LINE_RATE",
                                   ResultModes    = ['ALL'],
                                   DeleteResults  = True,
                                   parametersdict = None): 
        """Run a fixed duration test.        
        
        Parameters
        ----------
        testname : str
            The name of the test. This is only used for reporting purposes!
        Duration: int
            The duration of the test. The DurationMode determines the units for this value.
        DurationMode: str
            One of these values: "SECONDS" or "BURSTS".
        LearningMode: str
            One of these values: "None", "L2" or L3".
        FrameLengths: List(str)
            A list of frame lengths to execute the test for. Set to "-1" to use the existing streamblock frame length.
            A range of frame lengths may be specified with the starting, ending and step value (e.g. 128:1518+4).
        Loads: List(int)
            A list of loads to execute the test for.
        LoadUnit: str
            The units for the load. One of these values: "PERCENT_LINE_RATE", "FRAMES_PER_SECOND",
            "INTER_BURST_GAP", "BITS_PER_SECOND", "KILOBITS_PER_SECOND", "MEGABITS_PER_SECOND" or "L2_RATE".
        ResultModes: List(str)
            A list of result types that will be returned after the test: "ALL", "FLOW", "STREAM", "STREAMBLOCK", "PORT"
        DeleteResults: bool
            True: (default) The result DB files are deleted after they are added to the results dictionary. 
            False: The result DB files are zipped after they are added to the results dictionary.
        parametersdict : dict
            A dictionary of test parameters. Users can use either the parametersdict or
            keyword arguments. parameterdict settings take precedence over keyword arguments.      
        
        Returns
        -------
        dict
            Returns a dictionary containing test status, statistics and results database filename information.

        """ 

        logging.info("Executing runFixedDurationTest: " + str(locals()))

        if not parametersdict:
            parametersdict = {}        

        # Override the keyword arguments with the parametersdict settings.
        duration      = parametersdict.get("Duration",      Duration)
        durationmode  = parametersdict.get("DurationMode",  DurationMode)
        learning      = parametersdict.get("LearningMode",  LearningMode).upper()
        framelengths  = parametersdict.get("FrameLengths",  FrameLengths)      
        loads         = parametersdict.get("Loads",         Loads)
        loadunit      = parametersdict.get("LoadUnit",      LoadUnit)
        resultmodes   = parametersdict.get("ResultModes",   ResultModes)
        deleteresults = parametersdict.get("DeleteResults", DeleteResults)

        deleteresults = ast.literal_eval(str(deleteresults))

        # Create a timestamp for the result databases.
        now = datetime.datetime.now()
        timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
                
        resultsdbfilename = parametersdict.get("ResultsDbFileName", os.path.join("./results/", testname + "_" + timestamp + ".db"))

        results = {}
        results["TestType"] = "FixedDuration"
        results["Iterations"] = {}
        #results["ResultsDataBases"] = []
        results["Status"] = "FAILED"

        if "DataMining" in parametersdict.keys():
            self.__enableDataMining(parametersdict["DataMining"])

        portlist = self.stc.get(self.project, "children-port").split()

        # Set the duration for each port.
        for port in portlist:
            # Port locations in the name may screw things up.
            self.stc.config(port, AppendLocationToPortName=False)

            self.stc.config(port + ".generator.generatorconfig", DurationMode=durationmode, Duration=duration)         

        self.stc.apply()

        if not self.linksUp(ignoreportswithouttraffic=True):
            errmsg = "Some port links appear to be down. Aborting test..."
            logging.error(errmsg)
            raise Exception(errmsg)              
    
        if learning == "L3":
            self.trafficLearn("L3")
        elif learning == "L2":
            self.trafficLearn("L2")

        if not self.arpNdSuccess(): 
            errmsg = "ARP/ND appears to have failed. Aborting test..."
            logging.error(errmsg)
            raise Exception(errmsg)                      

        self.__lprint("Starting devices...")
        self.stc.perform("DevicesStartAll")

        # If set to -1, use the configured load/framelength.
        if not framelengths:
            framelengths = [-1]
        if not loads:
            loads = [-1]

        # Search for any frame length ranges in the framelength list. If any are found, replace the range 
        # with the actual frame length values.
        newframelengths = []
        for framelength in framelengths:
            if type(framelength) is not int:

                # This may be a frame length range (start:end+step)
                if len(framelength.split(":")) == 2:
                    start = framelength.split(":")[0]
                    stop  = framelength.split(":")[1]
                    step  = 1

                    if len(stop.split("+")) == 2:
                        step = stop.split("+")[1]
                        stop = stop.split("+")[0]

                    start = int(start)
                    stop  = int(stop)
                    step  = int(step)

                    for iteration in range(start, stop + 1, step):
                        newframelengths.append(iteration)
                else:
                    errmsg = "The framelength range " + str(framelength) + " does not appear to be valid. Please use the format <start>:<stop> or <start>:<stop>+<step>."
                    logging.error(errmsg)
                    raise Exception(errmsg)              
            else:
                newframelengths.append(framelength)

        framelengths = newframelengths             

        iteration = 1
        for framelength in framelengths:
            for load in loads:
                currentfilename = resultsdbfilename
                if load > 0:
                    self.__lprint("Setting Load to " + str(load))
                    
                    # Add the load to the results filename.
                    path     = os.path.dirname(currentfilename)
                    filename = os.path.basename(currentfilename)
                    basename = filename.split(".")[0]
                    basename += "_" + str(load)
                    extension = ".".join(filename.split(".")[1:])
                    currentfilename = os.path.join(path, basename + "." + extension)

                    for port in portlist:
                        self.stc.config(port + ".generator.generatorconfig", SchedulingMode="PORT_BASED", LoadUnit=loadunit, FixedLoad=load)                  

                if framelength > 0:
                    self.__lprint("Setting FrameLength to " + str(framelength))
                    
                    # Add the framelength to the results filename.
                    path     = os.path.dirname(currentfilename)
                    filename = os.path.basename(currentfilename)
                    basename = filename.split(".")[0]
                    basename += "_" + str(framelength)
                    extension = ".".join(filename.split(".")[1:])
                    currentfilename = os.path.join(path, basename + "." + extension)

                    self.setFrameLength(framelength)

                self.stc.apply()

                self.stc.perform("ResultsClearAll")
                               
                self.trafficStart()

                self.trafficWaitUntilDone()

                # Give the frames a little time to clear the DUT.
                time.sleep(3)                

                # Traffic has stopped. Gather results.
                resultsfilename = self.saveResultsDb(currentfilename)
                #resultsfilename = self.saveResultsDb(currentfilename, deletetemp=False)

                results["Iterations"][iteration] = {}

                if load > -1:
                    results["Iterations"][iteration]["Load"] = load
                    results["Iterations"][iteration]["LoadUnit"] = loadunit
                else:
                    results["Iterations"][iteration]["Load"] = "N/A"
                    results["Iterations"][iteration]["LoadUnit"] = "N/A"
                    
                if framelength > -1:
                    results["Iterations"][iteration]["FrameLength"] = framelength
                else:
                    results["Iterations"][iteration]["FrameLength"] = "N/A"

                results["Iterations"][iteration]["Database"]         = resultsfilename
                results["Iterations"][iteration]["FlowStats"]        = {}
                results["Iterations"][iteration]["StreamStats"]      = {}
                results["Iterations"][iteration]["StreamBlockStats"] = {}
                results["Iterations"][iteration]["PortStats"]        = {}

                if resultsfilename:
                    for mode in resultmodes:
                        if mode.upper() == "FLOW" or mode.upper() == "ALL": 
                            results["Iterations"][iteration]["FlowStats"] = self.getResultsDictFromDb(resultsfilename, mode="FLOW")

                        if mode.upper() == "STREAM" or mode.upper() == "ALL": 
                            results["Iterations"][iteration]["StreamStats"] = self.getResultsDictFromDb(resultsfilename, mode="STREAM")

                        if mode.upper() == "STREAMBLOCK" or mode.upper() == "ALL": 
                            results["Iterations"][iteration]["StreamBlockStats"] = self.getResultsDictFromDb(resultsfilename, mode="STREAMBLOCK")

                        if mode.upper() == "PORT" or mode.upper() == "ALL": 
                            results["Iterations"][iteration]["PortStats"] = self.getPortResultsDictFromDb(resultsfilename)

                    if not deleteresults:                        
                        # Zip the result DB file.                                 
                        zipfilename = resultsfilename + ".zip"
                        zipfh = zipfile.ZipFile(zipfilename, 'w')
                        zipfh.write(resultsfilename, compress_type=zipfile.ZIP_DEFLATED)
                        zipfh.close()

                    # Delete the original result DB file.
                    os.remove(resultsfilename)

                    #self.generateCsv(resultsfilename)
                
                iteration += 1

        results["Status"] = "PASSED"

        return(results)

    #==============================================================================
    def runPingTest(self, testname, Count=1, parametersdict=None):        
        """Run a ping test.

        A ping is sent from each port to all of the gateway addresses defined for that port.

        
        Parameters
        ----------
        testname : str
            The name of the test. This is only used for reporting purposes!
        Count: int
            The number of pings to send to each gateway.        
        parametersdict : dict
            A dictionary of test parameters. Users can use either the parametersdict or
            keyword arguments. parameterdict settings take precedence over keyword arguments.      
        
        Returns
        -------
        dict
            Returns a dictionary containing test status, statistics and results database filename information.

        """                     

        logging.info("Executing runPingTest: " + str(locals()))

        # The number of pings per device.
        count = parametersdict.get("Count", Count)

        results = {}
        results["Status"] = "FAILED"

        # Compile a list of unique gateways for each port. We only consider EmulatedDevices for this test.
        devices = self.stc.get("system1.project", "children-emulateddevice").split()

        gateways = {}        
        for device in devices:
            for interface in self.stc.get(device, "toplevelif").split():
                # NOTE: This code doesn't work if the interface is using a range or list of gateways.
                #       It also doesn't work if there isn't an IPv4 or IPv6 interface defined.
                gateway = str(IPAddress(self.stc.get(interface, "Gateway")))

                if gateway == "" or gateway == "::":
                    continue

                port = self.stc.get(device, "AffiliationPort-Targets")

                if port in gateways.keys():
                    if gateway not in gateways[port].keys():
                        gateways[port][gateway] = device
                else:
                    gateways[port] = {}
                    gateways[port][gateway] = device

        # Now perform the Ping Test on each unique gateway.
        for port in gateways.keys():
            for gateway in gateways[port].keys():
                self.stc.perform("ResultsClearAll", PortList=port)
                device = gateways[port][gateway]
                #print("DEBUG: Pinging " + port + " " + gateway + " " + device)                
                result = self.stc.perform("PingVerifyConnectivity", DeviceList=device, FrameCount=count)
                if result["PassFailState"] == "FAILED":
                    passed = False
                    print("PING TEST FAILED on " + port + " Gateway=" + gateway + " Device=" + device)

        if passed:
            results["Status"] = "PASSED"
                    
        return(results)

    #==============================================================================
    def runRfc2544Test(self, testname, parametersdict=None, **kwargs): 
        """Run a fixed duration test.        
        
        Parameters
        ----------
        testname : str
            The name of the test. This is only used for reporting purposes!
        AcceptableFrameLoss: float (0)
            Maximum percentage of frames that can be lost during an iteration in order for the iteration to be considered successful.
        Backoff: float (50.0)
            Rate at which the load decreases after the DUT fails at the current load. Applicable when SearchMode is BINARY or COMBO.
        DelayStart: int (0)
            The number of seconds to wait before running the throughput tests. This allows enough time for devices to come up fully.
        Duration: float (60)
            The duration of the test. The DurationMode determines the units for this value.
        DurationMode: str (SECONDS)
            One of these values: "SECONDS" or "BURSTS".            
        EnableJitterMeasurement: bool (False)
            Whether to enable or disable jitter measurements.
        EnableMaxLatencyThreshold: bool (False)
            Whether the search algorithm uses the maximum latency per stream to determine the throughput.
        EnableOutOfSeqThreshold: bool (False)
            Whether the search algorithm uses the out-of-sequence frame count per stream to determine the throughput.
        FrameSizeList: list
            List of frame sizes. A separate test is throughput test is execute for each size.
        LatencyType: enum (LILO)
            Latency type for different devices. FIFO, LIFO, LILO.
        LearningFreqMode: enum (LEARN_ONCE)
            Learning frequency type. NONE, LEARN_ONCE, LEARN_EVERY_TRIAL, LEARN_EVERY_FRAME_SIZE, LEARN_EVERY_ITERATION.
        LearningMode: enum (AUTO)
            Learning mode. AUTO, NONE, L2_LEARNING, L3_LEARNING.
        LoadUnits: enum (PERCENT_LINE_RATE)
            Unit of measurement for the load parameter, applied to all ports and streams in the test.
        MaxLatencyThreshold: float (30)
            Maximum latency per stream that can be reached during an iteration in order for the iteration to be considered successful.
        NumOfTrials: int (1)
            Number of trials.
        OutOfSeqThreshold: int (0)
            Maximum out-of-sequence frames per stream that can be received during an iteration in order for the iteration to be considered successful.
        RateInitial: float (99.0)
            If SearchMode is set to BINARY, the load rate at the beginning of the test. This attribute is ignored if SearchMode is set to STEP.
        RateLowerLimit: float (1.0)
            Lowest transmission rate at which the application will send traffic during any test iteration, as a percentage of the total wire rate.
        RateStep: float (10.0)
            If SearchMode is set to STEP, the amount the load increases from iteration to iteration. This attribute is ignored if SearchMode is set to BINARY.
        RateUpperLimit: float (100.0)
            Maximum rate at which Spirent TestCenter will send traffic during the test,as a percentage of the total wire rate.
        Resolution: float (1.0)
            Finest adjustment possible for the load from iteration to iteration. If the required adjustment to the current load for the next iteration is less than the resolution, the search stops. Applicable when SearchMode is BINARY or COMBO.
        SaveResults: bool (True)
            Save the results database files in a Zip file after the test has completed.            
        SearchMode: enum (BINARY)
            Mode used to find the throughput rate. BINARY, STEP, COMBO.
        TrafficList: list
            A list of Streamblocks to include in the test. All streamblocks are used by default.
        TrafficVerificationAbortOnFail: bool (True)
            Test behavior when traffic verification fails.
        TrafficVerificationFreqMode: enum (NONE)
            Specifies when to perform traffic verification during the test. NONE, VERIFY_EVERY_TRIAL, VERIFY_EVERY_FRAME_SIZE, VERIFY_EVERY_ITERATION.
        TrafficVerificationTxFrameCount: int (100)
            Number of frames to use for traffic verification.
        TrafficVerificationTxFrameRate: int (1000)
            Traffic rate in frames per second to use for traffic verification.
        parametersdict : dict
            A dictionary of test parameters. Users can use either the parametersdict or
            keyword arguments. parameterdict settings take precedence over keyword arguments.      
        
        Returns
        -------
        dict
            Returns a dictionary containing test status, statistics and results database filename information.

        """ 

        logging.info("Executing runRfc2544Test: " + str(locals()))

        self.__lprint("Starting RFC 2544 Throughput Test...")

        if not self.linksUp(ignoreportswithouttraffic=True):
            errmsg = "Some port links appear to be down. Aborting test..."
            logging.error(errmsg)
            raise Exception(errmsg)              

        self.__lprint("Starting devices...")
        self.stc.perform("DevicesStartAll")        

        delay = kwargs.get("DelayStart", 0)
        time.sleep(delay)

        if parametersdict:
            for key in parametersdict.keys():
                kwargs[key] = parametersdict[key]

       # Deal with specific arguments.
        enabledetailedresults = True
        streamblocks = []

        for key in list(kwargs):
            if key.lower() == "enabledetailedresultscollection":                
                enabledetailedresults = kwargs[key]
                del kwargs[key]

            if key.lower() == "trafficlist":
                # Only include the specified streamblocks.
                for item in kwargs[key]:
                    # Determine if item is a name or handle.
                    handle = self.getObject(item, objecttype="streamblock")    
                    if handle:
                        streamblocks.append(handle)
                    else:
                        # Assume item is actually a streamblock handle.
                        streamblocks.append(item)
    
        if len(streamblocks) == 0:
            # All streamblocks.            
            for port in self.stc.get("system1.project", "children-port").split():
                streamblocks += self.stc.get(port, "children-streamblock").split()                

        kwargs["TrafficList"] = streamblocks                

        result = self.stc.perform("Rfc2544SetupThroughputTestCommand", **kwargs)

        configobject = result["benchmarkingtestcaseconfig-Targets"]

        self.stc.config(configobject, EnableDetailedResultsCollection=enabledetailedresults)

        self.stc.apply()

        self.__lprint("Starting sequencer...")
        self.stc.perform("SequencerStart")

        # Wait for the sequencer to stop.
        stop = False
        currentstep = ""
        while not stop:
            #self.pp.pprint(self.stc.get("system1.sequencer"))

            state = self.stc.get("system1.sequencer", "State")

            current = self.stc.get("system1.sequencer", "CurrentCommand")

            if current != "" and currentstep != self.stc.get(current, "ProgressCurrentStepName"):
                currentstep = self.stc.get(current, "ProgressCurrentStepName")

                self.__lprint(currentstep)
                #print(currentstep)

            if state == "PAUSE" or state == "IDLE" or state == "FINALIZE":
                stop = True
            else:
                time.sleep(1)        

        results = {}      
        results["Status"] = self.stc.get("system1.sequencer", "TestState")

        self.__lprint("Test complete: " + results["Status"])

        dbfilenames = self.__getRfc2544ResultFiles()

        # First, find the summary DB file.
        summarydbfilename = None
        for filename in dbfilenames:
            if re.search("Summary", filename):
                summarydbfilename = filename
                break                      

        results["PerFramesizeResult"] = self.__getRfc2544SummaryResults(summarydbfilename, "Rfc2544ThroughputPerFrameSizeResult")
        results["PerLoadResult"] = self.__getRfc2544SummaryResults(summarydbfilename, "Rfc2544ThroughputPerLoadResult")

        results["DatabaseArchive"] = ""
        if kwargs.get("SaveResults", True):            
            # Zip the result DB files for reference later.
            # Use the summary DB filename for the name of the Zip.
            zipfilename = os.path.splitext(summarydbfilename)[0]
            zipfilename += ".zip"
            zipfilename = os.path.split(zipfilename)[1]
            zipfilename = os.path.abspath(zipfilename)

            results["DatabaseArchive"] = zipfilename

            self.__lprint("Saving results DB files to " + zipfilename)

            zipfh = zipfile.ZipFile(zipfilename, 'w')
            for filename in dbfilenames:
                basefilename = os.path.basename(filename)
                zipfh.write(filename, arcname=basefilename, compress_type=zipfile.ZIP_DEFLATED)
            zipfh.close()

        # Delete the original result DB files.
        for filename in dbfilenames:
            os.remove(filename)        

        return(results)


    #==============================================================================
    def trafficStart(self):
        """Start the traffic generators on all ports.
        """

        logging.info("Executing trafficStart: " + str(locals()))

        #, checklinkstatus=True, checkarpndstatus=True

        # if checklinkstatus:
        #     print("ToDo: Add linksUp check here!!!!")
        #     self.linksUp()

        #     exception

        # if checkarpndstatus:
        #     print("ToDo: Add linksUp check here!!!!")
        #     self.()

        #     exception

        self.__lprint("Starting generators...")
        self.stc.perform("GeneratorStart")

        return  

    #==============================================================================
    def trafficStop(self):
        """Stop the traffic generators on all ports.
        """        
        logging.info("Executing trafficStop: " + str(locals()))

        self.__lprint("Stopping generators...")
        self.stc.perform("GeneratorStop")
        return   

    #==============================================================================
    def trafficLearn(self, learningmode):
        """Start learning on all ports.

        Parameters
        ----------
        learningmode : str
            Either "L2" or "L3".        

        """

        logging.info("Executing trafficLearn: " + str(locals()))

        passed = True

        if learningmode.upper() == "L2":
            self.stc.perform("ArpNdStartOnAllDevices")
            self.stc.perform("L2LearningStart")
        elif learningmode.upper() == "L3":  
            self.stc.perform("ArpNdStartOnAllDevices")
            self.stc.perform("ArpNdStartOnAllStreamBlocks") 

            #for port in self.stc.get("system1.project", "children-port").split():
            #    print(self.stc.get(port + ".ArpNdReport"))
            #    print(self.stc.get(port + ".ArpCache"))

            result = self.stc.perform("ArpNdVerifyResolved")

            if result["PassFailState"] != "PASSED":
                passed = False

        return(passed)

    #==============================================================================
    def trafficWaitUntilDone(self):
        """Blocks execution until all generators have stopped sending frames.

        Raises
        ------
        Exception
            An exception is generated if the DurationMode is set to CONTINUOUS for
            any port.

        """

        logging.info("Executing trafficWaitUntilDone: " + str(locals()))

        # NOTE: This method will raise an exception if there are any ports where
        #       the DurationMode is set to CONTINUOUS.        
        for port in self.stc.get(self.project, "children-port").split():
            if self.stc.get(port + ".generator.generatorconfig", "DurationMode").upper() == "CONTINUOUS":
                errmsg = "The DurationMode for port '" + self.stc.get(port, "Name") + "' is set to CONTINUOUS."
                logging.error(errmsg)
                raise Exception(errmsg)

        elapsedtime = 0
        while self.isTrafficRunning():
            self.__lprint("Test is running (" + str(elapsedtime) + " seconds have elapsed)...")
            time.sleep(1)    
            elapsedtime += 1

        return

    #==============================================================================
    def resultsClear(self):
        """Clears all statistics.        
        """

        logging.info("Executing resultsClear: " + str(locals()))

        self.stc.perform("ResultsClearAll")
        return         

    #==============================================================================
    def createStreamBlock(self, port, name, headers=None, parametersdict=None):
        """Create a StreamBlock object.

        Parameters
        ----------
        port : str
            The parent port object handle.
        name: str
            The name of the streamblock.
        headers: str
            A space-delimited string of PDU types. Some valid values are: 
            EthernetII, Vlan, IPv4, IPv6, Udp, Tcp and Custom.
        parametersdict : dict
            A dictionary of attributes. 
        
        Returns
        -------
        str
            The StreamBlock object handle.

        """
        logging.info("Executing createStreamBlock: " + str(locals()))

        if not parametersdict:
            # This may seem a bit odd, but we don't want to initialize the parametersdict this way in the function header. 
            parametersdict = {}

        frameconfig = ""
        if parametersdict and "Headers" in parametersdict.keys():
            # Valid header options are (NOTE: These are case sensitive!!!):
            #   EthernetII
            #   IPv4 
            #   IPv6
            #   Udp
            #   Tcp
            #   Custom
            frameconfig = parametersdict["Headers"]
            del parametersdict["Headers"]
        elif headers:
            frameconfig = headers

        if name in self.objects.keys():
            self.__lprint("WARNING: Duplicate object name. The object '" + name + "' (" + self.objects[name] + ") already exists.")
            streamblock = self.objects[name]
        else:
            streamblock = self.stc.create("StreamBlock", under=port, FrameConfig=frameconfig, Name=name)
            self.stc.perform("StreamBlockUpdate", StreamBlock=streamblock)

            self.objects[name] = streamblock

        self.__addObject(parametersdict, streamblock)

        return streamblock 

    #==============================================================================
    def createDevice(self, port, name, parametersdict=None):
        """Create an Emulated Device object.

        Parameters
        ----------
        port : str
            The parent port object handle.
        name: str
            The name of the device.
        parametersdict : dict
            A dictionary of attributes. 
        
        Returns
        -------
        str
            The device object handle.

        """ 

        logging.info("Executing createDevice: " + str(locals()))

        if not parametersdict:
            parametersdict = {}

        if "Encapsulation" in parametersdict.keys():
            # Valid Encapsulations are: IPv4, IPv6 and IPv4v6.

            encapsulation = parametersdict["Encapsulation"]
            del parametersdict["Encapsulation"]
        else:
            encapsulation = "IPv4"

        if "VlanCount" in parametersdict.keys():
            vlancount = parametersdict["VlanCount"]
            del parametersdict["VlanCount"]
        else:
            vlancount = 0      

        if name in self.objects.keys():
            self.__lprint("WARNING: Duplicate object name. The object '" + name + "' (" + self.objects[name] + ") already exists.")
            device = self.objects[name]
        else:
            edp = self.stc.create("EmulatedDeviceGenParams", under=self.project,
                                                             Port=port,
                                                             Count=1,
                                                             DeviceName=name,                                       
                                                             BlockMode="ONE_DEVICE_PER_BLOCK")

            ethparams = self.stc.create("DeviceGenEthIIIfParams", under=edp)
            stackedon = ethparams

            if vlancount > 0:
                # We don't need to set the "DeviceGenStackedOnIf" attribute. It will automatically be set correctly
                # for stacked vlans.
                for index in range(vlancount):
                    vlanparams = self.stc.create("DeviceGenVlanIfParams", under=edp, Count=1, RepeatMode="NO_REPEAT")
                stackedon = vlanparams

            # We DO need to set the "DeviceGenStackedOnIf" attribute, otherwise, STC might not stack the IPv4/IPv6 
            # interface correctly.
            if encapsulation.lower() == "ipv4" or encapsulation.lower() == "ipv4v6":
                self.stc.create("DeviceGenIpv4IfParams", under=edp, **{"DeviceGenStackedOnIf-targets" : stackedon}) 

            if encapsulation.lower() == "ipv6" or encapsulation.lower() == "ipv4v6":
                self.stc.create("DeviceGenIpv6IfParams", under=edp, AddrType="NON_LINK_LOCAL", **{"DeviceGenStackedOnIf-targets" : stackedon})                                                                                    
                self.stc.create("DeviceGenIpv6IfParams", under=edp, AddrType="LINK_LOCAL", UseEui64LinkLocalAddress=True, **{"DeviceGenStackedOnIf-targets" : stackedon})

            # Finally, execute the command that will create the emulated devices.
            result = self.stc.perform("DeviceGenConfigExpand", DeleteExisting="No", GenParams=edp)
            device = result["ReturnList"]

            self.objects[name] = device

        self.__addObject(parametersdict, device)

        return device

    #==============================================================================
    def createModifier(self, streamblock, modifiertype, parametersdict=None): 
        """Create a StreamBlock modifier object.

        Parameters
        ----------
        streamblock : str
            The parent streamblock object handle.
        modifiertype: str
            The type of modifier object: "RangeModifier", "RandomModifier" or "TableModifier".
        parametersdict : dict
            A dictionary of attributes. 
        
        Returns
        -------
        str
            The modifier object handle.

        """ 

        logging.info("Executing createModifier: " + str(locals()))

        if not parametersdict:
            parametersdict = {}        

        # Modifiers are very "picky".
        # The modifier object's attributes are case sensitive, you the user
        # must enter the correct field, or the modifier will not work.

        if "Field" in parametersdict.keys():
            field = parametersdict["Field"]
            del parametersdict["Field"]
        else:
            errmsg = "You must specify the field attribute for a modifier."
            logging.error(errmsg)
            raise Exception(errmsg)

        # Now, like we did for PDU headers, we must translate.
        field = self.__modifyPDUKey(field)

        # Split the "field" up to determine the object and attribute specified by the user.
        # e.g. ipv4:IPv4.SourceAddr
        #      object = ipv4:IPv4
        #      attribute = SourceAddr

        fieldlist = field.split(".")
        object = streamblock + "." + ".".join(fieldlist[0:-1])
        attribute = fieldlist[-1]

        # Also watch out for ToS or Diffserv.
        if re.search("tos|diffserv", attribute, flags=re.I):
            attribute = re.sub("tos|diffserv", "tosDiffserv.tos", attribute, flags=re.I)        

        reference = self.stc.get(object, "Name") + "." + attribute
        
        modifier = self.stc.create(modifiertype, under=streamblock, OffsetReference=reference)  

        #self.pp.pprint(self.stc.get(modifier))

        self.__addObject(parametersdict, modifier)

        return modifier

    #==============================================================================
    def setFrameLength(self, framelength):
        """Set the frame length for all streamblocks.

        Parameters
        ----------
        framelength : int
            The length of the frame in bytes (includes FCS).
        
        """

        for port in self.stc.get("system1.project", "children-port").split():
            for streamblock in self.stc.get(port, "children-streamblock").split():
                self.stc.config(streamblock, FixedFrameLength=framelength)  

        return

    #==============================================================================
    def connectAndApply(self, revokeowner=False, waitforlinkup=60, waitforarpndsuccess=60):
        """Connect to the IL (hardware), reserve ports and apply the configuration.

        Parameters
        ----------
        revokeowner : bool
            Set to True to revoke any existing reservations.

        waitforlinkup : int
            The timeout value for links up. Set to a value greater than 0 to wait for links 
            to be up before proceeding. If the links do not come up before the specified timeout 
            value, an exception will be generated.

        waitforarpndsuccess : int
            The timeout value for successful ARP/ND resolution. Set to a value greater than 0 to 
            wait for ARP/ND to be resolved before proceeding. If the ARP/ND is not resolved 
            before the specified timeout value, an exception will be generated.            
        
        """

        logging.info("Executing connectAndApply: " + str(locals()))

        offlineports = []
        for port in self.stc.get(self.project, "children-port").split():
            online = self.stc.get(port, "Online")
            if online.lower() == "false":
                offlineports.append(port)

        if len(offlineports) > 0:
            self.__lprint("Connecting to the hardware...")
            self.stc.perform("AttachPorts", portList=" ".join(offlineports),
                                            autoConnect=True,
                                            RevokeOwner=revokeowner)
        self.stc.apply()


        if waitforlinkup > 0:
            self.waitForLinkUp(timeout=waitforlinkup, ignoreportswithouttraffic=True)

        if waitforarpndsuccess > 0:
            self.waitForArpNdSuccess(timeout=waitforarpndsuccess)

        return

    #==============================================================================
    def relocatePort(self, portname, location): 
        """Change the port's location attribute.

        This is used to remap the port locations.
        NOTE: This must be performed BEFORE connecting to the hardware.

        Parameters
        ----------
        portname : str
            The name of the port to remap.
        location: str
            The new location string for the port. These have the syntax <chassisip>/<slot>/<port>
        
        Returns
        -------
        str
            The port object handle (if a matching port found).

        """

        logging.info("Executing relocatePort: " + str(locals()))

        # Find the specified port. This needs to be called BEFORE connecting to the
        # hardware.
        found = False
        for port in self.stc.get(self.project, "children-port").split():
            name = self.stc.get(port, "Name")
            if portname == name:
                found = True
                stc.config(port, location=location)
                break

        if not found:
            return(None)
        else:
            return(port)

    #==============================================================================
    def isTrafficRunning(self): 
        """Determines is the generators are running on any of the defined ports.
        
        Returns
        -------
        bool
            Returns True if any generator is not STOPPED.

        """

        logging.info("Executing isTrafficRunning: " + str(locals()))

        running = False
        for port in self.stc.get(self.project, "children-port").split():            
            if self.stc.get(port + ".generator", "State") != "STOPPED":
                running = True
                break

        return(running)

    #==============================================================================
    def linksUp(self, portnamelist=None, ignoreportswithouttraffic=False): 
        """Returns the link status for the specified ports.

        Parameters
        ----------
        portnamelist : list of str
            A list of ports to check for link up. All ports by default.

        ignoreportswithouttraffic: bool
            Set to True to only check those ports that are sending or receiving traffic.

        Returns
        -------
        bool
            Returns True if all specified port links are up. False otherwise.

        """
        logging.info("Executing linksUp: " + str(locals()))   

        if not portnamelist:
            if ignoreportswithouttraffic:
                # Only check those ports that Tx or Rx traffic frames.
                portnamelist = self.__getTrafficPorts()
            else:
                portnamelist = self.stc.get(self.project, "children-port").split()   

        portlist = []

        for portname in portnamelist:
            handle = self.getObject(portname, objecttype="port")    
            if handle:
                portlist.append(handle)
            else:
                # Just assume that the portname is actually a port handle.
                portlist.append(portname)

        linksup = True

        for port in portlist:
            result = self.stc.perform("PortSetupGetActivePhy", Port=port)
            phy = result['ActivePhy']
            linkstatus = self.stc.get(phy, "LinkStatus")
                
            if linkstatus.upper() != "UP":
                linksup = False
                break

        return(linksup)


    #==============================================================================
    def waitForLinkUp(self, portnamelist=None, timeout=60, ignoreportswithouttraffic=False): 
        """Block execution until all links are up. An exception is generated if the 
           links are not up by the specified timeout.

        Parameters
        ----------
        portnamelist : list of str
            A list of ports to check for link up. All ports by default.

        timeout : int
            The maximum time to wait for links up before generating an exception.

        ignoreportswithouttraffic: bool
            Set to True to only wait for those ports that transmit or receive traffic.

        """

        logging.info("Executing waitForLinkUp: " + str(locals()))   
       
        stop = False
        elapsedtime = 0
        while not stop:
            stop = True
            if not self.linksUp(portnamelist, ignoreportswithouttraffic=ignoreportswithouttraffic):
                stop = False

            elapsedtime += 1

            if elapsedtime > timeout:
                errmsg = "A timeout occurred while waiting for interface links to come up (" + str(elapsedtime) + " seconds)."
                logging.error(errmsg)
                raise Exception(errmsg)
            elif stop == False:
                time.sleep(1)

        return  

    #==============================================================================
    def breakLinks(self, portnamelist=None, waittime=0):
        """Breaks the specified physical port links. If specified, the links are 
        restored after the specified wait time.

        Parameters
        ----------
        portnamelist : list of str
            Break the links of these ports. All ports by default.

        waittime: int
            If specified (seconds), the links will be restored after specified wait time.

        """

        logging.info("Executing breakLinks: " + str(locals()))   

        if not portnamelist:
            portnamelist = self.stc.get(self.project, "children-port").split()   

        portlist = []

        for portname in portnamelist:
            handle = self.getObject(portname, objecttype="port")    
            if handle:
                portlist.append(handle)
            else:
                # Just assume that the portname is actually a port handle.
                portlist.append(portname)

        result = self.stc.perform("L2TestBreakLink", Port=" ".join(portlist))

        if waittime > 0:
            # Retore the links after the specified wait time (in seconds).
            time.sleep(waittime)
            self.restoreLinks(portnamelist)

        return

    #==============================================================================
    def restoreLinks(self, portnamelist=None):
        """Restore the links for the specified ports.

        Parameters
        ----------
        portnamelist : list of str
            Restores the links of these ports. All ports by default.

        """

        logging.info("Executing restoreLinks: " + str(locals()))   

        if not portnamelist:
            portnamelist = self.stc.get(self.project, "children-port").split()   

        portlist = []

        for portname in portnamelist:
            handle = self.getObject(portname, objecttype="port")    
            if handle:
                portlist.append(handle)
            else:
                # Just assume that the portname is actually a port handle.
                portlist.append(portname)

        result = self.stc.perform("L2TestRestoreLink", Port=" ".join(portlist))

        return

    #==============================================================================
    def arpNdSuccess(self):         
        """Returns the ARP/ND status (devices/streamblocks).

        Returns
        -------
        bool
            Returns True if ARP/ND is successful. False otherwise.

        """
        logging.info("Executing arpNdSuccess: " + str(locals()))   

        result = self.stc.perform("ArpNdVerifyResolved")

        #print("DEBUG")
        #self.pp.pprint(result)

        passed = True
        if result["PassFailState"] != "PASSED":
            passed = False    

        return(passed)

    #==============================================================================
    def waitForArpNdSuccess(self, timeout=60):       
        """Block execution until all ARP/ND is resolved. An exception is generated if the if a timeout occurs.

        Parameters
        ----------
        portnamelist : list of str
            A list of ports to check for ARP/ND completion.

        timeout : int
            The maximum time to wait for links up before generating an exception.

        """        
        
        stop = False
        start = datetime.datetime.now()
        while not stop:
            stop = True

            if not self.trafficLearn("L3"):                
                stop = False

            delta = datetime.datetime.now() - start
            elapsedtime = int(delta / datetime.timedelta(seconds=1))

            if not stop and elapsedtime > timeout:
                errmsg = "A timeout occurred while waiting for ARP/ND to be resolved (" + str(elapsedtime) + " seconds)."
                logging.error(errmsg)
                raise Exception(errmsg)

        return

    #==============================================================================
    def bgpWithdrawRoutes(self, objectnamelist=None):
        """Withdraw the BGP routes for the specified objects (ports or devices).

        Parameters
        ----------
        objectnamelist: list
            List of ports/device names to withdraw routes.

        """

        logging.info("Executing bgpWithdrawRoutes: " + str(locals()))   

        if not objectnamelist:
            objectnamelist = self.stc.get(self.project, "children-emulateddevice").split()   

        objectlist = []

        for name in objectnamelist:
            handle = self.getObject(name)    
            if handle:
                objectlist.append(handle)
            else:
                # Just assume that the name is actually an object handle.
                objectlist.append(name)

        result = self.stc.perform("BgpWithdrawRoute", RouteList=" ".join(objectlist))

        return

    #==============================================================================
    def bgpReadvertiseRoutes(self, objectnamelist=None):
        """Readvertise the BGP routes for the specified objects (ports or devices).

        Parameters
        ----------
        objectnamelist: list
            List of ports/device names to withdraw routes.

        """

        logging.info("Executing bgpReadvertiseRoutes: " + str(locals()))   

        if not objectnamelist:
            objectnamelist = self.stc.get(self.project, "children-emulateddevice").split()   

        objectlist = []

        for name in objectnamelist:
            handle = self.getObject(name)    
            if handle:
                objectlist.append(handle)
            else:
                # Just assume that the name is actually an object handle.
                objectlist.append(name)

        result = self.stc.perform("BgpReadvertiseRoute", RouterList=" ".join(objectlist))

        return        


    #==============================================================================
    def saveConfiguration(self, filename): 
        """Saves the current configuration to a TCC or XML file.

        Parameters
        ----------
        filename : str
            The filename of the database or XML file that will be saved.
       
        """

        extension = "tcc"

        if filename != "":
            extension = filename.split(".")[-1].lower()

        if extension == "xml":
            self.stc.perform("SaveAsXml", filename=filename)
        else:
            self.stc.perform("SaveToTcc", filename=filename)   

        self.stc.perform("cssynchronizefiles")

        return   
    

    #==============================================================================
    def saveResultsDb(self, filename): 
        """Saves the results to a SQLite database 

        Parameters
        ----------
        filename : str
            The filename of the database that will be saved.
        deletetemp : bool
            If True, delete the temporary directory used during file synchronization.
            This is only relevant when using a Lab Server.
       
        Returns
        -------
        str
            The fully normalized filename of the saved results database.

        """

        logging.info("Executing saveResultsDb: " + str(locals()))

        filename = os.path.abspath(filename)

        if self.labserverip:
            # ...not so simple...we need to download the file from the Lab Server,
            # and then move it to the specified location.
            path     = os.path.dirname(filename)
            filename = os.path.basename(filename)

            # We set the configuration filename so that when we use the cssynchronizefiles
            # command, the results DB files will be in a predictable location.
            self.stc.config("system1.project", ConfigurationFileName="stcgen_results")

        # Save the database to disk.
        self.stc.perform("SaveResult", CollectResult="TRUE",
                                       SaveDetailedResults="TRUE",
                                       DatabaseConnectionString=filename,
                                       OverwriteIfExist="TRUE")          

        if self.labserverip:
            # If we are using a Lab Server, we need to do a little dance to get the files
            # from there to the desired location on the local client.
            # First, copy all of the files from the Lab Server, and then copy the specific
            # database file to the desired target location.
            
            # Do the following in the temporary log directory. This will allow us to clean
            # up all of the extra files (logs and stuff) when we are done.
            originalpath = os.getcwd()
            temppath = self.logpath

            try:
                # Determine the sourcefilename BEFORE changing directories. It looks like 
                # there is some weirdness with the os.path.abspath() function.

                labserverfilename = os.path.join("stcgen_results", filename)

                sourcefilename = os.path.join(temppath, labserverfilename)                
                sourcefilename = os.path.abspath(sourcefilename)

                # Currently, the Python ReST adapter differs from the native Python API
                # when using the CsSynchronizeFiles command. The ReST adapter flattens
                # the directory structure and puts all files in the same directory.
                # We need to check for the results database in this secondary location
                # if we don't find it in the intended location.
                sourcefilename2 = os.path.join(temppath, filename)                
                sourcefilename2 = os.path.abspath(sourcefilename2)

                if not os.path.exists(temppath):
                    os.makedirs(temppath)

                os.chdir(temppath)


                if self.usingrestapi:
                    # The native API doesn't support downloading a single file.
                    if labserverfilename in self.stc._stc.files():                    
                        # The DB file is in the subdirectory.
                        self.stc._stc.download(labserverfilename)
                    elif filename in self.stc._stc.files():           
                        # It's in the root directory.         
                        self.stc._stc.download(filename)
                    else:
                        # Unable to find the DB file.
                        errmsg = "Unable to locate the results DB file '" + filename + "' on the lab server. Available files are: " + str(self.stc._stc.files())
                        logging.error(errmsg)
                        raise Exception(errmsg)                
                else:
                    # NOTE: This command becomes VERY slow when there are a large number of DB files to download.
                    # Download all files from the Lab Server into the temporary directory.
                    # The native API doesn't support downloading single files.
                    self.stc.perform("cssynchronizefiles")



                # Now move the DB file from the temporary directory and put it into 
                # the specified directory (path).
                if not os.path.exists(path):
                    os.makedirs(path)                

                targetfilename = os.path.join(path, filename)

                if not os.path.isfile(sourcefilename):
                    # Search for the file in the secondary location.
                    sourcefilename = sourcefilename2                

                if os.path.isfile(sourcefilename):
                    # This is the move function.
                    #os.rename(sourcefilename, targetfilename)
                    shutil.move(sourcefilename, targetfilename)

                    filename = targetfilename

                else:
                    # Something went wrong. Spare the temporary results directory for debugging.
                    errmsg = "Unable to locate the results DB file '" + filename + "' (" + sourcefilename + ")."
                    logging.error(errmsg)
                    raise Exception(errmsg)                

            except Exception as errmsg:                
                filename = None
                erroroutput = "WARNING: Something went wrong while downloading the results DB.\n" + str(errmsg)
                self.__lprint(erroroutput)
                pass

            os.chdir(originalpath)

        return(filename)

    #==============================================================================
    def getResultsDictFromDb(self, resultsdatabase, mode="FLOW", datasetid=None):
        """Generates a result view from the specified Spirent TestCenter End-of-Test results (sqlite) database.

        NOTE: Using duplicate Port and/or StreamBlock names will definitely mess up the results, which are stored
              in a dictionary that uses those names as its keys.
        
        Parameters
        ----------
        resultsdatabase : str
            The filename of the Sqlite EoT results database.
        mode : str
            The level of aggregation for results: FLOW, STREAM or STREAMBLOCK.
        datasetid : int
            Specifies which results dataset to process. Defaults to the latest data.
            This is not normally used.
        
        Returns
        -------
        dict
            A dictionary that contains results.

            When mode is set to "FLOW", the dictionary has the following keys:

            { <StreamBlockName>: { <StreamId:Comp1:Comp2:Comp3:Comp4>: { '<RxPortName': { <Stats> } } } }

            When mode is set to "STREAM", the dictionary has the following keys:

            { <StreamBlockName>: { <StreamId>: { '<RxPortName': { <Stats> } } } }

            When mode is set to "STREAMBLOCK", the dictionary has the following keys:

            { <StreamBlockName>: { '<RxPortName': { <Stats> } } }

            The following <Stats> keys are provided for all three modes:

              'ApiStreamBlockHandle'                          
              'ApiTxPortHandle'                               
              'Comp16_1'                                       
              'Comp16_2'                                       
              'Comp16_3'                                       
              'Comp16_4'                                       
              'DataSetId'                                      
              'DuplicateFrameCount'                            
              'FilteredValue_1'                                
              'FilteredValue_10'                               
              'FilteredValue_2'                                
              'FilteredValue_3'                                
              'FilteredValue_4'                                
              'FilteredValue_5'                                
              'FilteredValue_6'                                
              'FilteredValue_7'                                
              'FilteredValue_8'                                
              'FilteredValue_9'                                
              'First TimeStamp'                                
              'FlowCount'                                      
              'Frames Delta'                                   
              'IsExpectedPort'                                 
              'Last TimeStamp'                                 
              'Loss %'                                         
              'Packet Loss Duration (ms)'                      
              'ParentStreamBlock'                              
              'Rx Bytes'                                       
              'Rx Expected Frames'                             
              'Rx Frame Rate'                                  
              'Rx Frames'                                      
              'Rx Port'                                        
              'Rx Port Location'                               
              'Rx Rate (Bps)'                                  
              'Rx Rate (Kbps)'                                 
              'Rx Rate (Mbps)'                                 
              'Rx Rate (bps)'                                  
              'RxFilterList'                                   
              'RxPortHandle'                                   
              'Store-Forward Avg Latency (ns)'                 
              'Store-Forward Max Latency (ns)'                 
              'Store-Forward Min Latency (ns)'                 
              'StreamBlockName'                                     
              'StreamId'                                            
              'Tx Bytes'                                            
              'Tx Frame Rate'                                       
              'Tx Frames'                                           
              'Tx Port'                                             
              'Tx Port Location'                                    
              'Tx Rate (Bps)'                                       
              'Tx Rate (Kbps)'                                      
              'Tx Rate (Mbps)'                                      
              'Tx Rate (bps)'                                       
              'TxPortHandle'                                        
              'TxStreamId'                                          

            Also included are the "DataMining" fields specified by the user:
            e.g.
              'StreamBlock.FrameConfig.ipv4:IPv4.1.destAddr'        
              'StreamBlock.FrameConfig.ipv4:IPv4.1.sourceAddr'      
              'StreamBlock.FrameConfig.ipv4:IPv4.1.tosDiffserv.tos' 

        """

        logging.info("Executing getResultsDictFromDb: " + str(locals()))
        
        conn = sqlite3.connect(resultsdatabase)
        db = conn.cursor()

        if not datasetid:
            # The datasetid was not specified. Determine the ID of the latest set of results.
            # All queries will need to use this ID so that we are not pulling results
            # from different tests.
            datasetid = self.__getLatestDataSetId(db)

        # We need the Tx rate per stream for some of our calculations.    
        # If available, we will use the data mining field "fps per stream" (ideal). Otherwise, we will need
        # to calculate it from the port rates (less accurate).
        
        # Find the FpsLoad for each port. This is plan-B if the data-mining rate is not available.
        query = "SELECT \
                    Port.Handle, \
                    Port.Location, \
                    GenC.SchedulingMode, \
                    GenC.FpsLoad, \
                    GenC.DataSetId \
                 FROM \
                    GeneratorConfig As GenC \
                 LEFT JOIN Generator As Gen  ON GenC.ParentHnd = Gen.Handle \
                 LEFT JOIN Port      ON Gen.ParentHnd  = Port.Handle \
                 WHERE \
                    GenC.DataSetId = " + str(datasetid) + " AND Gen.DataSetId = " + str(datasetid) + " AND Port.DataSetId = " + str(datasetid)
        
        db.execute(query)
        
        portconfig = defaultdict(dict)
        for row in db.fetchall():
            port = row[0]                    
            portconfig[port]['location'] = row[1]
            portconfig[port]['mode']     = row[2]
            portconfig[port]['fps']      = row[3]            

            # Determine the number of streams for the port.
            query = "SELECT sum(StreamBlock.StreamCount) FROM StreamBlock WHERE StreamBlock.ParentHnd = " + str(port) + " AND DataSetId = " + str(datasetid)
            db.execute(query)
            portconfig[port]['streamcount'] = db.fetchone()[0]        

        # Add the custom SQLite tables that make it easier to parse the results.        
        self.__addTxRxEotStreamCustomResultsTable(db, datasetid)

        # Create a dictionary with the API handles for all objects. This will allow us to convert DB object handles to API object handles.
        db.execute("SELECT * FROM HandleMap")        
        handlemap = defaultdict(dict)
        for row in db.fetchall():
            handlemap[row[1]] = row[0]

        # Now, extract the stream results per port from the database and add it to the results dictionary.
        query = "SELECT * FROM TxRxEotStreamCustomResults WHERE DataSetId = " + str(datasetid)
        
        resultsdict   = defaultdict(dict)
        flowremainder = defaultdict(dict)
       
        db.execute(query)
        description = db.description
        for row in db.fetchall():
            # Each row represents a single flow result.
            # Some of the statistics are stored in the results database, and the rest we need to calculate.
            # Start by dealing with the extracted results.
            results = self.__getResultsAsDict(row, description)

            # Skip all streams that did not transmit any frames.
            if results['Tx Frames'] < 1:
                continue          
            
            txport = results['TxPortHandle']        
            rxport = results['RxPortHandle']
            
            if results['StreamId'] < 1:
                # Use the TxStreamId instead.
                results['StreamId'] = results['TxStreamId']

            # If the user has defined analyzer filters, which changes the hashing algorithm on the Rx port,
            # we need to determine how many Rx flows were created for that one stream on that Rx port.
            # All of the Tx counts and rates will need to be divided by the number of flows generated.            
            if rxport != "N/A":
                rxflowcount = results['RxFlowCount']
            else:
                rxflowcount = 1

            # The user should not see this internal field.
            results.pop('RxFlowCount', None)

            # Make corrections if there are multiple Rx flows per stream on this port.
            if 'StreamBlock.Rate.Fps' in results:
                results['StreamBlock.Rate.Fps'] = results['StreamBlock.Rate.Fps'] / rxflowcount            

            txframes = results['Tx Frames']
            results['Tx Frames'] = int(results['Tx Frames'] / rxflowcount)
            results['Tx Bytes']  = int(results['Tx Bytes']  / rxflowcount)

            if rxflowcount > 1:
                # Attempt to account for roundoff error.
                # NOTE: Since we cannot guarantee which flows are received first on the receiver, we cannot 100% determine
                #       which Rx flows need to have an additional frame added to their Tx count.
                streamkey = str(rxport) + "." + str(results['StreamId'])
                if streamkey in flowremainder:
                    flowremainder[streamkey] += 1
                else:
                    flowremainder[streamkey] = 1
                if txframes % rxflowcount >= flowremainder[streamkey]:
                    results['Tx Frames'] += 1
            # End multiple flow corrections.

            # Add all of the statistics from the SQLite query to the flowresult dictionary.
            flowresult = defaultdict(dict)
            flowresult['Tx Port Location'] = portconfig[txport]['location']

            if rxport != "N/A":
                flowresult['Rx Port Location'] = portconfig[rxport]['location']
            else:
                flowresult['Rx Port Location'] = "N/A"

            for key in results.keys():
                flowresult[key] = results[key]
                
                # Look for data-mining fields (they start with "StreamBlock"). Convert to human-readable values if necessary.
                # Only worry about non-empty values.
                if key.startswith("StreamBlock.") and results[key]: 

                    # Convert the hex value to an IPv4, IPv6 or MAC address if necessary.                                
                    if re.search(r'StreamBlock\.FrameConfig\.ipv4:IPv4\.[0-9]+\..+Addr', key):
                        # Convert the IPv4 hex string to an integer.
                        value = int(results[key], 16)
                        # Convert the integer to an IPv6 address.
                        flowresult[key] = str(IPAddress(value))
                    elif re.search(r'StreamBlock\.FrameConfig\.ipv6:IPv6\.[0-9]+\..+Addr', key):
                        # Convert the IPv6 hex string to an integer.
                        value = int(results[key], 16)
                        # Convert the integer to an IPv6 address.
                        flowresult[key] = str(IPAddress(value))
                    elif re.search(r'StreamBlock\.FrameConfig\.ethernet:EthernetII\.[0-9]+\..+Mac', key):
                        # Convert the MAC hex string to an integer.
                        value = int(results[key], 16)
                        # Convert the integer to a MAC address (00-01-02-03-04-05).
                        flowresult[key] = str(EUI(value))
            
            # Determine the Tx rate (in FPS) and duration.
            if 'StreamBlock.Rate.Fps' not in results:
                # The Tx FPS per stream information is not present (data-mining).
                # We need to attempt to calculate the Tx duration and Tx rate (FPS) manually, based on the configuration data (not the result data).
                if portconfig[txport]['mode'] == "RATE_BASED":
                    # The rate is specified per streamblock. Extract the load from the streamblock.
                    streamblock = results['ParentStreamBlock']
                    query = "SELECT FpsLoad, StreamCount FROM StreamBlock WHERE Handle = " + str(streamblock)
                    db.execute(query)
                    streamblockinfo = db.fetchone()
                    streamblocktxfps = streamblockinfo[0]
                    streamcount      = streamblockinfo[1]

                    txfps = streamblocktxfps / streamcount
                    # Correct for multiple analyzer flows.
                    txfps = txfps / rxflowcount             
                    txduration = results['Tx Frames'] / txfps

                elif portconfig[txport]['mode'] == "PORT_BASED":                
                    # The rate is specified per port.

                    # WARNING: I know this isn't working. The GeneratorConfig.FpsLoad field does NOT give you the correct value AND
                    #          the streamcount doesn't take into account streamblocks that are inactive.

                    txduration  = results['Tx Frames'] * portconfig[txport]['streamcount'] / portconfig[txport]['fps']
                    txfps       = portconfig[txport]['fps'] / portconfig[txport]['streamcount']

                else:
                    # We don't support other modes. 
                    # You COULD ignore this error, but all rate calculations would be incorrect.
                    errmsg = "ERROR: " + str(portconfig[txport]['mode']) + " scheduling is not supported. Rate calculations will be incorrect."
                    logging.error(errmsg)
                    raise ValueError(errmsg)
            else:
                # Pull the Tx rate directly from the Tx stream results (data-mining).
                txduration  = results['Tx Frames'] / results['StreamBlock.Rate.Fps']
                # This rate is PER STREAM...even though it looks like it should be per streamblock.
                txfps       = results['StreamBlock.Rate.Fps']                               

            # Calculate the L1 byte rate.
            if txduration == 0:
                # Avoid a divide-by-zero error.
                l1txbytespersecond = 0
            else:
                overhead           = results['Tx Frames'] * (12 + 8)  ;# 12 byte min IFG + 8 byte preamble
                l1txbytespersecond = (results['Tx Bytes'] + overhead) / txduration
            
            # Attempt to use the first/last Rx timestamps to calculate the Rx rate (VERY ACCURATE).
            # The timestamps are in microseconds. This will be used to calculate a number of stats.
            # This only works if the first/last timestamps are populated. 
            # I believe only the JITTER, LATENCY_JITTER, FORWARDING, and INTERARRIVALTIME modes support these stats,
            # however, this might be module dependent.
            rxtime = (results['Last TimeStamp'] - results['First TimeStamp']) / 1000000

            if rxtime <= 0:
                # Using the timestamps didn't work for some reason.
                # Make a best-guess at the Rx rate. It will not be as accurate as using the timestamps method.
                rxtime = results['Rx Frames'] / txfps

            if rxtime != 0:
                rxfps              = results['Rx Frames'] / rxtime
                overhead           = results['Rx Frames'] * (12 + 8) ;# 12 byte min IFG + 8 byte preamble
                l1rxbytespersecond = (results['Rx Bytes'] + overhead) / rxtime
            else:
                # Avoid a divide-by-zero error.
                rxfps              = 0
                l1rxbytespersecond = 0
            
            # The "IsExpectedPort" field is only valid when frames have been received on a port.
            # If the traffic was completely dropped, then the "IsExpectedPort" will report 0.
            if (results['Rx Frames'] > 0 and results['IsExpectedPort']) or results['Rx Frames'] == 0:
                flowresult['Rx Expected Frames'] = results['Tx Frames']
            else:
                flowresult['Rx Expected Frames'] = 0
            
            flowresult['Tx Frame Rate']  = txfps
            flowresult['Tx Rate (Bps)']  = l1txbytespersecond
            flowresult['Tx Rate (bps)']  = l1txbytespersecond * 8
            flowresult['Tx Rate (Kbps)'] = l1txbytespersecond * 8 / 1000
            flowresult['Tx Rate (Mbps)'] = l1txbytespersecond * 8 / 1000000
            
            # NOT taking into account duplicate frames.
            #flowresult['Frames Delta']  =  results['Tx Frames'] - results['Rx Frames'] + results['DuplicateFrameCount']
            flowresult['Frames Delta']   = abs(flowresult['Rx Expected Frames'] - results['Rx Frames'])
            flowresult['Rx Frame Rate']  = rxfps
            flowresult['Rx Rate (Bps)']  = l1rxbytespersecond
            flowresult['Rx Rate (bps)']  = l1rxbytespersecond * 8
            flowresult['Rx Rate (Kbps)'] = l1rxbytespersecond * 8 / 1000
            flowresult['Rx Rate (Mbps)'] = l1rxbytespersecond * 8 / 1000000
            
            if flowresult['Rx Expected Frames'] == 0:
                loss = 0
            else:
                loss = flowresult['Frames Delta'] * 100.0 / flowresult['Rx Expected Frames']

            flowresult['Loss %']                    = loss
            flowresult['Packet Loss Duration (ms)'] = txduration * flowresult['Loss %'] * 1000 / 100.0
            
            if flowresult['DuplicateFrameCount'] == "":
                flowresult['DuplicateFrameCount'] = 0

            sb = flowresult['StreamBlockName']
            rxportname = flowresult['Rx Port']

            # Add the API handles to the results. This is just for convenience. Just use the existing
            # value if we are unable find the corresponding API handle.
            flowresult['ApiStreamBlockHandle'] = handlemap.get(flowresult['ParentStreamBlock'], "N/A")
            flowresult['ApiTxPortHandle']      = handlemap.get(flowresult['TxPortHandle'], "N/A")
            flowresult['ApiRxPortHandle']      = handlemap.get(flowresult['RxPortHandle'], "N/A")

            # Transfer the individual flow result to the return resultsdict.
            
            # The following code is for aggregating the results.
            # If the mode is "FLOW", no aggregation is performed.
            # If the mode is "STREAM" or "STREAMBLOCK", then the results are aggregated at that level.

            #txid = str(results['TxStreamId'])

            if mode.upper() == "FLOW":                        
                # Store the flow result in the resultsdict array.  
                # We need to store each flow in the dictionary using a unique key.
                # The unique key is created from the all of the CompX values.            
                key  = str(results['StreamId']) + "." 
                key += str(results['Comp16_1']) + "." 
                key += str(results['Comp16_2']) + "." 
                key += str(results['Comp16_3']) + "." 
                key += str(results['Comp16_4'])

                # Warn the user if this entry already exists.
                if sb in resultsdict and key in resultsdict[sb] and rxportname in resultsdict[sb][key]:
                    # This case should not occur. If it does, we may need to reconsider the (supposedly) unique 
                    # dictionary keys (streamblock:key:rxportname) that we are using.
                    errmsg = "WARNING: The flow " + sb + ":" + key + ":" + rxportname + " already exists. The results may be missing an entry."
                    self.__lprint(errmsg)
                
                resultsdict[sb][key] = defaultdict(dict)
                resultsdict[sb][key][rxportname] = flowresult
                resultsdict[sb][key][rxportname]['FlowCount'] = 1
                
            elif mode.upper() == "STREAM":
                # Aggregate the flow results (per RxPort) for this stream or streamblock.
                streamid = results['StreamId']

                if streamid not in resultsdict[sb].keys():
                    resultsdict[sb][streamid] = defaultdict(dict)

                    # Create a new entry for this stream.                    
                    resultsdict[sb][streamid][rxportname] = flowresult 
                    resultsdict[sb][streamid][rxportname]['FlowCount'] = 1

                elif rxportname not in resultsdict[sb][streamid].keys():
                    resultsdict[sb][streamid][rxportname] = flowresult 
                    resultsdict[sb][streamid][rxportname]['FlowCount'] = 1                                        
                else:
                    resultsdict[sb][streamid][rxportname]['Tx Frames']           += flowresult['Tx Frames']
                    resultsdict[sb][streamid][rxportname]['Tx Bytes']            += flowresult['Tx Bytes']
                    resultsdict[sb][streamid][rxportname]['Rx Frames']           += flowresult['Rx Frames']
                    resultsdict[sb][streamid][rxportname]['Rx Bytes']            += flowresult['Rx Bytes']
                    resultsdict[sb][streamid][rxportname]['DuplicateFrameCount'] += flowresult['DuplicateFrameCount']
                                
                    resultsdict[sb][streamid][rxportname]['First TimeStamp'] = min(resultsdict[sb][streamid][rxportname]['First TimeStamp'], flowresult['First TimeStamp'])                
                    resultsdict[sb][streamid][rxportname]['Last TimeStamp']  = max(resultsdict[sb][streamid][rxportname]['Last TimeStamp'],  flowresult['Last TimeStamp'])
                    
                    # Sum the latency. We'll divide it by the total flow count later.
                    resultsdict[sb][streamid][rxportname]['Store-Forward Avg Latency (ns)'] += flowresult['Store-Forward Avg Latency (ns)']
                                    
                    resultsdict[sb][streamid][rxportname]['Store-Forward Min Latency (ns)'] = min(resultsdict[sb][streamid][rxportname]['Store-Forward Min Latency (ns)'], flowresult['Store-Forward Min Latency (ns)'])
                    resultsdict[sb][streamid][rxportname]['Store-Forward Max Latency (ns)'] = max(resultsdict[sb][streamid][rxportname]['Store-Forward Max Latency (ns)'], flowresult['Store-Forward Max Latency (ns)'])
                    
                    resultsdict[sb][streamid][rxportname]['Tx Frame Rate']      += flowresult['Tx Frame Rate']
                    resultsdict[sb][streamid][rxportname]['Tx Rate (Bps)']      += flowresult['Tx Rate (Bps)']
                    resultsdict[sb][streamid][rxportname]['Tx Rate (bps)']      += flowresult['Tx Rate (bps)']
                    resultsdict[sb][streamid][rxportname]['Tx Rate (Kbps)']     += flowresult['Tx Rate (Kbps)']
                    resultsdict[sb][streamid][rxportname]['Tx Rate (Mbps)']     += flowresult['Tx Rate (Mbps)']
                    resultsdict[sb][streamid][rxportname]['Rx Expected Frames'] += flowresult['Rx Expected Frames']
                    resultsdict[sb][streamid][rxportname]['Frames Delta']       += flowresult['Frames Delta']
                    resultsdict[sb][streamid][rxportname]['Rx Frame Rate']      += flowresult['Rx Frame Rate']
                    resultsdict[sb][streamid][rxportname]['Rx Rate (Bps)']      += flowresult['Rx Rate (Bps)']
                    resultsdict[sb][streamid][rxportname]['Rx Rate (bps)']      += flowresult['Rx Rate (bps)']
                    resultsdict[sb][streamid][rxportname]['Rx Rate (Kbps)']     += flowresult['Rx Rate (Kbps)']
                    resultsdict[sb][streamid][rxportname]['Rx Rate (Mbps)']     += flowresult['Rx Rate (Mbps)']
                    
                    if resultsdict[sb][streamid][rxportname]['Rx Expected Frames'] == 0:
                        # Avoid a divide-by-zero error.
                        loss = 0
                    else:
                        loss = resultsdict[sb][streamid][rxportname]['Frames Delta'] * 100.0 / resultsdict[sb][streamid][rxportname]['Rx Expected Frames']

                    resultsdict[sb][streamid][rxportname]['Loss %']                    = loss
                    resultsdict[sb][streamid][rxportname]['Packet Loss Duration (ms)'] = txduration * resultsdict[sb][streamid][rxportname]['Loss %'] * 1000 / 100.0                                        

                    resultsdict[sb][streamid][rxportname]['FlowCount'] += 1       

            elif mode.upper() == "STREAMBLOCK":

                if rxportname not in resultsdict[sb]:
                    resultsdict[sb][rxportname] = defaultdict(dict)

                    # Create a new entry for this [rxport][stream/streamblock].                
                    resultsdict[sb][rxportname] = flowresult 
                    resultsdict[sb][rxportname]['FlowCount'] = 1

                else:
                    resultsdict[sb][rxportname]['Tx Frames']           += flowresult['Tx Frames']
                    resultsdict[sb][rxportname]['Tx Bytes']            += flowresult['Tx Bytes']
                    resultsdict[sb][rxportname]['Rx Frames']           += flowresult['Rx Frames']
                    resultsdict[sb][rxportname]['Rx Bytes']            += flowresult['Rx Bytes']
                    resultsdict[sb][rxportname]['DuplicateFrameCount'] += flowresult['DuplicateFrameCount']
                                
                    resultsdict[sb][rxportname]['First TimeStamp'] = min(resultsdict[sb][rxportname]['First TimeStamp'], flowresult['First TimeStamp'])                
                    resultsdict[sb][rxportname]['Last TimeStamp']  = max(resultsdict[sb][rxportname]['Last TimeStamp'],  flowresult['Last TimeStamp'])
                    
                    # Sum the latency. We'll divide it by the total flow count later.
                    resultsdict[sb][rxportname]['Store-Forward Avg Latency (ns)'] += flowresult['Store-Forward Avg Latency (ns)']
                                    
                    resultsdict[sb][rxportname]['Store-Forward Min Latency (ns)'] = min(resultsdict[sb][rxportname]['Store-Forward Min Latency (ns)'], flowresult['Store-Forward Min Latency (ns)'])
                    resultsdict[sb][rxportname]['Store-Forward Max Latency (ns)'] = max(resultsdict[sb][rxportname]['Store-Forward Max Latency (ns)'], flowresult['Store-Forward Max Latency (ns)'])
                    
                    resultsdict[sb][rxportname]['Tx Frame Rate']      += flowresult['Tx Frame Rate']
                    resultsdict[sb][rxportname]['Tx Rate (Bps)']      += flowresult['Tx Rate (Bps)']
                    resultsdict[sb][rxportname]['Tx Rate (bps)']      += flowresult['Tx Rate (bps)']
                    resultsdict[sb][rxportname]['Tx Rate (Kbps)']     += flowresult['Tx Rate (Kbps)']
                    resultsdict[sb][rxportname]['Tx Rate (Mbps)']     += flowresult['Tx Rate (Mbps)']
                    resultsdict[sb][rxportname]['Rx Expected Frames'] += flowresult['Rx Expected Frames']
                    resultsdict[sb][rxportname]['Frames Delta']       += flowresult['Frames Delta']
                    resultsdict[sb][rxportname]['Rx Frame Rate']      += flowresult['Rx Frame Rate']
                    resultsdict[sb][rxportname]['Rx Rate (Bps)']      += flowresult['Rx Rate (Bps)']
                    resultsdict[sb][rxportname]['Rx Rate (bps)']      += flowresult['Rx Rate (bps)']
                    resultsdict[sb][rxportname]['Rx Rate (Kbps)']     += flowresult['Rx Rate (Kbps)']
                    resultsdict[sb][rxportname]['Rx Rate (Mbps)']     += flowresult['Rx Rate (Mbps)']
                    
                    if resultsdict[sb][rxportname]['Rx Expected Frames'] == 0:
                        # Avoid a divide-by-zero error.
                        loss = 0
                    else:
                        loss = resultsdict[sb][rxportname]['Frames Delta'] * 100.0 / resultsdict[sb][rxportname]['Rx Expected Frames']

                    resultsdict[sb][rxportname]['Loss %']                    = loss
                    resultsdict[sb][rxportname]['Packet Loss Duration (ms)'] = txduration * resultsdict[sb][rxportname]['Loss %'] * 1000 / 100.0                                        

                    resultsdict[sb][rxportname]['FlowCount'] += 1                
            else:
                errmsg = "The results mode '" + mode + "' is invalid."
                logging.error(errmsg)
                raise Exception(errmsg)

        # Lastly, we need to calculate averages, correct Tx counts for Rx flows belonging to the same stream, as well as clean up some labels.
        # Keep a list of all Rx (analyzer) filters.
        rxfilterlist = []
        if mode.upper() == "FLOW" or mode.upper() == "STREAM":
            for sb in resultsdict.keys():
                for key in resultsdict[sb].keys():
                    for rxport in resultsdict[sb][key].keys():

                        if rxport == "N/A":
                            continue

                        # Determine the correct label to substitute for the "FilteredName_X" keys.
                        # This information is in the RxEotAnalyzerFilterNamesTable table, and is ONLY 
                        # valid if the Rx port has an analyzer filter defined.
                        rxportobject = resultsdict[sb][key][rxport]['RxPortHandle']
                        query = "SELECT * FROM RxEotAnalyzerFilterNamesTable WHERE ParentHnd = " + str(rxportobject)   
                        db.execute(query)
                        for row in db.fetchall():
                            # The filters are labeled FilteredName_1 through FilteredName_10.
                            for index in range(1,11):
                                newlabel = row[index + 3]
                                oldlabel = "FilteredValue_" + str(index)

                                if newlabel:
                                    # Add the correct filtername to each flow/stream/streamblock entry in the results.                            
                                    resultsdict[sb][key][rxport][newlabel] = resultsdict[sb][key][rxport][oldlabel]

                                    if newlabel not in rxfilterlist:
                                        rxfilterlist.append(newlabel)                        
                                else:
                                    # Trim off unused filters from the dictionary.
                                    resultsdict[sb][key][rxport].pop(oldlabel)  

                        # Calculate the average latency.
                        latency   = resultsdict[sb][key][rxport]['Store-Forward Avg Latency (ns)']
                        flowcount = resultsdict[sb][key][rxport]['FlowCount']
                        resultsdict[sb][key][rxport]['Store-Forward Avg Latency (ns)'] = latency / flowcount
                        
                        # Add the RxFilterList to each entry in the results dictionary.
           
                        # The flow lable is a little misleading. It could be a flow, stream or streamblock.
                        resultsdict[sb][key][rxport]['RxFilterList'] = rxfilterlist

                        # Make sure all filters are listed for all flows.
                        for rxfilter in rxfilterlist:
                            if rxfilter not in resultsdict[sb][key][rxport].keys():
                                resultsdict[sb][key][rxport][rxfilter] = ""


        elif mode.upper() == "STREAMBLOCK":
            for sb in resultsdict.keys():                
                for rxport in resultsdict[sb].keys():

                    if rxport == "N/A":
                        continue

                    # Determine the correct label to substitute for the "FilteredName_X" keys.
                    # This information is in the RxEotAnalyzerFilterNamesTable table, and is ONLY 
                    # valid if the Rx port has an analyzer filter defined.
                    rxportobject = resultsdict[sb][rxport]['RxPortHandle']
                    query = "SELECT * FROM RxEotAnalyzerFilterNamesTable WHERE ParentHnd = " + str(rxportobject)   
                    db.execute(query)
                    for row in db.fetchall():
                        # The filters are labeled FilteredName_1 through FilteredName_10.
                        for index in range(1,11):
                            newlabel = row[index + 3]
                            oldlabel = "FilteredValue_" + str(index)

                            if newlabel:
                                # Add the correct filtername to each flow/stream/streamblock entry in the results.                            
                                resultsdict[sb][rxport][newlabel] = resultsdict[sb][rxport][oldlabel]

                                if newlabel not in rxfilterlist:
                                    rxfilterlist.append(newlabel)                        
                            else:
                                # Trim off unused filters from the dictionary.
                                resultsdict[sb][rxport].pop(oldlabel)  

                    # Calculate the average latency.
                    latency   = resultsdict[sb][rxport]['Store-Forward Avg Latency (ns)']
                    flowcount = resultsdict[sb][rxport]['FlowCount']
                    resultsdict[sb][rxport]['Store-Forward Avg Latency (ns)'] = latency / flowcount
                    
                    # Add the RxFilterList to each entry in the results dictionary.
       
                    # The flow lable is a little misleading. It could be a flow, stream or streamblock.
                    resultsdict[sb][rxport]['RxFilterList'] = rxfilterlist

                    # Make sure all filters are listed for all flows.
                    for rxfilter in rxfilterlist:
                        if rxfilter not in resultsdict[sb][rxport].keys():
                            resultsdict[sb][rxport][rxfilter] = ""                                                                       
                    
        # We are done with the database.
        conn.close()

        return(resultsdict)        

    #==============================================================================
    def getPortResultsDictFromDb(self, resultsdatabase, datasetid=None):
        """Generates a port result view from the specified Spirent TestCenter End-of-Test results (sqlite) database.

        NOTE: Using duplicate Port names will definitely mess up the results, which are stored in a dictionary 
              that uses those names as its keys.
        
        Parameters
        ----------
        resultsdatabase : str
            The filename of the Sqlite EoT results database.
        datasetid : int
            Specifies which results dataset to process. Defaults to the latest data.
            This is not normally used.
        
        Returns
        -------
        dict
            A dictionary that contains results.

            { <PortName>: { 'ApiHandle': 'port3',
                            'DbHandle': 2532,
                            'GeneratorStatus': 'STOPPED',
                            'IsVirtual': 0,
                            'Location': '//10.140.96.81/5/6',
                            'Tx': { <TxStats> },
                            'Rx': { <RxStats> } } }

            The following <Stats> keys are provided:
            Tx
                'AlarmState'                              
                'CounterTimestamp'                        
                'GeneratorAbortFrameCount'                
                'GeneratorAbortFrameRate'                 
                'GeneratorBitRate'                        
                'GeneratorCrcErrorFrameCount'             
                'GeneratorCrcErrorFrameRate'              
                'GeneratorFrameCount'                     
                'GeneratorFrameRate'                      
                'GeneratorIpv4FrameCount'                 
                'GeneratorIpv4FrameRate'                  
                'GeneratorIpv6FrameCount'                 
                'GeneratorIpv6FrameRate'                  
                'GeneratorJumboFrameCount'                
                'GeneratorJumboFrameRate'                 
                'GeneratorL3ChecksumErrorCount'           
                'GeneratorL3ChecksumErrorRate'            
                'GeneratorL4ChecksumErrorCount'           
                'GeneratorL4ChecksumErrorRate'            
                'GeneratorMplsFrameCount'                 
                'GeneratorMplsFrameRate'                  
                'GeneratorOctetCount'                     
                'GeneratorOctetRate'                      
                'GeneratorOversizeFrameCount'             
                'GeneratorOversizeFrameRate'              
                'GeneratorSigFrameCount'                  
                'GeneratorSigFrameRate'                   
                'GeneratorUndersizeFrameCount'            
                'GeneratorUndersizeFrameRate'             
                'GeneratorVlanFrameCount'                 
                'GeneratorVlanFrameRate'                  
                'HwFrameCount'                            
                'L1BitCount'                              
                'L1BitRate'                               
                'L1BitRatePercent'                        
                'LastModified'                            
                'PcTimestamp'                             
                'PfcFrameCount'                           
                'PfcPri0FrameCount'                       
                'PfcPri1FrameCount'                       
                'PfcPri2FrameCount'                       
                'PfcPri3FrameCount'                       
                'PfcPri4FrameCount'                       
                'PfcPri5FrameCount'                       
                'PfcPri6FrameCount'                       
                'PfcPri7FrameCount'                       
                'RateTimestamp'                           
                'ResultState'                             
                'TotalBitCount'                           
                'TotalBitRate'                            
                'TotalCellCount'                          
                'TotalCellRate'                           
                'TotalFrameCount'                         
                'TotalFrameRate'                          
                'TotalIpv4FrameCount'                     
                'TotalIpv4FrameRate'                      
                'TotalIpv6FrameCount'                     
                'TotalIpv6FrameRate'                      
                'TotalMplsFrameCount'                     
                'TotalMplsFrameRate'                      
                'TotalOctetCount'                         
                'TotalOctetRate'                          
                'TxDuration'                              

            Rx
                'AlarmState'                             
                'AvgLatency'                             
                'ComboTriggerCount'                      
                'ComboTriggerRate'                       
                'CorrectedBaseRFecErrorCount'            
                'CorrectedRsFecErrorCount'               
                'CorrectedRsFecSymbols'                  
                'CounterTimestamp'                       
                'DroppedFrameCount'                      
                'DuplicateFrameCount'                    
                'FcoeFrameCount'                         
                'FcoeFrameRate'                          
                'FcsErrorFrameCount'                     
                'FcsErrorFrameRate'                      
                'FirstArrivalTime'                       
                'HwFrameCount'                           
                'IcmpFrameCount'                         
                'IcmpFrameRate'                          
                'InOrderFrameCount'                      
                'Ipv4ChecksumErrorCount'                 
                'Ipv4ChecksumErrorRate'                  
                'Ipv4FrameCount'                         
                'Ipv4FrameRate'                          
                'Ipv6FrameCount'                         
                'Ipv6FrameRate'                          
                'Ipv6OverIpv4FrameCount'                 
                'Ipv6OverIpv4FrameRate'                  
                'JumboFrameCount'                        
                'JumboFrameRate'                         
                'L1BitCount'                             
                'L1BitRate'                              
                'L1BitRatePercent'                       
                'LastArrivalTime'                        
                'LastModified'                           
                'LateFrameCount'                         
                'MaxFrameLength'                         
                'MaxLatency'                             
                'MinFrameLength'                         
                'MinLatency'                             
                'MplsFrameCount'                         
                'MplsFrameRate'                          
                'OutSeqFrameCount'                       
                'OversizeFrameCount'                     
                'OversizeFrameRate'                      
                'PauseFrameCount'                        
                'PauseFrameRate'                         
                'PcTimestamp'                            
                'PfcFrameCount'                          
                'PfcFrameRate'                           
                'PfcPri0FrameCount'                      
                'PfcPri0FrameRate'                       
                'PfcPri0Quanta'                          
                'PfcPri1FrameCount'                      
                'PfcPri1FrameRate'                       
                'PfcPri1Quanta'                          
                'PfcPri2FrameCount'                      
                'PfcPri2FrameRate'                       
                'PfcPri2Quanta'                          
                'PfcPri3FrameCount'                      
                'PfcPri3FrameRate'                       
                'PfcPri3Quanta'                          
                'PfcPri4FrameCount'                      
                'PfcPri4FrameRate'                       
                'PfcPri4Quanta'                          
                'PfcPri5FrameCount'                      
                'PfcPri5FrameRate'                       
                'PfcPri5Quanta'                          
                'PfcPri6FrameCount'                      
                'PfcPri6FrameRate'                       
                'PfcPri6Quanta'                          
                'PfcPri7FrameCount'                      
                'PfcPri7FrameRate'                       
                'PfcPri7Quanta'                          
                'PostBaseRFecSerRate'                    
                'PostRsFecSerRate'                       
                'PrbsBitErrorCount'                      
                'PrbsBitErrorRate'                       
                'PrbsBitErrorRatio'                      
                'PrbsErrorFrameCount'                    
                'PrbsErrorFrameRate'                     
                'PrbsFillOctetCount'                     
                'PrbsFillOctetRate'                      
                'PreBaseRFecSerRate'                     
                'PreRsFecSerRate'                        
                'PreambleMaxLength'                      
                'PreambleMinLength'                      
                'PreambleTotalBytes'                     
                'RateTimestamp'                          
                'ReorderedFrameCount'                    
                'ResultState'                            
                'SigFrameCount'                          
                'SigFrameRate'                           
                'TcpChecksumErrorCount'                  
                'TcpChecksumErrorRate'                   
                'TcpFrameCount'                          
                'TcpFrameRate'                           
                'TotalBitCount'                          
                'TotalBitRate'                           
                'TotalCellCount'                         
                'TotalCellRate'                          
                'TotalFrameCount'                        
                'TotalFrameRate'                         
                'TotalLatency'                           
                'TotalOctetCount'                        
                'TotalOctetRate'                         
                'Trigger1Count'                          
                'Trigger1Name'                           
                'Trigger1Rate'                           
                'Trigger2Count'                          
                'Trigger2Name'                           
                'Trigger2Rate'                           
                'Trigger3Count'                          
                'Trigger3Name'                           
                'Trigger3Rate'                           
                'Trigger4Count'                          
                'Trigger4Name'                           
                'Trigger4Rate'                           
                'Trigger5Count'                          
                'Trigger5Name'                           
                'Trigger5Rate'                           
                'Trigger6Count'                          
                'Trigger6Name'                           
                'Trigger6Rate'                           
                'Trigger7Count'                          
                'Trigger7Name'                           
                'Trigger7Rate'                           
                'Trigger8Count'                          
                'Trigger8Name'                           
                'Trigger8Rate'                           
                'UdpChecksumErrorCount'                  
                'UdpChecksumErrorRate'                   
                'UdpFrameCount'                          
                'UdpFrameRate'                           
                'UncorrectedBaseRFecErrorCount'          
                'UncorrectedRsFecErrorCount'             
                'UndersizeFrameCount'                    
                'UndersizeFrameRate'                     
                'UserDefinedFrameCount1'                 
                'UserDefinedFrameCount2'                 
                'UserDefinedFrameCount3'                 
                'UserDefinedFrameCount4'                 
                'UserDefinedFrameCount5'                 
                'UserDefinedFrameCount6'                 
                'UserDefinedFrameRate1'                  
                'UserDefinedFrameRate2'                  
                'UserDefinedFrameRate3'                  
                'UserDefinedFrameRate4'                  
                'UserDefinedFrameRate5'                  
                'UserDefinedFrameRate6'                  
                'VlanFrameCount'                         
                'VlanFrameRate'                          

        """

        logging.info("Executing getPortResultsDictFromDb: " + str(locals()))
        
        conn = sqlite3.connect(resultsdatabase)
        db = conn.cursor()

        if not datasetid:
            # The datasetid was not specified. Determine the ID of the latest set of results.
            # All queries will need to use this ID so that we are not pulling results
            # from different tests.
            datasetid = self.__getLatestDataSetId(db)
        
        # Create a dictionary with the API handles for all objects. This will allow us to convert DB object handles to API object handles.      
        db.execute("SELECT * FROM HandleMap")        
        handlemap = defaultdict(dict)
        for row in db.fetchall():
            handlemap[row[1]] = row[0]

        # Create a dictionary with all of the port information. The DB object is used as the dictionary key.
        db.execute("SELECT * FROM Port")        
        portinfo = defaultdict(dict)
        description = db.description
        for row in db.fetchall():
            results = self.__getResultsAsDict(row, description)            
            portinfo[results['Handle']] = results

        # Create a dictionary with all of the generator information. The DB object is used as the dictionary key.
        # I want this for one thing only...the generator state. This will tell us if the generator is running when
        # we gather these results.
        db.execute("SELECT State, ParentHnd FROM Generator")        
        generatorinfo = defaultdict(dict)
        for row in db.fetchall():            
            status = row[0]
            portobject = row[1]
            portinfo[portobject]['GeneratorStatus'] = status
        
        resultsdict = defaultdict(dict)

        # Start with the Generator results.
        query = "SELECT * FROM GeneratorPortResults WHERE DataSetId = " + str(datasetid)               
        db.execute(query)
        description = db.description
        for row in db.fetchall():
            results = self.__getResultsAsDict(row, description)
           
            portobject = results['ParentPort']                                
            portname = portinfo[portobject]['Name']            

            if portname in resultsdict.keys():
                self.__lprint("WARNING: There appears to be a duplicate port name '" + portname + "'. The results will be impacted.")

            resultsdict[portname] = defaultdict(dict)

            results.pop('Active')
            results.pop('DataSetId')
            results.pop('Handle')
            results.pop('Id')
            results.pop('LocalActive')
            results.pop('Name')
            results.pop('ParentHnd')
            results.pop('ParentPort')
            results.pop('PortUiName')

            resultsdict[portname]['Tx'] = results         

            resultsdict[portname]['ApiHandle'] = handlemap.get(portobject, "N/A")
            resultsdict[portname]['DbHandle'] = portinfo[portobject]['Handle']
            resultsdict[portname]['Location'] = portinfo[portobject]['Location']
            resultsdict[portname]['IsVirtual'] = portinfo[portobject]['IsVirtual']
            resultsdict[portname]['GeneratorStatus'] = portinfo[portobject]['GeneratorStatus']            

        # Now grab the Analyzer results.
        query = "SELECT * FROM AnalyzerPortResults WHERE DataSetId = " + str(datasetid)               
        db.execute(query)
        description = db.description
        for row in db.fetchall():
            results = self.__getResultsAsDict(row, description)
           
            portobject = results['ParentPort']                                
            portname = portinfo[portobject]['Name']

            # Remove some unnecessary keys from the results.
            results.pop('Active')
            results.pop('DataSetId')
            results.pop('Handle')
            results.pop('Id')
            results.pop('LocalActive')
            results.pop('Name')
            results.pop('ParentHnd')
            results.pop('ParentPort')
            results.pop('PortUiName')

            resultsdict[portname]['Rx'] = results            
                    
        # We are done with the database.
        conn.close()

        return(resultsdict)        

    #==============================================================================
    def generateCsv(self, resultsdb, prefix=None):
        """Generate a plain-text CSV file from the specified results database.

        The CSV file will be generated in the same directory as the results database.

        Parameters
        ----------
        resultsdb : str
            The filename of the source results database.
        prefix: str
            An optional prefix to add to the CSV results filename.

        """

        logging.info("Executing generateCsv: " + str(locals()))

        if not prefix:
            prefix = ""

        # Create some CSV files in the same directory as the results DB.
        path = os.path.dirname(resultsdb)

        result = self.getResultsDictFromDb(resultsdb)  

        #self.pp.pprint(result)
        for streamid in result.keys():
            for rxport in result[streamid].keys():        
                # Generate a new file for each rxport.

                if rxport != "N/A":

                    # Start with the header:
                    noheader = 1
                    header   = ""
                    body     = ""
                    for key in result[streamid][rxport].keys():            
                        if type(result[streamid][rxport][key]) is defaultdict:                
                            for label in sorted(result[streamid][rxport][key].keys()):
                                if noheader:
                                    header += str(label) + ", "
                                body += str(result[streamid][rxport][key][label]) + ", "
                                #print("    " + label + "=" + str(result[streamid][rxport][key][label]))
                            body += "\n"
                            noheader = 0

                    filename = str(prefix) + str(rxport) + ".csv"                    
                    fh = open(os.path.join(path, filename), 'w')
                    fh.write(header)
                    fh.write("\n")
                    fh.write(body)
                    fh.close()  # you can omit in most cases as the destructor will call it
        return            

    # #==============================================================================
    # def findObjectsByName(self, objecttype, objectname=""):
    #     """Finds the specified object types and returns their handles.

    #     Parameters
    #     ----------
    #     objecttype : str
    #         The type of object.
    #     objectname: str
    #         Optional: The name of the object. If not specified, all objects of that type are returned.

    #     """
    #     if objectname == "*" or objectname == "":
    #         result = self.stc.perform("GetObjects", ClassName=objecttype)        
    #     else:
    #         result = self.stc.perform("GetObjects", ClassName=objecttype, Condition="Name = " + objectname)        


    #     return(result['ObjectList'])

    #==============================================================================
    def getObject(self, name, objecttype=None):
        """Returns the object handle for the corresponding object name.

        If the name is not stored in the local "objects" dict, the API is searched.
        Returns "None" if the object is not found.

        Parameters
        ----------
        objectname: str
            The name of the object.

        objecttype : str
            Optional: The type of object. If this is not specified, no attempt will be made
            to search the API for the object. 

        Returns
        -------
        str
            Returns the object handle for the corresponding object, or None if not found.

        """

        if name not in self.objects.keys():                            
            # We didn't find the specified object in the dictionary. Search the API for it as well.
            objecthandlelist = []
            if objecttype:
                result = self.stc.perform("GetObjects", ClassName=objecttype, Condition="Name = " + name)        
                objecthandlelist = result['ObjectList'].split()

            if len(objecthandlelist) > 1:
                errmsg = "There is more than one object with the name '" + name + "'."
                logging.error(errmsg)
                raise Exception(errmsg)
            elif len(objecthandlelist) == 0:
                objecthandle = None
            else:
                self.objects[name] = objecthandlelist[0]
                objecthandle = objecthandlelist[0]
        else:
            objecthandle = self.objects[name]

        return(objecthandle)

    #==============================================================================
    def archiveDirectory(source):    
        """Zip the specified directory.
        
        Parameters
        ----------
        source: str
            The path to the directory that will be zipped.

        """
        archivename = shutil.make_archive(base_name=source, root_dir=source, format="zip")

        # Remove the results directory.
        #shutil.rmtree(source)    

        return(archivename)        

    #==============================================================================
    def cleanUpSession(self):        
        """Terminates the current Lab Server session. This is really only necessary when using the REST API. 
        """
        if self.labserverip:
            self.stc.perform("CsTestSessionDisconnect", Terminate=True)

        return

    #==============================================================================
    def cleanupTempDirectory(self):
        """Clean up the temporary log directory.        
        """

        if self.cleanuponexit:
            # Delete temporary session directory.
            self.__rmtree(self.logpath)
               
        return        

    #==============================================================================
    #
    #   Private Methods
    #
    #==============================================================================

    def __doesSessionExist(self, testsessionname, ownerid, action):
        """Allows the user to kill an existing Lab Server (if it exists). 

           It is safe to call this method for sessions that don't exist.

        Parameters
        ----------
        testsessionname : str
            The name of the session (not the session ID).
        ownerid: str
            The username for the session.

        Returns
        -------
        bool
            True if the session already exists. False if it doesn't, or was killed 
            by this method.

        """
        exists = False

        self.stc.perform("CSServerConnect", host=self.labserverip)

        for session in self.stc.get("system1.csserver", "children-CSTestSession").split():
            if self.stc.get(session, "name") == testsessionname + " - " + ownerid:
                # The session exists.
                if action.lower() == "kill":
                    # ...so kill it.
                    self.stc.perform("CSStopTestSession", TestSession=session)
                    self.stc.perform("CSDestroyTestSession", TestSession=session)
                    break
                else:
                    exists = True
                    break

        self.stc.perform("CSServerDisconnect")

        return(exists)


    #==============================================================================
    def __convertJsonToDict(self, inputfilename):        

        # Open and read the JSON input file.
        jsondict = {}

        try:
            with open(inputfilename) as json_file:
                jsondict = json.load(json_file)
        except:
            errmsg = "Unexpected error while parsing the JSON:", sys.exc_info()[1]
            logging.error(errmsg)
            raise Exception(errmsg)

        return jsondict

    #==============================================================================
    def __populateObjectsDict(self):
        # Populate the "objects" dictionary with existing object information.
        # This is vital for ports, but I'm going to add devices and streamblocks
        # as well (for performance).       

        for port in self.stc.get("system1.project", "children-port").split():
            self.stc.config(port, AppendLocationToPortName=False)

            # If necessary, remore " (offline)" from the name.
            name = self.stc.get(port, "name")
            name = re.sub(r' \(offline\)', '', name)

            self.objects[name] = port

            for streamblock in self.stc.get(port, "children-streamblock").split():
                name = self.stc.get(streamblock, "name")
                self.objects[name] = streamblock

        for device in self.stc.get("system1.project", "children-emulateddevice").split():
            name = self.stc.get(device, "name")
            self.objects[name] = device

        # print(self.__getObject("Port1"))
        # print(self.__getObject("Device 1"))


        # This is a recursive method to discover all child objects of the initial
        # object handle in the current BLL session. Their names and object handles 
        # are stored in the variable "objects".

        # if clearobjectsdict:
        #     del self.objects
        #     self.objects = {}

        # if objecthandle != "":
        #     name = self.stc.get(objecthandle, "name")

        #     if name == "":
        #         name = objecthandle
        #         self.stc.config(objecthandle, name=name)

        #     if re.match("port[0-9]+", objecthandle, flags=re.I):
        #         # This is a port object. We need to remove some extra garbage from 
        #         # the name.

        #         # If present, remove " (offline)" from the name.                
        #         name = re.sub(r' \(offline\)', '', name)

        #     if name in self.objects.keys():
        #         print("WARNING: the name " + name + " already exists.")

        #     self.objects[name] = objecthandle

        #     for child in self.stc.get(objecthandle, "children").split():
        #         self.__populateObjectsDict(child, clearobjectsdict=False)

        return

    #==============================================================================
    def __addObject(self, objectdict, parent=None):   
        # This is a recursive method for parsing the JSON/Dictionary configuration
        # and translating it into Spirent TestCenter objects.

        if not parent:
            parent = self.project

        if objectdict.get("DeleteExisting"):
            # The user would like to delete existing objects.
            # They can either specify specific objects by name, or they can specify object types.

            itemlist = objectdict.get("DeleteExisting")

            #objectattributes = objectdict.copy()
            del objectdict["DeleteExisting"]

            # Make sure item is a list.
            if not isinstance(itemlist, list):
                # Convert this string to a list.
                itemlist = [itemlist]

            for item in itemlist:
                item = item.lower()

                objecthandle = self.getObject(item)                

                if objecthandle:
                    # The item is referencing an object name. Delete this specific object.
                    self.stc.delete(objecthandle)                              
                else:
                    # Okay, so "item" is not an object name. Try treating it as an object type and
                    # delete all objects of this type.
                    #childlist = self.stc.get(parent, "children-" + item).split()

                    childlist = self.stc.perform("GetObjects", RootList=parent, ClassName=item)['ObjectList'].split()

                    parentobjecttype = self.stc.perform("GetObjectInfo", object=parent)["ObjectType"].lower()

                    if parentobjecttype == "port" and (item == "emulateddevice" or item == "device" or item == "router" or item == "host"):
                        # Add the relations as well.
                        childlist += self.stc.get(parent, "AffiliationPort-Sources").split()
                    
                    for objecthandle in childlist:
                        try:
                            self.stc.delete(objecthandle)
                        except Exception as ex:
                            pass

            # We need to purge the objects dictionary of all of the objects that were deleted.
            self.__purgeObjects()

        # Iterate through each key.
        for key in sorted(objectdict.keys()):
            # A new object will contain a dictionary for the "value", and that
            # dictionary will have the "ObjectType" key defined.
            objecttype = self.__getObjectType(key, objectdict[key])

            if objecttype:       

                # We found a new object. Make a copy of the objectdict and delete the "ObjectType" key.
                # If we don't make a copy of the dict, the "for" loop that we are in may fail.
                objectattributes = objectdict[key].copy()
                del objectattributes["ObjectType"]

                if objecttype.lower() == "streamblock":
                    object = self.createStreamBlock(port=parent, name=key, parametersdict=objectattributes)
                elif objecttype.lower() == "emulateddevice" or objecttype.lower() == "device" or objecttype.lower() == "router":
                    object = self.createDevice(parent, key, objectattributes)                    
                elif re.search("modifier", objecttype, flags=re.I):
                    # Delay the creation of modifiers until after all relations have been resolved. This will
                    # prevent a race condition for IPv6 streamblocks.
                    #object = self.createModifier(parent, objecttype, objectattributes)

                    # Concatenate the streamblock and modifier names. This will help ensure a unique key.
                    modifiername = parent + "." + key
                    
                    if modifiername in self.modifiers.keys():
                        self.__lprint("WARNING: Duplicate modifier name. The modifier '" + modifiername + "' already exists. Skipping...")
                    else:

                        self.modifiers[modifiername] = {}

                        self.modifiers[modifiername]["parent"] = parent
                        self.modifiers[modifiername]["type"] = objecttype
                        self.modifiers[modifiername]["attributes"] = objectattributes
                else:

                    if re.search("routerconfig$", objecttype, flags=re.I) or re.search("hostconfig$", objecttype, flags=re.I) or re.search("blockconfig$", objecttype, flags=re.I):
                        # Some protocols require some addition modifications to the existing protocol stack.                        
                        # The ProtocolCreate command will handle that for us.
                        primaryif = self.stc.get(parent, "primaryif")
                        result = self.stc.perform("ProtocolCreateCommand", CreateClassId=objecttype, ParentList=parent, UsesIfList=primaryif)
                        object = result['ReturnList']    

                    else:
                        object = ""

                        # There is no need to create Dot1xEap***Config objects. They are created automatically
                        # when the Dot1xSupplicantBlockConfig (parent) attribute "EapAuthMethod" is configured.
                        if re.search("Dot1xEap.+Config$", objecttype, flags=re.I):
                            # Set the EapAuthMethod, which will create the object for us, and then search for the desired object.
                            # Note that the user may ALSO set this attribute, however, it is not guaranteed to have
                            # been processed before this point, so we need to manually figure it out.

                            # Find the EapAuthMethod Type.
                            match = re.search("Dot1xEap(.+)Config", objecttype, flags=re.I)
                            # This is assuming that there is always a match. It shouldn't fail.
                            eapauthmethod = match.group(1)  

                            # This will ensure that the correct object is created.
                            self.stc.config(parent, EapAuthMethod=eapauthmethod)

                            # Just find the matching object type.
                            for child in self.stc.get(parent, "children").split():
                                if re.search(objecttype, child, flags=re.I):                                    
                                    object = child
                                    break


                        if key in self.objects.keys():

                            # The object already exists. Don't create a duplicate.
                            self.__lprint("WARNING: Duplicate object name. The object '" + key + "' (" + self.objects[key] + ") already exists.")

                            object = self.objects[key]                            

                        if not object:
                            object = self.stc.create(objecttype, under=parent, name=key)

                            if re.match("port[0-9]+", object, flags=re.I):
                                # Port locations in name mess things up.
                                self.stc.config(object, AppendLocationToPortName=False)

                    if objecttype.lower() == "bgpipv4routeconfig" or objecttype.lower() == "bgpipv6routeconfig":
                        # Add a sane default for the AS path.
                        router = self.stc.get(object, "parent")
                        asnum = self.stc.get(router, "AsNum")
                        self.stc.config(object, AsPath=asnum) 

                    # Keep track of all objects that are created. We need these for when we resolve "relations" later on.
                    self.objects[key] = object                       

                    self.__addObject(objectattributes, object)                    
            else:               

                attribute = key

                # Some keys may need to be modified.
                attribute = self.__modifyPDUKey(key)

                # Handle to ToS and DiffServ keys separately.
                if re.search("\.tos|\.diffserv", attribute, flags=re.I):
                    self.__setIPv4Tos(parent, attribute, objectdict[key])
                else:

                    if re.search("Relation:", attribute, flags=re.I):
                        # This attribute makes reference to an object.
                        # We must delay setting this value until later to avoid a race condition.
                        if parent not in self.relations.keys():
                            self.relations[parent] = {}

                        self.relations[parent][key] = objectdict[key]
                    else:
                        self.__config(parent, attribute, objectdict[key])

        return 

    #==============================================================================
    def __getObjectType(self, key, value):
        # Returns True if the specified key is referencing an object.
        # Key is referencing an object if "value" is a dictionary, and the field
        # "ObjectType" exists.
        if type(value) is dict and "ObjectType" in value.keys():
            return value["ObjectType"]
        else:
            return None

    #==============================================================================
    def __resolveRelations(self):
        # If an attribute has the keyword "Relation:", it means the value of the 
        # attribute references an object's name (or list of object names).
        # This method processes all of the references found during the configuration process.

        # Keep track of all streamblocks that require an update.
        # Bound streamblocks need to be updated after changing bindings.
        streamblockupdaterequired = {}

        for object in self.relations.keys():
            for attribute in self.relations[object].keys():
                value = self.relations[object][attribute]

                # Make sure value is a list.
                if not isinstance(value, list):
                    # Convert this string to a list.
                    value = [value]
                
                # Remove the "Relation:" tag from the attribute.
                attribute = re.sub("Relation:", "", attribute, flags=re.I)

                if attribute.lower() == "srcbinding" or attribute.lower() == "dstbinding":
                    streamblockupdaterequired[object] = True
           
                # value can be either a string or a list.
                objectlist = []
                for objectname in value:    

                    # The objectname may also include DDN notation (e.g. Device1.ipv4if(1)).
                    # Only attempt to resolve the object name (Device1).
                    match = re.search("\..+", objectname)

                    if match:
                        # Strip off the descendant notation. We'll add it back after.
                        objectnameonly = objectname[:match.start()]
                    else:
                        objectnameonly = objectname

                    if objectnameonly not in self.objects.keys():                            
                        errmsg = "An error occurred while processing '" + attribute + "' = '" + str(objectname) + "'\nUnable to locate the object."
                        logging.error(errmsg)
                        raise Exception(errmsg)
                    else:                     

                        objecthandle = self.objects[objectnameonly]   

                        if match:
                            # The DDN/DAN notation was used to specify a descendant of objecthandle. 
                            # We need to find the object handle of the descendant object instead.
                            objecthandle = self.stc.get(objecthandle + match.group(0), "Handle")                            

                        objectlist.append(objecthandle)
                 
                # We should have a list of objects now.
                if len(objectlist) > 0:
                    self.__config(object, attribute, " ".join(objectlist))                            

        for streamblock in streamblockupdaterequired.keys():
            self.stc.perform("StreamBlockUpdate", StreamBlock=streamblock)

        # Reset the relations dictionary.
        self.relations = {}
        return 

    #==============================================================================
    def __addModifiers(self):
        # If an attribute has the keyword "Relation:", it means the value of the 
        # attribute references an object's name (or list of object names).
        # This method processes all of the references found during the configuration process.

        for modifiername in self.modifiers.keys():

            parent     = self.modifiers[modifiername]["parent"]
            objecttype = self.modifiers[modifiername]["type"]
            attributes = self.modifiers[modifiername]["attributes"]

            object = self.createModifier(parent, objecttype, attributes)

            self.objects[modifiername] = object          

        # Reset the modifiers dictionary.
        self.modifiers = {}
        return         


    #==============================================================================
    def __purgeObjects(self):
        # Remove all entries from the objects dictionary that no longer exist.

        # We need to make a copy of the objects dictionary in order to make this work.
        objectscopy = self.objects.copy()

        for objectname in objectscopy.keys():
            objecthandle = self.objects[objectname]

            try:
                self.stc.get(objecthandle, "name")
            except Exception as ex:
                # The objecthandle is no longer valid so delete it from the objects dict.
                del self.objects[objectname]

        return

    #==============================================================================
    def __config(self, object, attribute, value):
        # This is a replacement for the built-in Spirent TestCenter config command.
        # I've designed it this way to speed up execution. The "expensive" __findAttributes
        # method is only used if the attribute can't be found.
        try:            
            # See if the build-in config function works.
            args = {attribute: value}
            self.stc.config(object, **args)
            #print(object + "." + attribute + " = " + str(value))
        except Exception as ex:
            # Some error occurred. It may have been that the attribute wasn't found for the current
            # object. Check the descendant objects to see if one of them has the attribute.
            resultdict = self.__findAttribute(object, attribute)

            #self.pp.pprint(resultdict)

            if not resultdict["foundmatch"]:
                # Nope...something went wrong.                
                errmsg = "An error occurred while processing '" + attribute + "' = " + str(value)
                logging.error(errmsg)
                #raise Exception("An error occurred while processing '" + attribute + "' = " + str(value) + "\n" + str(ex.args[2]))
                raise Exception(errmsg)

            # We can either use the DDN or the actual object. I'm just going with the DDN.
            object = resultdict["ddn"]
            attribute = resultdict["attribute"]
            args = {attribute: value}
            #print(object + "." + attribute + " = " + str(value))
            self.stc.config(object, **args)
            

        return
    #==============================================================================
    def __findAttribute(self, object, attribute, _topmost=True):
        # This recursive method will search the specified object, and all of its children,
        # for the specified attribute. The first match is always returned.
        # Returns a dictionary containing information on the match.
        # The original object and attribute are returned if no match is found.
        #
        # e.g.
        #   self.__findAttribute("port1", "SchedulingMode")        
        #
        # Returns:
        #   object     - The handle of the object where the matching attribute was found.
        #   attribute  - The exact attribute that was found. The case matches what the API uses.
        #   value      - The attribute's value.
        #   ddn        - The DDN path to the object.
        #   foundmatch - Boolean

        resultdict = {}

        # First, check the current object for a match:
        attributelist = self.stc.get(object).keys()

        matchingattribute = next((x for x in attributelist if x.lower() == attribute.lower()), None)       
        if matchingattribute:
            # Found the matching attribute.
            
            resultdict["object"] = object
            resultdict["attribute"] = matchingattribute
            resultdict["value"] = self.stc.get(object, matchingattribute)
            
            if _topmost:
                resultdict["ddn"] = object
            else:
                resultdict["ddn"] = self.stc.perform("GetObjectInfo", object=object)["ObjectType"]

            resultdict["foundmatch"] = True
        else:
            # Search the children.
            # NOTE: This can be computationally expensive (it's recursive).
            resultdict["foundmatch"] = False
            for child in self.stc.get(object, "children").split():
                resultdict = self.__findAttribute(child, attribute, _topmost=False)

                if resultdict["foundmatch"]:
                    # Prepend this object to the DDN path.
                    if _topmost:
                        resultdict["ddn"] = object + "." + resultdict["ddn"]
                    else:
                        objecttype = self.stc.perform("GetObjectInfo", object=object)["ObjectType"]
                        resultdict["ddn"] = objecttype + "." + resultdict["ddn"]
                    break

        if not resultdict["foundmatch"]:                   
            # Just return the original attribute and object.
            resultdict["object"] = object
            resultdict["ddn"] = object
            resultdict["attribute"] = attribute
            resultdict["value"] = None

        return resultdict      

    #==============================================================================
    def __modifyPDUKey(self, attribute): 
        # Any keys relating to PDU headers need to be modified to the strings expected by the API.
        # I could let the user simply use the native strings, however, they are different from
        # what they pass to the "Header" key, and that could be confusing to the user.
        attribute = re.sub("EthernetII\.", "ethernet:EthernetII.",            attribute, flags=re.I)
        attribute = re.sub("Vlan\.",       "ethernet:EthernetII.vlans.vlan.", attribute, flags=re.I)                        
        attribute = re.sub("IPv4\.",       "ipv4:IPv4.",                      attribute, flags=re.I)                        
        attribute = re.sub("IPv6\.",       "ipv6:IPv6.",                      attribute, flags=re.I)                        
        attribute = re.sub("UDP\.",        "udp:Udp.",                        attribute, flags=re.I)                        
        attribute = re.sub("TCP\.",        "tcp:Tcp.",                        attribute, flags=re.I)
        attribute = re.sub("Custom\.",     "custom:Custom.",                  attribute, flags=re.I) 
        return attribute

    #==============================================================================
    def __setIPv4Tos(self, streamblock, attribute, hexstring): 
        # Spirent TestCenter API doesn't allow the user to set the IPv4 ToS/Diffserv as
        # a single byte. This method does.
        object = streamblock + ".ipv4:IPv4.tosdiffserv.tos"
        value = int(hexstring, 16)
        self.stc.config(object, precedence=(value & 0xE0) >> 5,
                                dBit=(value & 0x10) >> 4,
                                tBit=(value & 0x08) >> 3,
                                rBit=(value & 0x04) >> 2,
                                mBit=(value & 0x02) >> 1,
                                reserved=(value & 0x01))
        return

    #==============================================================================
    def __getTrafficPorts(self): 
        # Returns a list of port handles of ports that are either sending or receiving traffic.
        portlist = []
        for port in self.stc.get("system1.project", "children-port").split():
            for streamblock in self.stc.get(port, "children-streamblock").split():
                
                #self.pp.pprint(self.stc.get(streamblock))
                #print()

                txports = []
                rxports = []

                # The Tx/Rx ports can be associated in several ways.
                # The Src/Dst bindings can point to emulated device interfaces, network blocks for routes,
                # network blocks for multicast groups, etc.
                # Raw streamblocks can explicitly list the Rx ports.

                srcbinding = self.stc.get(streamblock, "srcbinding").split()

                if len(srcbinding) > 0:
                    dstbinding = self.stc.get(streamblock, "dstbinding").split()

                    txports = self.__findAssociatedPorts(srcbinding)
                    rxports = self.__findAssociatedPorts(dstbinding)

                else:                    
                    # This is a raw streamblock.
                    txports += self.stc.get(streamblock, "parent").split()
                    rxports += self.stc.get(streamblock, "expectedrx-Targets").split()

                for port in txports:
                    if port not in portlist:
                        portlist.append(port)

                for port in rxports:
                    if port not in portlist:
                        portlist.append(port)

        return(portlist)

    #==============================================================================
    def __findAssociatedPorts(self, objecthandlelist): 
        # This is a "fun" method used to find out which ports are associated with the specified object.
        # The specified object could be something like a device's interface or multicast group networkblock.
        # Any port that is linked with that object will be returned.

        portlist = []
        devicelist = []

        # The first step is to determine the EmulatedDevice associated with this object.
        for objecthandle in objecthandlelist:
            #device = self.stc.perform("GetObjects", RootList=objecthandle, ClassName="EmulatedDevice", Direction="Up")["ObjectList"]
            device = self.__findDevice(objecthandle)
            
            if not device:
                # This is probably a multicast group object. If it's not, we'll need to add some additional code
                # to deal with that case.
                parent = self.stc.get(objecthandle, "parent")
                for associatedobject in self.stc.get(parent, "subscribedgroups-Sources").split():
                    #device = self.stc.perform("GetObjects", RootList=associatedobject, ClassName="EmulatedDevice", Direction="UP")["ObjectList"]
                    device = self.__findDevice(associatedobject)

                    if device and device not in devicelist:
                        devicelist.append(device)
                    else:
                        errmsg = "WARNING: Unable to find the associated port for the object '" + objecthandle + "'."
                        self.__lprint(errmsg)                        
            else:
                if device not in devicelist:
                    devicelist.append(device)
        
        # Now find all of the ports for these devices.        
        for device in devicelist:
            port = self.stc.get(device, "AffiliationPort-Targets")
            if port != "" and port not in portlist:
                portlist.append(port)

        return(portlist)

    #==============================================================================
    def __findDevice(self, objecthandle): 
        # This recursive method will search the ancestors objects of "objecthandle"
        # looking for a device/router/host object.
        parent = self.stc.get(objecthandle, "parent")

        if re.match("project[0-9]+", parent, flags=re.I) or parent.lower() == "system1" or parent == "":
            return(None)            
        elif re.match("emulateddevice[0-9]+", parent, flags=re.I) or re.match("router[0-9]+", parent, flags=re.I) or re.match("host[0-9]+", parent, flags=re.I):
            return(parent)
        else:
            return(self.__findDevice(parent))

    #==============================================================================
    def __enableDataMining(self, dataminingdict): 

        fieldlist = []
        for key in dataminingdict:
            fieldlist.append(dataminingdict[key])

        self.stc.config("system1.project.ResultOptions", SaveAtEotProperties=" ".join(fieldlist))

        return     

    #==============================================================================
    def __addTxRxEotStreamCustomResultsTable(self, db, datasetid):
        #   Use SQLite queries to build the result tables per flow. This will save
        #   execution time verse trying to do everything in Python.
        #
        # Arguments:
        #   db        - The database handle.
        #   datasetid - The datasetid to use when there are multiple datasets.
        #
        # Results:
        #   N/A: Throws an exception on error.

        # First, we need to build some other tables.
        self.__addRxEotStreamCustomResultsTable(db, datasetid)

        # Delete the table if it already exists. We need to create it every time, just in case additional
        # data has been added to the database.
        db.execute("DROP TABLE IF EXISTS TxRxEotStreamCustomTempResults")
        db.execute("DROP TABLE IF EXISTS TxRxEotStreamCustomResults")
        
        # Construct a list of data-mining columns. 
        # Every column that comes after NumOfMulticastExpectedRxPort column should be a data-mining column.
        columnlist = []
        dataminingcolumn = 0
        
        db.execute("PRAGMA table_info(TxEotStreamResults)")

        for row in db.fetchall():
            column = row[1]
            if dataminingcolumn == 1:
                columnlist.append(column)

            if column == "NumOfMulticastExpectedRxPort":
                dataminingcolumn = 1

        # Create the TxRxEotStreamCustomResults table. This will simplify and speed up the code.
        query = "CREATE TABLE TxRxEotStreamCustomTempResults AS \
            SELECT \
            Tx.DataSetId                                      AS 'DataSetId', \
            Tx.ParentHnd                                      AS 'TxPortHandle', \
            Tx.PortName                                       AS 'Tx Port', \
            Tx.StreamId                                       AS TxStreamId, \
            Tx.FrameCount                                     AS 'Tx Frames', \
            Tx.OctetCount                                     AS 'Tx Bytes', \
            Tx.StreamBlockName                                AS 'StreamBlockName', \
            Tx.ParentStreamBlock                              AS 'ParentStreamBlock', \
            COALESCE(Rx.PortName            ,'N/A')           AS 'Rx Port', \
            COALESCE(Rx.ParentHnd           ,'N/A')           AS 'RxPortHandle', \
            COALESCE(Rx.FlowCount           ,0)               AS 'RxFlowCount', \
            COALESCE(Rx.Comp32              ,0)               AS 'StreamId', \
            COALESCE(Rx.Comp16_1            ,0)               AS 'Comp16_1', \
            COALESCE(Rx.Comp16_2            ,0)               AS 'Comp16_2', \
            COALESCE(Rx.Comp16_3            ,0)               AS 'Comp16_3', \
            COALESCE(Rx.Comp16_4            ,0)               AS 'Comp16_4', \
            COALESCE(Rx.FrameCount          ,0)               AS 'Rx Frames', \
            COALESCE(Rx.OctetCount          ,0)               AS 'Rx Bytes', \
            COALESCE(Rx.FirstArrivalTime    ,-1)              AS 'First TimeStamp', \
            COALESCE(Rx.LastArrivalTime     ,-1)              AS 'Last TimeStamp', \
            COALESCE(Rx.AvgLatency          ,-1)              AS 'Store-Forward Avg Latency (ns)', \
            COALESCE(Rx.MinLatency          ,-1)              AS 'Store-Forward Min Latency (ns)', \
            COALESCE(Rx.MaxLatency          ,-1)              AS 'Store-Forward Max Latency (ns)', \
            COALESCE(Rx.IsExpectedPort      ,0)               AS 'IsExpectedPort', \
            COALESCE(Rx.DuplicateFrameCount ,0)               AS 'DuplicateFrameCount', \
            Rx.FilteredValue_1, \
            Rx.FilteredValue_2, \
            Rx.FilteredValue_3, \
            Rx.FilteredValue_4, \
            Rx.FilteredValue_5, \
            Rx.FilteredValue_6, \
            Rx.FilteredValue_7, \
            Rx.FilteredValue_8, \
            Rx.FilteredValue_9, \
            Rx.FilteredValue_10"
        
        # Now add the data-mining columns. I don't know of an easier way to do this.
        for column in columnlist:
            query += ", \n    Tx.'" + column + "' AS '" + column + "'"
        
        query += "\nFROM \
                      TxEotStreamResults as Tx \
                    LEFT JOIN \
                      RxEotStreamCustomResults as Rx \
                    ON \
                      Tx.DataSetId = " + str(datasetid) + "\
                    AND \
                      Rx.DataSetId = " + str(datasetid) + "\
                    AND \
                      Tx.StreamId = Rx.Comp32"   

        # This command will create the table.
        db.execute(query)

        # At this point, the TxRxEotStreamCustomTempResults table may have extra Tx rows for other dataset IDs.
        
        # The delete command doesn't seem to work.
        #db.execute("DELETE FROM TxRxEotStreamCustomResults WHERE DataSetId != " + str(datasetid))

        # Create the final table that will contain the results for the specified datasetid.
        db.execute("CREATE TABLE TxRxEotStreamCustomResults AS SELECT * FROM TxRxEotStreamCustomTempResults WHERE DataSetId = " + str(datasetid))

        # Clean up the temporary tables that we created to save space. You can comment out these lines if you want to debug issues.        
        db.execute("DROP TABLE IF EXISTS RxEotStreamCustomResults")
        db.execute("DROP TABLE IF EXISTS RxEotFlowCountPerStreamResults")
        db.execute("DROP TABLE IF EXISTS TxRxEotStreamCustomTempResults")

        return

    #==============================================================================
    def __addRxEotStreamCustomResultsTable(self, db, datasetid):    
        #
        #   If the user has specified an analyzer filter, the human-readable results
        #   are stored in the RxEotAnalyzerFilterValuesTable. To make things simple,
        #   join this table with the RxEotStreamResults table.
        #   The resulting table is RxEotStreamCustomResults.
        #
        # Arguments:
        #   db        - The database handle.
        #   datasetid - The datasetid to use when there are multiple datasets.
        #
        # Results:
        #   N/A: Throws an exception on error.

        # Calculate the number of flows per stream per Rx port.
        self.__addRxEotFlowCountPerStreamTable(db, datasetid)        

        # Delete the table if it already exists. We need to create it every time, just in case additional
        # data has been added to the database.
        db.execute("DROP TABLE IF EXISTS RxEotStreamCustomResults")

        query = "CREATE TABLE RxEotStreamCustomResults AS \
                 SELECT \
                   Rx.DataSetId, \
                   Rx.PortName, \
                   Rx.ParentHnd, \
                   Flow.FlowCount, \
                   Rx.Comp32, \
                   Rx.Comp16_1, \
                   Rx.Comp16_2, \
                   Rx.Comp16_3, \
                   Rx.Comp16_4, \
                   Rx.FrameCount, \
                   Rx.OctetCount, \
                   Rx.FirstArrivalTime, \
                   Rx.LastArrivalTime, \
                   Rx.AvgLatency, \
                   Rx.MinLatency, \
                   Rx.MaxLatency, \
                   Rx.IsExpectedPort, \
                   Rx.DuplicateFrameCount, \
                   Filter.FilteredValue_1, \
                   Filter.FilteredValue_2, \
                   Filter.FilteredValue_3, \
                   Filter.FilteredValue_4, \
                   Filter.FilteredValue_5, \
                   Filter.FilteredValue_6, \
                   Filter.FilteredValue_7, \
                   Filter.FilteredValue_8, \
                   Filter.FilteredValue_9, \
                   Filter.FilteredValue_10 \
                 FROM \
                   RxEotStreamResults AS Rx \
                 LEFT JOIN \
                   RxEotAnalyzerFilterValuesTable AS Filter \
                 ON \
                   Filter.DataSetId = " + str(datasetid) + "\
                 AND \
                   Rx.DataSetId = " + str(datasetid) + "\
                 AND \
                   Rx.Comp32 = Filter.Comp32 \
                 AND \
                   Rx.Comp16_1 = Filter.Comp16_1 \
                 AND \
                   Rx.Comp16_2 = Filter.Comp16_2 \
                 AND \
                   Rx.Comp16_3 = Filter.Comp16_3 \
                 AND \
                   Rx.Comp16_4 = Filter.Comp16_4 \
                 LEFT JOIN \
                   RxEotFlowCountPerStreamResults AS Flow \
                 ON \
                   Rx.DataSetId = " + str(datasetid) + "\
                 AND \
                   Rx.Comp32 = Flow.Comp32 \
                 AND \
                   Rx.ParentHnd = Flow.ParentHnd \
                 WHERE \
                   Rx.DataSetId = " + str(datasetid)                   

        # This command will create the table.
        db.execute(query)
        return

    #==============================================================================
    def __addRxEotFlowCountPerStreamTable(self, db, datasetid):    

        #   Add a table that contains the number of flows per StreamID per Rx port.   
        #   This will allow us to speed up execution with large datasets.
        #
        # Arguments:
        #   db        - The database handle.
        #   datasetid - The datasetid to use when there are multiple datasets.
        #
        # Results:
        #   N/A: Throws an exception on error.
        
        # Delete the table if it already exists. We need to create it every time, just in case additional
        # data has been added to the database.
        db.execute("DROP TABLE IF EXISTS RxEotFlowCountPerStreamResults")

        # Create the RxEotFlowCountPerStreamResults table.
        query = "CREATE TABLE RxEotFlowCountPerStreamResults AS \
                 SELECT \
                    DataSetId, \
                    PortName, \
                    ParentHnd, \
                    Comp32, \
                    Count(Comp32) AS 'FlowCount' \
                 FROM \
                    RxEotStreamResults \
                 WHERE \
                    DataSetId = " + str(datasetid) + "\
                 GROUP BY \
                    Comp32, ParentHnd"

        # This command will create the table.
        db.execute(query)

        return


    #==============================================================================
    def __getLatestDataSetId(self, db):
        # Determine the latest DataSetId from the DataSet table.
        query = "SELECT MAX(Id) from DataSet"
        db.execute(query)
        return(db.fetchone()[0])

    #==============================================================================
    def __getResultsAsDict(self, row, columns):
        # Construct a dictionary from the specified row (value) and columns (keys).
        resultdict = defaultdict(dict)

        for column, value in zip(columns, row):
            resultdict[column[0]] = value

        return resultdict

    #==============================================================================
    def __getRfc2544ResultFiles(self):
        """Retrieve the result database files. They could be local, or stored on the
        Lab Server, or the ReST API server. All three scenarios require a different
        process.
        """

        dbfilenames = []

        if self.labserverip:
            # If we are using a Lab Server, we need to do a little dance to get the files
            # from the server to the desired location on the local client.
            
            # Do the following in the temporary log directory. This will allow us to clean
            # up all of the extra files (logs and stuff) when we are done.
            originalpath = os.path.abspath(os.getcwd())
            temppath = os.path.abspath(os.path.join(self.logpath, "results"))

            if not os.path.exists(temppath):
                os.makedirs(temppath)

            try:
                os.chdir(temppath)        

                if self.usingrestapi:
                    # Currently, the Python ReST adapter differs from the native Python API
                    # when using the CsSynchronizeFiles command. The ReST adapter flattens
                    # the directory structure and puts all files in the same directory.

                    # Download each result DB file.
                    for filename in self.stc._stc.files():
                        if re.match(r"Results/+", filename):
                            savedfilename, sizeinbytes = self.stc._stc.download(filename)
                            savedfilename = os.path.abspath(savedfilename)
                            dbfilenames.append(savedfilename)

                else:
                    # NOTE: This command becomes VERY slow when there are a large number of DB files to download.
                    # Download all files from the Lab Server into the temporary directory.
                    # The native API doesn't support downloading single files.
                    self.stc.perform("cssynchronizefiles")      

                    for root, dirs, files in os.walk("./Results"):
                        for name in files:
                            filename = os.path.abspath(os.path.join(root, name))
                            dbfilenames.append(filename)
            except Exception as errmsg:                                        
                self.__lprint(errmsg)                

            os.chdir(originalpath)

        else:
            # The files are local to this host.
            for root, dirs, files in os.walk("./Results"):
                for name in files:
                    filename = os.path.abspath(os.path.join(root, name))
                    dbfilenames.append(filename) 

        return dbfilenames

    #==============================================================================
    def __getRfc2544SummaryResults(self, dbfilename, tablename):
        """Process the specified result DB file and return the specified result 
        table from the RFC2544 summary result database file.
        """
        
        # if os.path.isfile(dbfilename):
        #     errmsg = "The summary results database file '" + dbfilename + "' could not be located."
        #     logging.error(errmsg)
        #     raise Exception(errmsg)

        results = {}
       
        try:

            conn = sqlite3.connect(dbfilename)
            db = conn.cursor()

            query = "SELECT * FROM " + tablename
            
            db.execute(query)
            
            i = 1
            description = db.description
            for row in db.fetchall():
                rowdict = self.__getResultsAsDict(row, description)

                id = rowdict["Id"]
                results[id] = {}

                for key in rowdict.keys():
                    results[id][key] = rowdict[key]                

        except Exception as errmsg:                                                    
            self.__lprint(errmsg)
            raise Exception(errmsg)

        conn.close()

        return results

    #==============================================================================
    def __rmtree(self, path):
        """Delete the specified file or directory. This works for nested,
           subdirectories and they do NOT need to be empty.
        """

        if os.path.isfile(path):
            os.remove(path)
        else:
            for the_file in os.listdir(path):
                file_path = os.path.join(path, the_file)
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                    elif os.path.isdir(file_path): 
                        self.__rmtree(file_path)
                except Exception as errmsg:                                        
                    self.__lprint(errmsg)


            os.rmdir(path)
        return

    #==============================================================================
    def __lprint(self, message):
        """Log the specified message. Print it to STDOUT if the verbose flag is set.
        """

        logging.info(message)

        if self.verbose:
            print(str(message))

        return

    
###############################################################################
####
####    Functions
####
###############################################################################
def main():    

    try:
        #stcgen = StcGen(userest=True, labserverip="192.168.8.138", existingsession="kill", terminateonexit=True)
        print("Main")

    except:
        errormsg = sys.exc_info()[0]
        print(errmsg)

    print("Done!")    

    return

###############################################################################
####
####    Main
####
###############################################################################

if __name__ == "__main__":
    main()
