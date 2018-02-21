###############################################################################
#
#           Spirent TestCenter Spitfire Traffic Generator Library
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
import os
import time
import datetime
import re
import getpass
import json
import sqlite3

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
    def __init__(self, userest=False, labserverip=None, username=None, sessionname="StcGen", existingsession="", verbose=False, terminateonexit=False):
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
            Increasing the logging verbosity if set to True.
        terminateonexit: bool
            Terminates the Lab Server session, if set to True, when cleaning up the object instance.

        """

        #print("DEBUG: Using PPRINT")
        #self.pp = pprint.PrettyPrinter(indent=2)

        if not username:
            username = getpass.getuser()
        
        self.labserverip = labserverip

        if userest:
            print("Using the native Python adaptor...")
            from stcrestclient import stcpythonrest
            self.stc = stcpythonrest.StcPythonRest()
        
            # This is the REST-only method for connecting to the server.
            self.stc.new_session(labserverip, session_name=sessionname, user_name=username, existing_session=existingsession)

        else:            
            
            from StcPython import StcPython
            self.stc = StcPython()  

            if labserverip:                
                print("DEBUG: WIP...need to determine if the existing session should be joined or killed.")  
                self.stc.perform("CSTestSessionConnect", host=labserverip,
                                                         TestSessionName=sessionname,
                                                         OwnerId=username,
                                                         CreateNewTestSession="True")    

        self.terminateonexit = terminateonexit
        
        # This dictionary keeps track of relations that we need to resolve after all
        # objects have been created. This is necessary to prevent race conditions.
        self.relations = {}

        # This dictionary keeps track of all objects that we have created. 
        # The key is the object name so multiple objects with the same name is a problem.
        self.objects   = {}

        return

    #==============================================================================
    def __del__(self):
        if self.terminateonexit and self.labserverip:
            # Terminate the Lab Server session.
            self.stc.perform("CSTestSessionDisconnect", Terminate="True")

            print("DEBUG: NOTE...the disconnect is not working...")
               
        return

    #==============================================================================
    #
    #   Public Methods
    #
    #==============================================================================
    def loadJson(self, inputfilename, deleteExistingConfig=True):
        """Parses the specifed JSON file and generates the corresponding Spirent TestCenter objects.

        Parameters
        ----------
        inputfilename : str
            The name of the JSON input configuration file.
        deleteExistingConfig : bool
            Set to True to delete the existing project and create a new configuration.

        """

        # The configuration should be specified in the JSON.
        if deleteExistingConfig:
            self.resetConfig()

        inputdict = self.__convertJsonToDict(inputfilename)

        # Start with the configuration section of the input file.
        if "Configuration" in inputdict.keys():
            # Loading a new configuration will reset the existing config.
            configdict = inputdict["Configuration"]

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
                        raise Exception("The file '" + filename + "' needs to be either an XML, TCC or DB file.")
                else:
                    raise Exception("The file '" + filename + "' does not exist.")
            else:
                self.project = self.stc.get("system1", "children-project")

                # Parse the configuration dict and create all objects.
                self.__addObject(configdict)

                # Resolve all object references from the configuration.
                # We need to do this after all of the objects have been created
                # in order to prevent any race conditions.
                self.__resolveRelations()

        # Now deal with the Tests section of the input file.
        if "Tests" in inputdict.keys():
            # NOTE: The order of keys for a JSON file is NOT maintain when it gets 
            #       loaded into Python. The order of the tests is determined by 
            #       the name of the tests!!!
            self.testsdict = inputdict["Tests"]

        return

    #==============================================================================
    def resetConfig(self):
        """Delete the existing project and return the configuration to the default state.

        """

        # Reset the configuration.
        self.stc.perform("ResetConfig", config="system1")        
        self.relations = {}
        self.objects = {}
        self.testsdict = {}

        return

    #==============================================================================
    def runAllTests(self):
        """Run all tests defined by the JSON configuration.

        Returns
        -------
        dict
            Returns a dictionary containing test status, statistics and results database filename information.         
        
        """

        self.connectAndApply()   

        results = {}

        # First, make sure none of the tests are "continuous"
        for testname in sorted(self.testsdict.keys()):
            #testtype = self.testsdict[testname].get("Type","FixedDuration")
            results[testname] = self.runTest(testname, parametersdict=self.testsdict[testname].copy())

        # print("Disconnecting from hardware...")
        self.stc.perform("ChassisDisconnectAll")

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

        # Options are: Ping, FixedDuration. Consider adding RFC 2544 and 2889 (as well as others).
        if not parametersdict:
            parametersdict = {}

        testtype = str(parametersdict.get("Type", testtype))

        results = {}
        if testtype.lower() == "fixedduration":
            results = self.runFixedDurationTest(testname, parametersdict=parametersdict, **kwargs)
        elif testtype.lower() == "ping":
            results = self.runPingTest(testname, parametersdict=parametersdict, **kwargs)
        else:
            raise Exception("Unknown test type '" + testtype + "' for test '" + testname + "'.")

        return(results)

    #==============================================================================
    def runFixedDurationTest(self, testname, 
                                   Duration       = 60,
                                   DurationMode   = "SECONDS",
                                   LearningMode   = "L3",
                                   FrameLengths   = None,
                                   Loads          = None,
                                   LoadUnit       = "PERCENT_LINE_RATE",
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
            One of these values: "L2" or L3".
        FrameLengths: List(int)
            A list of frame lengths to execute the test for. Set to "-1"
        Loads: List(int)
            A list of loads to execute the test for.
        LoadUnit: str
            The units for the load. One of these values: "PERCENT_LINE_RATE", "FRAMES_PER_SECOND",
            "INTER_BURST_GAP", "BITS_PER_SECOND", "KILOBITS_PER_SECOND", "MEGABITS_PER_SECOND" or "L2_RATE".
        parametersdict : dict
            A dictionary of test parameters. Users can use either the parametersdict or
            keyword arguments. parameterdict settings take precedence over keyword arguments.      
        
        Returns
        -------
        dict
            Returns a dictionary containing test status, statistics and results database filename information.

        """ 

        # Override the keyword arguments with the parametersdict settings.
        duration     = parametersdict.get("Duration",     Duration)
        durationmode = parametersdict.get("DurationMode", DurationMode)
        learning     = parametersdict.get("LearningMode", LearningMode)  
        framelengths = parametersdict.get("FrameLengths", FrameLengths)      
        loads        = parametersdict.get("Loads",        Loads)
        loadunit     = parametersdict.get("LoadUnit",     LoadUnit)

        # Create a timestamp for the result databases.
        now = datetime.datetime.now()
        timestamp = now.strftime("%Y-%m-%d-%H-%M-%S")
                
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

        self.trafficLearn(learning)

        self.stc.perform("DevicesStartAll")

        # If set to -1, use the configured load/framelength.
        if not framelengths:
            framelengths = [-1]
        if not loads:
            loads = [-1]

        iteration = 1
        for framelength in framelengths:
            for load in loads:
                currentfilename = resultsdbfilename
                if load > 0:
                    print("Setting Load to " + str(load))
                    
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
                    print("Setting FrameLength to " + str(framelength))
                    
                    # Add the framelength to the results filename.
                    path     = os.path.dirname(currentfilename)
                    filename = os.path.basename(currentfilename)
                    basename = filename.split(".")[0]
                    basename += "_" + str(framelength)
                    extension = ".".join(filename.split(".")[1:])
                    currentfilename = os.path.join(path, basename + "." + extension)

                    for port in portlist:
                        for streamblock in self.stc.get(port, "children-streamblock").split():
                            self.stc.config(streamblock, FixedFrameLength=framelength)

                self.stc.apply()

                self.stc.perform("ResultsClearAll")
                               
                self.trafficStart()

                self.trafficWaitUntilDone()

                # Give the frames a little time to clear the DUT.
                time.sleep(1)                

                # Traffic has stopped. Gather results.
                resultsfilename = self.saveResultsDb(currentfilename)
                #resultsfilename = self.saveResultsDb(currentfilename, deletetemp=False)

                results["Iterations"][iteration] = {}
                results["Iterations"][iteration]["Load"] = load
                results["Iterations"][iteration]["LoadUnit"] = loadunit
                results["Iterations"][iteration]["FrameLength"] = framelength
                results["Iterations"][iteration]["Database"] = resultsfilename
                results["Iterations"][iteration]["FlowStats"] = {}
                results["Iterations"][iteration]["StreamStats"] = {}
                results["Iterations"][iteration]["StreamBlockStats"] = {}

                if resultsfilename:
                    results["Iterations"][iteration]["FlowStats"] = self.getResultsDictFromDb(resultsfilename, mode="FLOW")
                    results["Iterations"][iteration]["StreamStats"] = self.getResultsDictFromDb(resultsfilename, mode="STREAM")
                    results["Iterations"][iteration]["StreamBlockStats"] = self.getResultsDictFromDb(resultsfilename, mode="STREAMBLOCK")

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

        # result = self.stc.perform("PingVerifyConnectivity", FrameCount=count)
        # if result["PassFailState"] == "PASSED":
        #     passed = True
        # else:
        #     # The ping test didn't pass. Now we need to figure out which devices failed.            
        #     # Unfortunately, I don't think we can simply look at a report to figure this
        #     # out. I think we have to do a ping test for each device.
        #     passed = False
        #     devices = self.stc.get("system1.project", "children-emulateddevice").split()

        #     passedlist = []
        #     failedlist = []
        #     for device in devices:

        #         result = self.stc.perform("PingVerifyConnectivity", DeviceList=device, FrameCount=count)
        #         if result["PassFailState"] == "PASSED":
        #             passedlist.append(device)
        #             print(device + " = PASSED")
        #         else:
        #             print(device + " = FAILED")
        #             failedlist.append(device)
        #             # print("FAILED=" + device)
        #             # self.pp.pprint(self.stc.get(device))
        #             # for child in self.stc.get(device, "children").split():
        #             #     print(child)
        #             #     self.pp.pprint(self.stc.get(child))
        #             #     print()
        #         port = self.stc.get(device, "AffiliationPort-Targets")
        #         self.pp.pprint(self.stc.get(port + ".PingReport"))
        #         print()

        if passed:
            results["Status"] = "PASSED"
                    
        return(results)

    #==============================================================================
    def trafficStart(self):
        """Start the traffic generators on all ports.
        """
        # print("Starting generators...")
        self.stc.perform("GeneratorStart")

        return  

    #==============================================================================
    def trafficStop(self):
        """Stop the traffic generators on all ports.
        """        
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
        if learningmode.upper() == "L2":
            self.stc.perform("ArpNdStartOnAllDevices")
            self.stc.perform("L2LearningStart")
        elif learningmode.lower() == "L3":  
            self.stc.perform("ArpNdStartOnAllDevices")
            self.stc.perform("ArpNdStartOnAllStreamBlocks") 

        return   

    #==============================================================================
    def trafficWaitUntilDone(self):
        """Blocks execution until all generators have stopped sending frames.

        Raises
        ------
        Exception
            An exception is generated if the DurationMode is set to CONTINUOUS for
            any port.

        """

        # NOTE: This method will raise an exception if there are any ports where
        #       the DurationMode is set to CONTINUOUS.        
        for port in self.stc.get(self.project, "children-port").split():
            if self.stc.get(port + ".generator.generatorconfig", "DurationMode").upper() == "CONTINUOUS":
                raise Exception("The DurationMode for port '" + self.stc.get(port, "Name") + "' is set to CONTINUOUS.")

        while self.isTrafficRunning():
            print("Test is running...")
            time.sleep(1)    

        return

    #==============================================================================
    def resultsClear(self):
        """Clears all statistics.        
        """
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

        streamblock = self.stc.create("StreamBlock", under=port, FrameConfig=frameconfig, Name=name)
        self.stc.perform("StreamBlockUpdate", StreamBlock=streamblock)

        self.__addObject(parametersdict, streamblock)

        return streamblock 

    #==============================================================================
    def createDevice(self, port, name, parametersdict=""):
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

        self.__addObject(parametersdict, device)

        return device

    #==============================================================================
    def createModifier(self, streamblock, modifiertype, parametersdict=""): 
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

        # Modifiers are very "picky".
        # The modifier object's attributes are case sensitive, you the user
        # must enter the correct field, or the modifier will not work.

        if "Field" in parametersdict.keys():
            field = parametersdict["Field"]
            del parametersdict["Field"]
        else:
            raise Exception("You must specify the field attribute for a modifier.")

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
    def connectAndApply(self, revokeowner=False):
        """Connect to the IL (hardware), reserve ports and apply the configuration.

        Parameters
        ----------
        revokeowner : bool
            Set to True to revoke any existing reservations.
        
        """

        offlineports = []
        for port in self.stc.get(self.project, "children-port").split():
            online = self.stc.get(port, "Online")
            if online.lower() == "false":
                offlineports.append(port)

        if len(offlineports) > 0:
            self.stc.perform("AttachPorts", portList=" ".join(offlineports),
                                            autoConnect=True,
                                            RevokeOwner=revokeowner)
        self.stc.apply()
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

        running = False
        for port in self.stc.get(self.project, "children-port").split():            
            if self.stc.get(port + ".generator", "State") != "STOPPED":
                running = True
                break

        return(running)

    #==============================================================================
    def saveResultsDb(self, filename, deletetemp=True): 
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
            # Delete the temporary directory afterward.
            
            # Do the following in a temporary directory. This will allow us to clean
            # up all of the extra files (logs and stuff) when we are done.
            originalpath = os.getcwd()
            temppath = ".stcgen_results_temp"

            try:
                # Determine the sourcefilename BEFORE changing directories. It looks like 
                # there is some weirdness with the os.path.abspath() function.
                sourcefilename = os.path.join(temppath, "stcgen_results", filename)                
                sourcefilename = os.path.abspath(sourcefilename)

                if not os.path.exists(temppath):
                    os.makedirs(temppath)

                os.chdir(temppath)

                # Download all files from the Lab Server into the temporary directory.
                self.stc.perform("cssynchronizefiles")

                # Now move the DB file from the temporary directory and put it into 
                # the specified directory (path).
                if not os.path.exists(path):
                    os.makedirs(path)                

                targetfilename = os.path.join(path, filename)

                if os.path.isfile(sourcefilename):
                    # This is the move function.
                    os.rename(sourcefilename, targetfilename)

                    filename = targetfilename

                else:
                    # Something went wrong. Spare the temporary results directory for debugging.
                    raise Exception("Unable to locate the results DB file '" + filename + "' (" + sourcefilename + ").")                

            except Exception as ex:                
                filename = None
                print("WARNING: Something went wrong while downloading the results DB.")
                print(ex)
                pass

            os.chdir(originalpath)

            if deletetemp:
                # Delete the temporary results directory.                        
                self.__rmtree(temppath)

        return filename

    #==============================================================================
    def getResultsDictFromDb(self, resultsdatabase, mode="FLOW", datasetid=""):
        """Generates a result view from the specified Spirent TestCenter End-of-Test results (sqlite) database.
        
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

        """
        
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
        self.__addRxEotStreamCustomResultsTable(db, datasetid)
        self.__addTxRxEotStreamCustomResultsTable(db, datasetid)

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
                query = "SELECT count(StreamId) FROM TxRxEotStreamCustomResults WHERE RxPortHandle = " + str(rxport) + " AND StreamId = " + str(results['StreamId'])
                db.execute(query)
                for entry in db.fetchall():
                    rxflowcount = entry[0]                
            else:
                # There are actually no flows received on this port, but this rxflowcount is only
                # used to correct the Tx count for more than one Rx flow.
                rxflowcount = 1

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
                    txduration  = results['Tx Frames'] * portconfig[txport]['streamcount'] / portconfig[txport]['fps']
                    txfps       = portconfig[txport]['fps'] / portconfig[txport]['streamcount']

                else:
                    # We don't support other modes. 
                    # You COULD ignore this error, but all rate calculations would be incorrect.
                    raise ValueError("WARNING: " + str(portconfig[txport]['mode']) + " scheduling is not supported. Rate calculations will be incorrect.")
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
            
            # The following code is for aggregating the results.
            # If the mode is "FLOW", no aggregation is performed.
            # If the mode is "STREAM" or "STREAMBLOCK", then the results are aggregated at that level.
            txid = str(results['TxStreamId'])

            if mode.upper() == "FLOW":                        
                # Store the flow result in the resultsdict array.  
                # We need to store each flow in the dictionary using a unique key.
                # The unique key is created from the all of the CompX values.            
                key  = str(results['StreamId']) + "." 
                key += str(results['Comp16_1']) + "." 
                key += str(results['Comp16_2']) + "." 
                key += str(results['Comp16_3']) + "." 
                key += str(results['Comp16_4'])
               
                resultsdict[txid] = defaultdict(dict)
                resultsdict[txid][rxport][key] = flowresult
                resultsdict[txid][rxport][key]['FlowCount'] = 1
                
            elif mode.upper() == "STREAM" or mode.upper() == "STREAMBLOCK":
                
                # Aggregate the flow results (per RxPort) for this stream or streamblock.
                if mode.upper() == "STREAM":
                    key = results['StreamId']
                else:
                    key = results['ParentStreamBlock']

                if not rxport in resultsdict:
                    resultsdict[txid][rxport] = defaultdict(dict)

                if not key in resultsdict[txid][rxport]:
                    # Create a new entry for this [rxport][stream/streamblock].                
                    resultsdict[txid][rxport][key] = flowresult 
                    resultsdict[txid][rxport][key]['FlowCount'] = 1
                else:
                    resultsdict[txid][rxport][key]['Tx Frames']           += flowresult['Tx Frames']
                    resultsdict[txid][rxport][key]['Tx Bytes']            += flowresult['Tx Bytes']
                    resultsdict[txid][rxport][key]['Rx Frames']           += flowresult['Rx Frames']
                    resultsdict[txid][rxport][key]['Rx Bytes']            += flowresult['Rx Bytes']
                    resultsdict[txid][rxport][key]['DuplicateFrameCount'] += flowresult['DuplicateFrameCount']
                                
                    resultsdict[txid][rxport][key]['First TimeStamp'] = min(resultsdict[txid][rxport][key]['First TimeStamp'], flowresult['First TimeStamp'])                
                    resultsdict[txid][rxport][key]['Last TimeStamp']  = max(resultsdict[txid][rxport][key]['Last TimeStamp'],  flowresult['Last TimeStamp'])
                    
                    # Sum the latency. We'll divide it by the total flow count later.
                    resultsdict[txid][rxport][key]['Store-Forward Avg Latency (ns)'] += flowresult['Store-Forward Avg Latency (ns)']
                                    
                    resultsdict[txid][rxport][key]['Store-Forward Min Latency (ns)'] = min(resultsdict[txid][rxport][key]['Store-Forward Min Latency (ns)'], flowresult['Store-Forward Min Latency (ns)'])
                    resultsdict[txid][rxport][key]['Store-Forward Max Latency (ns)'] = max(resultsdict[txid][rxport][key]['Store-Forward Max Latency (ns)'], flowresult['Store-Forward Max Latency (ns)'])
                    
                    resultsdict[txid][rxport][key]['Tx Frame Rate']      += flowresult['Tx Frame Rate']
                    resultsdict[txid][rxport][key]['Tx Rate (Bps)']      += flowresult['Tx Rate (Bps)']
                    resultsdict[txid][rxport][key]['Tx Rate (bps)']      += flowresult['Tx Rate (bps)']
                    resultsdict[txid][rxport][key]['Tx Rate (Kbps)']     += flowresult['Tx Rate (Kbps)']
                    resultsdict[txid][rxport][key]['Tx Rate (Mbps)']     += flowresult['Tx Rate (Mbps)']
                    resultsdict[txid][rxport][key]['Rx Expected Frames'] += flowresult['Rx Expected Frames']
                    resultsdict[txid][rxport][key]['Frames Delta']       += flowresult['Frames Delta']
                    resultsdict[txid][rxport][key]['Rx Frame Rate']      += flowresult['Rx Frame Rate']
                    resultsdict[txid][rxport][key]['Rx Rate (Bps)']      += flowresult['Rx Rate (Bps)']
                    resultsdict[txid][rxport][key]['Rx Rate (bps)']      += flowresult['Rx Rate (bps)']
                    resultsdict[txid][rxport][key]['Rx Rate (Kbps)']     += flowresult['Rx Rate (Kbps)']
                    resultsdict[txid][rxport][key]['Rx Rate (Mbps)']     += flowresult['Rx Rate (Mbps)']
                    
                    if resultsdict[txid][rxport][key]['Rx Expected Frames'] == 0:
                        # Avoid a divide-by-zero error.
                        loss = 0
                    else:
                        loss = resultsdict[txid][rxport][key]['Frames Delta'] * 100.0 / resultsdict[txid][rxport][key]['Rx Expected Frames']

                    resultsdict[txid][rxport][key]['Loss %']                    = loss
                    resultsdict[txid][rxport][key]['Packet Loss Duration (ms)'] = txduration * resultsdict[txid][rxport][key]['Loss %'] * 1000 / 100.0                                        

                    resultsdict[txid][rxport][key]['FlowCount'] += 1                

        # Lastly, we need to calculate averages, correct Tx counts for Rx flows belonging to the same stream, as well as clean up some labels.
        # Keep a list of all Rx (analyzer) filters.
        rxfilterlist = []
        for streamid in resultsdict.keys():
            for rxport in resultsdict[streamid].keys():

                if rxport == "N/A":
                    continue

                # Determine the correct label to substitute for the "FilteredName_X" keys.
                # This information is in the RxEotAnalyzerFilterNamesTable table, and is ONLY 
                # valid if the Rx port has an analyzer filter defined.
                query = "SELECT * FROM RxEotAnalyzerFilterNamesTable WHERE ParentHnd = " + str(rxport)   
                db.execute(query)
                for row in db.fetchall():
                    # The filters are labeled FilteredName_1 through FilteredName_10.
                    for index in range(1,11):
                        newlabel = row[index + 3]
                        oldlabel = "FilteredValue_" + str(index)

                        if newlabel:
                            # Add the correct filtername to each flow/stream/streamblock entry in the results.                            
                            for key in resultsdict[streamid][rxport].keys():                                            
                                resultsdict[streamid][rxport][key][newlabel] = resultsdict[streamid][rxport][key][oldlabel]

                            if newlabel not in rxfilterlist:
                                rxfilterlist.append(newlabel)                        
                        else:
                            # Trim off unused filters from the dictionary.
                            for key in resultsdict[streamid][rxport].keys():                                            
                                resultsdict[streamid][rxport][key].pop(oldlabel)  

                for key in resultsdict[streamid][rxport].keys():
                    # Calculate the average latency.
                    latency   = resultsdict[streamid][rxport][key]['Store-Forward Avg Latency (ns)']
                    flowcount = resultsdict[streamid][rxport][key]['FlowCount']
                    resultsdict[streamid][rxport][key]['Store-Forward Avg Latency (ns)'] = latency / flowcount
                    
        # Add the RxFilterList to each entry in the results dictionary.
        for streamid in resultsdict.keys():
            for rxport in resultsdict[streamid].keys():
                if rxport == "N/A":
                    continue
        
                    # The flow lable is a little misleading. It could be a flow, stream or streamblock.
                    for flow in resultsdict[streamid][rxport].keys():                                            
                        resultsdict[streamid][rxport][flow]['RxFilterList'] = rxfilterlist
                        # Make sure all filters are listed for all flows.
                        for rxfilter in rxfilterlist:
                            if rxfilter not in resultsdict[streamid][rxport][flow].keys():
                                resultsdict[streamid][rxport][flow][rxfilter] = ""
                    
        # We are done with the database.
        conn.close()

        return(resultsdict)        

    #==============================================================================
    def generateCsv(self, resultsdb, prefix=""):
        """Generate a plain-text CSV file from the specified results database.

        The CSV file will be generated in the same directory as the results database.

        Parameters
        ----------
        resultsdb : str
            The filename of the source results database.
        prefix: 
            An optional prefix to add to the CSV results filename.

        """

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

    #==============================================================================
    #
    #   Private Methods
    #
    #==============================================================================
    def __convertJsonToDict(self, inputfilename):        

        # Open and read the JSON input file.
        jsondict = {}

        try:
            with open(inputfilename) as json_file:
                jsondict = json.load(json_file)
        except:
            print("Unexpected error while parsing the JSON:", sys.exc_info()[1])
            print()
            raise

        return jsondict

    #==============================================================================
    def __addObject(self, objectdict, parent=None):   
        # This is a recursive method for parsing the JSON/Dictionary configuration
        # and translating it into Spirent TestCenter objects.

        if not parent:
            parent = self.project

        # Iterate through each key.
        for key in sorted(objectdict.keys()):
            # A new object will contain a dictionary for the "value", and that
            # dictionary will have the "ObjectType" key defined.
            objecttype = self.__getObjectType(key, objectdict[key])

            if objecttype:                     
                # We found a new object. Make a copy of the objectdict and delete the "ObjectType" key.
                # If we don't make a copy of the dict, the for loop that we are in may fail.
                objectattributes = objectdict[key].copy()
                del objectattributes["ObjectType"]

                if objecttype.lower() == "streamblock":
                    object = self.createStreamBlock(port=parent, name=key, parametersdict=objectattributes)
                if objecttype.lower() == "emulateddevice" or objecttype.lower() == "device" or objecttype.lower() == "router":
                    object = self.createDevice(parent, key, objectattributes)                    
                elif re.search("modifier", objecttype, flags=re.I):
                    object = self.createModifier(parent, objecttype, objectattributes)
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
                            # Note that the user may ALSO set this attribute, however, it is not guarenteed to have
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

                        if not object:
                            object = self.stc.create(objecttype, under=parent, name=key)

                    if objecttype.lower() == "bgpipv4routeconfig" or objecttype.lower() == "bgpipv6routeconfig":
                        # Add a sane default for the AS path.
                        router = self.stc.get(object, "parent")
                        asnum = self.stc.get(router, "AsNum")
                        self.stc.config(object, AsPath=asnum)                        

                    self.__addObject(objectattributes, object)
                # Keep track of all objects that are created. We need these for when
                # we resolve "relations" later on.
                if key in self.objects.keys():
                    print("WARNING: Duplicate object name. The object '" + self.objects[key] + "' already has the name '" + key + "'.")
                else:
                    self.objects[key] = object
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

        for object in self.relations.keys():
            for attribute in self.relations[object].keys():
                value = self.relations[object][attribute]

                # Make sure value is a list.
                if not isinstance(value, list):
                    # Convert this string to a list.
                    value = [value]
                
                # Remove the "Relation:" tag from the attribute.
                attribute = re.sub("Relation:", "", attribute, flags=re.I)
       
                # value can be either a string or a list.
                objectlist = []
                for objectname in value:    

                    if objectname not in self.objects.keys():                            
                        raise Exception("An error occurred while processing '" + attribute + "' = " + str(objectname) + "\nUnable to locate the object.")
                    else:                        
                        objectlist.append(self.objects[objectname])
                       
                # We should have a list of objects now.
                if len(objectlist) > 0:
                    self.__config(object, attribute, " ".join(objectlist))

        # Reset the relations dictionary.
        self.relations = {}
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

            if not resultdict["foundmatch"]:
                # Nope...something went wrong.                
                #raise Exception("An error occurred while processing '" + attribute + "' = " + str(value) + "\n" + str(ex.args[2]))
                raise Exception("An error occurred while processing '" + attribute + "' = " + str(value))

            # We can either use the DDN or the actual object. I'm just going with the DDN.
            object = resultdict["ddn"]
            attribute = resultdict["attribute"]
            args = {attribute: value}
            self.stc.config(object, **args)
            #print(object + "." + attribute + " = " + str(value))

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
    def __enableDataMining(self, dataminingdict): 

        fieldlist = []
        for key in dataminingdict:
            fieldlist.append(dataminingdict[key])

        self.stc.config("system1.project.ResultOptions", SaveAtEotProperties=" ".join(fieldlist))

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
        #   databasefilename - Name of the results database to be saved.
        #   datasetid        - The datasetid to use when there are multiple datasets.
        #
        # Results:
        #   N/A: Throws an exception on error.
        

        # Delete the table if it already exists. We need to create it every time, just in case additional
        # data has been added to the database.
        db.execute("DROP TABLE IF EXISTS RxEotStreamCustomResults")

        query = "CREATE TABLE RxEotStreamCustomResults AS \
                 SELECT \
                   Rx.DataSetId, \
                   Rx.PortName, \
                   Rx.ParentHnd, \
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
                   Rx.Comp16_4 = Filter.Comp16_4"

        # This command will create the table.
        db.execute(query)
        return

    #==============================================================================
    def __addTxRxEotStreamCustomResultsTable(self, db, datasetid):
        #   The database results files are missing an important table when they are
        #   first saved to disk. Normally, the Results Reporter creates this table
        #   when it opens the database, but that doesn't help us with the API.
        #   Instead, this procedure will create the missing TxRxEotStreamResults table.
        #
        # Arguments:
        #   databasefilename - Name of the results database to be saved.
        #   datasetid        - The datasetid to use when there are multiple datasets.
        #
        # Results:
        #   N/A: Throws an exception on error.

           
        # Delete the table if it already exists. We need to create it every time, just in case additional
        # data has been added to the database.
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
        query = "CREATE TABLE TxRxEotStreamCustomResults AS \
            SELECT \
            Tx.DataSetId                                      AS 'DataSetId', \
            Tx.ParentHnd                                      AS 'TxPortHandle', \
            Tx.PortName                                       AS 'Tx Port', \
            Tx.StreamId                                       AS TxStreamId, \
            Tx.FrameCount                                     AS 'Tx Frames', \
            Tx.OctetCount                                     AS 'Tx Bytes', \
            Tx.StreamBlockName                                AS 'Traffic Item', \
            Tx.ParentStreamBlock                              AS 'ParentStreamBlock', \
            COALESCE(Rx.PortName    ,'N/A')                   AS 'Rx Port', \
            COALESCE(Rx.ParentHnd   ,'N/A')                   AS 'RxPortHandle', \
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
                except Exception as e:
                    print(e)

            os.rmdir(path)
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
