import sys
import pprint
import pdb


sys.path.append('Z:/Projects/SET/Cisco - Spitfire/stc_gen')
sys.path.append('C:/Program Files (x86)/Spirent Communications/Spirent TestCenter 4.81/Spirent TestCenter Application/API/Python')
sys.path.append('../stc_gen')
import stc_gen

pp = pprint.PrettyPrinter(indent=2)

print("Initializing...")
#stcgen = stc_gen.StcGen(userest=True, labserverip="192.168.8.167", existingsession="join", cleanuponexit=False, verbose=True)
#stcgen = stc_gen.StcGen(userest=True, labserverip="192.168.8.232", existingsession="join", cleanuponexit=False, verbose=True)
#stcgen = stc_gen.StcGen(userest=True, labserverip="10.140.99.125", existingsession="join", cleanuponexit=False, verbose=True)
#stcgen = stc_gen.StcGen(userest=True, labserverip="10.140.99.125", existingsession="join", cleanuponexit=False)
#stcgen = stc_gen.StcGen(userest=False, labserverip="192.168.8.138", existingsession="kill")

#stcgen = stc_gen.StcGen(userest=True, labserverip="192.168.8.134", existingsession="kill", cleanuponexit=False, verbose=True)
stcgen = stc_gen.StcGen(userest=True, labserverip="192.168.8.134", existingsession="join", cleanuponexit=False, verbose=True)
#stcgen = stc_gen.StcGen(userest=True, labserverip="192.168.8.190", existingsession="join", cleanuponexit=False, verbose=True)
    
#print("Loading the configuration...")
#stcgen.loadJson("base.json")
#stcgen.loadJson("existing.json")



#stcgen.loadJson("test.json", deleteExistingConfig=True)
#stcgen.loadJson("second.json", deleteExistingConfig=False)
#stcgen.loadJson("mpls.json")
#stcgen.loadJson("IPv4_Unicast.json")
#stcgen.loadJson("fromtcc.json")
#stcgen.loadJson("ping.json")
#stcgen.loadJson("ping_one_port.json")
#stcgen.loadJson("bound_streamblock.json")
#stcgen.loadJson("./issue/IPv4_basic_multicast_working.json")



#stcgen.saveResultsDb("second.db")

#stcgen.saveConfiguration("test.tcc")

stcgen.connectAndApply()

#stcgen.waitForLinkUp()

#stcgen.waitForArpNdSuccess(timeout=40)

print("Done!")
exit()

print("Running test...")
results = stcgen.runAllTests()
pp.pprint(results)


                            # testname, 
                            #        Duration       = 60,
                            #        DurationMode   = "SECONDS",
                            #        LearningMode   = "L3",
                            #        FrameLengths   = None,
                            #        Loads          = None,
                            #        LoadUnit       = "PERCENT_LINE_RATE",
                            #        ResultModes    = ['ALL'],
                            #        parametersdict = None): 


#stcgen.connectAndApply()

#pdb.set_trace()

#stcgen.loadJson("everything.json")
#stcgen.loadJson("testingtests.json")
#stcgen.loadJson("simple.json")

#stcgen.saveResultsDb("./resultstest/dummy.db")

#results = stcgen.runAllTests()

#stcgen.runTest("Test1")
#pp.pprint(results)

#results = stcgen.getResultsDictFromDb("./results/Test2_2018-02-21_16-09-14_10_128.db", mode="STREAM")
#pp.pprint(results)

#results = stcgen.getResultsDictFromDb("./results/Test2_2018-02-22_15-18-28_100.db", mode="STREAM")
#pp.pprint(results)

# print("=========================================")
# results = stcgen.getResultsDictFromDb("./results/Test2_2018-02-22_15-18-28_100.db", mode="FLOW")
# pp.pprint(results)

# print("=========================================")
# results = stcgen.getResultsDictFromDb("./results/Test2_2018-02-23_12-42-38.db", mode="STREAM")
# pp.pprint(results)
# print("=========================================")
#results = stcgen.getResultsDictFromDb("/Users/mjefferson/Projects/Temp/resultsissue/F6_ASR9900_POD6_20180418_190316-akshanja.db", mode="STREAM")
#pp.pprint(results)

#results = stcgen.getResultsDictFromDb("./vlan.db", mode="STREAM")
#pp.pprint(results)



#results = stcgen.getPortResultsDictFromDb("./results/Test2_2018-02-23_12-42-38.db")
#pp.pprint(results)

#results = stcgen.getResultsDictFromDb("./issue/Test2_2018-03-27_13-32-25_10_128_notworking.db", mode="STREAM")
#pp.pprint(results)

#stcgen.generateCsv("./results/Test2_10_128.db")

#pdb.set_trace()

#print("Setting the location...")
#stcgen.config("project1", "name", "1.1.1.1/1/1")

#print("Commiting an error...")
#stcgen.config("project1", "blaw", "1")

#stcgen.stc.perform("CSTestSessionDisconnect", Terminate="True")

#stcgen.cleanUp()

print("Done!")
exit()

# To Do:
# Ping tests (ping gateway)
# Tests change framelength, rate, scale?
# Add functional test methods (start/stop traffic/get results)
# Mulitcast v4/v6
# BFD (on all port)
# OSPF and BGP <- basic support to start


# 802.1X

# DHCPv4 and DHCPv6

# IGMP
# MLD

# LDP (MPLS)
# RSVP

# BFD
# PIM
# BGP
# IS-IS
# OSPFv2 and v3

