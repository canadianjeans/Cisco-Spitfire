import sys
import pprint
import pdb


sys.path.append('Z:/Projects/SET/Cisco - Spitfire/stc_gen')
sys.path.append('C:/Program Files (x86)/Spirent Communications/Spirent TestCenter 4.81/Spirent TestCenter Application/API/Python')
import stc_gen

pp = pprint.PrettyPrinter(indent=2)

#stcgen = stc_gen.StcGen(userest=True, labserverip="192.168.8.138", existingsession="join")
#stcgen = stc_gen.StcGen(userest=True, labserverip="10.140.99.125", existingsession="join", cleanuponexit=False, verbose=True)
stcgen = stc_gen.StcGen(userest=False, labserverip="192.168.8.138", existingsession="kill")
    
#stcgen.loadJson("sample_input.json")
#stcgen.loadJson("IPv4_Unicast.json")
#stcgen.loadJson("fromtcc.json")
#stcgen.loadJson("ping.json")
#stcgen.loadJson("ping_one_port.json")

#stcgen.loadJson("everything.json")
#stcgen.loadJson("testingtests.json")
#stcgen.loadJson("simple.json")

#stcgen.saveResultsDb("./resultstest/dummy.db")

#results = stcgen.runAllTests()
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
# results = stcgen.getResultsDictFromDb("./results/Test2_2018-02-23_12-42-38.db", mode="STREAMBLOCK")
# pp.pprint(results)


#results = stcgen.getPortResultsDictFromDb("./results/Test2_2018-02-23_12-42-38.db")
#pp.pprint(results)

#results = stcgen.getResultsDictFromDb("./results")
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

