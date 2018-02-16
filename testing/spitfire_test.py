import sys
import pprint
import pdb

sys.path.append('/Users/mjefferson/Projects/SET/Cisco - Spitfire/stc_gen')
import stc_gen

pp = pprint.PrettyPrinter(indent=2)

stcgen = stc_gen.StcGen(userest=True, labserverip="192.168.8.138", existingsession="join", terminateonexit=True)
    
#stcgen.loadJson("sample_input.json")
#stcgen.loadJson("IPv4_Unicast.json")
#stcgen.loadJson("fromtcc.json")
#stcgen.loadJson("ping.json")
#stcgen.loadJson("ping_one_port.json")
stcgen.loadJson("everything.json")

#stcgen.runAllTests()

#stcgen.generateCsv("./results/Test2.db")

#pdb.set_trace()

#print("Setting the location...")
#stcgen.config("project1", "name", "1.1.1.1/1/1")

#print("Commiting an error...")
#stcgen.config("project1", "blaw", "1")

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

