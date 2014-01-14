#!/bin/env python
# Polls the Condor collector for some data and outputs it in collectd format
# L.B. 13-Jan-2014
# usage: ./collect_condor.py [condor collector hostname]

### notes:
#  the whole code needs to be rewritten to accept multiple collectors
#  simultaneously. since each collector daemon is on a separate server, i
#  don't see why we can't parallelize this. but we would first need to figure
#  out where the bottleneck is. likely my crappy code.

# Import some standard python utilities
import sys, time, argparse
import classad, htcondor # requires condor 7.9.5+

## Parse our arguments
parser = argparse.ArgumentParser(description="Poll HTCondor collector for information and dump into redis")
parser.add_argument("collector", help="address of the HTCondor collector")
args = parser.parse_args()

# Connect to condor collector and grab some data
coll = htcondor.Collector(args.collector)

collectd_interval = 10

while True: 
  # query the schedd, return classad
  slotState = coll.query(htcondor.AdTypes.Startd, "true",['Name','RemoteGroup','NodeOnline','JobId','State','RemoteOwner','COLLECTOR_HOST_STRING'])

  identifier_string = args.collector + "/htcondor/"

  # initialize values
  slot_owner = 0
  slot_unclaimed = 0
  slot_claimed = []   

  timestamp = str(int(time.time()))
  for slot in slotState[:]:
    if (slot['State'] == "Owner"):  ## If slot is in owner state there is no RemoteOwner or RemoteGroup
      value = ["nil",slot['NodeOnline'],slot['State'],"nil","nil",slot['COLLECTOR_HOST_STRING']]
      slot_owner += 1 
    elif (slot['State'] == "Unclaimed"): 
      value = ["nil",slot['NodeOnline'],slot['State'],"nil","nil",slot['COLLECTOR_HOST_STRING']]
      slot_unclaimed += 1
    elif (slot['State'] == "Claimed"): 
      #value = [slot['JobId'],slot['NodeOnline'],slot['State'],slot['RemoteOwner'],slot['RemoteGroup'],slot['COLLECTOR_HOST_STRING']]
      #print timestamp + "," + value[3].split("@")[0] + " " + "\"" + identifier_string + "\""
      #print "PUTVAL " + "\"" + identifier_string + "\"" + "interval=" + collectd_interval + "N:U"
      slot_claimed.append(slot['RemoteOwner'].split("@")[0])

  # since owner and unclaimed are special cases, print it
  print "PUTVAL " + "\"" + identifier_string + "slot_owner" "\"" + " interval=" + str(collectd_interval) + " N:" + str(slot_owner)
  print "PUTVAL " + "\"" + identifier_string + "slot_unclaimed" "\"" + " interval=" + str(collectd_interval) + " N:" + str(slot_unclaimed)

  # create a dict that counts the # of claimed slots of each unique user
  # this is probably stupid and better done in the above for loop. whatever.
  user_set = set(slot_claimed)
  freq = {}
  for user in user_set:
    freq[user] = slot_claimed.count(user)
  for slot_user,count in freq.items():
    print "PUTVAL " + "\"" + identifier_string + "slot_claimed_" + slot_user + " interval=" + str(collectd_interval) +  " N:" + str(count)
  time.sleep(collectd_interval)
