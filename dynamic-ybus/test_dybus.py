#!/usr/bin/env python3

import sys
import time

# gridappsd-python module
from gridappsd import GridAPPSD
from gridappsd.topics import service_output_topic


class DYbusTester:
  def __init__(self, gapps, simID):
    self.Ybus = {}
    self.YbusPreInit = {}
    self.timestampPreInit = 0
    self.ybusInitFlag = False

    # subscribe to Dynamic Ybus changes
    gapps.subscribe(service_output_topic('gridappsd-dynamic-ybus', simID), self)

    # request/response for snapshot Ybus
    topic = 'goss.gridappsd.request.data.dynamic-ybus.' + simID
    request = {
      "requestType": "GET_SNAPSHOT_YBUS"
    }
    print('Requesting dynamic Ybus snapshot for sim_id: ' + simID + '\n', flush=True)
    message = gapps.get_response(topic, request, timeout=90)
    print('Got dynamic Ybus snapshot response: ' + str(message) + '\n', flush=True)

    snapshotTimestamp = message['timestamp']
    if self.timestampPreInit > message['timestamp']:
      print('Dynamic Ybus initialized from an update message with timestamp: ' + str(self.timestampPreInit) + ', snapshot timestamp: ' + str(message['timestamp']) + '\n', flush=True)
      self.Ybus = self.fullComplexInit(self.YbusPreInit)
      self.YbusPreInit = {} # free up memory no longer needed
    else:
      print('Dynamic Ybus initialized from snapshot response\n', flush=True)
      self.Ybus = self.fullComplexInit(message['ybus'])

    self.ybusInitFlag = True

    # do any processing after Ybus is initialized here


  def on_message(self, header, message):
    if not self.ybusInitFlag:
      # save the most recent ybus message we get before request/response
      # initialization to compare timestamps with what comes back from that
      self.YbusPreInit = message['ybus']
      self.timestampPreInit = message['timestamp']
      print('Received pre-init Ybus update message with timestamp: ' + str(self.timestampPreInit) + '\n', flush=True)

    else:
      print("Received Ybus update message with timestamp: ' + str(message['timestamp']) + ', Ybus changes: ' + str(message['ybus_changes']) + \n", flush=True)
      self.fullComplexUpdate(message['ybus_changes'])

      # do any processing after Ybus is updated here


  def fullComplexInit(self, lowerUncomplex):
    YbusInit = {}

    for noderow in lowerUncomplex:
      for nodecol,value in lowerUncomplex[noderow].items():
        if noderow not in YbusInit:
          YbusInit[noderow] = {}
        if nodecol not in YbusInit:
          YbusInit[nodecol] = {}
        YbusInit[noderow][nodecol] = YbusInit[nodecol][noderow] = complex(value[0], value[1])
    #print(YbusInit, flush=True)

    return YbusInit


  def fullComplexUpdate(self, lowerUncomplex):
    for noderow in lowerUncomplex:
      for nodecol,value in lowerUncomplex[noderow].items():
        self.Ybus[noderow][nodecol] = self.Ybus[nodecol][noderow] = complex(value[0], value[1])
    #print(self.Ybus, flush=True)


def _main():
  if len(sys.argv) < 2:
    usestr =  '\nUsage: ' + sys.argv[0] + ' simID\n' 
    print(usestr, flush=True)
    exit()

  simID = sys.argv[1]

  gapps = GridAPPSD()

  dybus = DYbusTester(gapps, simID)

  while True:
    time.sleep(0.1)

  gapps.disconnect()


if __name__ == '__main__':
  _main()

