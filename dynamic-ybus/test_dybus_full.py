#!/usr/bin/env python3

import sys
import time

# gridappsd-python module
from gridappsd import GridAPPSD
from gridappsd.topics import service_output_topic

Ybus = {}


def fullComplex(lowerUncomplex):
  global Ybus

  for noderow in lowerUncomplex:
    for nodecol,value in lowerUncomplex[noderow].items():
      if noderow not in Ybus:
        Ybus[noderow] = {}
      if nodecol not in Ybus:
        Ybus[nodecol] = {}
      Ybus[noderow][nodecol] = Ybus[nodecol][noderow] = complex(value[0], value[1])
  #print(Ybus, flush=True)


def ybusFullCallback(header, message):
  print('Received Ybus Full message: ' + str(message) + '\n', flush=True)
  #print('Received Ybus Full message\n', flush=True)

  fullComplex(message['ybus'])


def _main():
  global YbusInitializedFlag

  if len(sys.argv) < 2:
    usestr =  '\nUsage: ' + sys.argv[0] + ' simID\n' 
    print(usestr, flush=True)
    exit()

  simID = sys.argv[1]

  gapps = GridAPPSD()

  # subscribe to Dynamic Ybus full updates
  gapps.subscribe(service_output_topic('gridappsd-dynamic-ybus-full',
                                                 simID), ybusFullCallback)

  # request/response for snapshot Ybus
  topic = 'goss.gridappsd.request.data.dynamic-ybus.' + simID
  request = {
    "requestType": "GET_SNAPSHOT_YBUS"
  }
  print('Requesting dynamic Ybus snapshot for sim_id: ' + simID + '\n', flush=True)
  message = gapps.get_response(topic, request, timeout=90)
  print('Got dynamic Ybus snapshot response: ' + str(message) + '\n', flush=True)

  fullComplex(message['ybus'])

  while True:
    time.sleep(0.1)

  gapps.disconnect()


if __name__ == '__main__':
  _main()

