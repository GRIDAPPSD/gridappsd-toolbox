#!/usr/bin/env python3

import sys
import time

# gridappsd-python module
from gridappsd import GridAPPSD
from gridappsd.topics import simulation_output_topic, service_output_topic


def ybusFullCallback(header, message):
  print('Received Ybus Full message: ' + str(message) + '\n', flush=True)


def ybusChangesCallback(header, message):
  print('Received Ybus Changes message: ' + str(message) + '\n', flush=True)


def _main():
  if len(sys.argv) < 2:
    usestr =  '\nUsage: ' + sys.argv[0] + ' simID\n' 
    print(usestr, flush=True)
    exit()

  simID = sys.argv[1]

  gapps = GridAPPSD()

  # subscribe to all the Dynamic Ybus subscriptions
  gapps.subscribe(service_output_topic('gridappsd-dynamic-ybus-full',
                                                 simID), ybusFullCallback)
  gapps.subscribe(service_output_topic('gridappsd-dynamic-ybus-changes',
                                                 simID), ybusChangesCallback)

  # request/response for snapshot Ybus
  topic = 'goss.gridappsd.request.data.ybus.' + simID
  request = {
    "requestType": "GET_SNAPSHOT_YBUS"
  }
  message = gapps.get_response(topic, request, timeout=90)
  print('Got Ybus Snapshot response: ' + str(message) + '\n', flush=True)

  while True:
    time.sleep(0.1)

  gapps.disconnect()




if __name__ == '__main__':
  _main()

