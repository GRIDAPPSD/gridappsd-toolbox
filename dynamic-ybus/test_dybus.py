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

  gapps.subscribe(service_output_topic('gridappsd-dynamic-ybus-full',
                                                 simID), ybusFullCallback)
  gapps.subscribe(service_output_topic('gridappsd-dynamic-ybus-changes',
                                                 simID), ybusChangesCallback)

  while True:
    time.sleep(0.1)

  gapps.disconnect()




if __name__ == '__main__':
  _main()

