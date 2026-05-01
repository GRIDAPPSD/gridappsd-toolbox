"""
Created on May 1, 2026

@author: Gary Black
"""""

import sys
import time
import os
import argparse
import json
import importlib
import math
import pprint
import numpy as np

from gridappsd import GridAPPSD
from gridappsd.topics import simulation_output_topic, simulation_log_topic, service_output_topic, service_input_topic


class SimWrapper(object):
  def __init__(self, gapps, feeder_mrid, simulation_id):
    self.gapps = gapps
    self.feeder_mrid = feeder_mrid
    self.simulation_id = simulation_id
    self.timestamp = 0
    self.keepLoopingFlag = True


  def keepLooping(self):
    return self.keepLoopingFlag


  def on_message(self, header, message):
    # TODO workaround for broken unsubscribe method
    if not self.keepLoopingFlag:
      return

    if 'processStatus' in message:
      status = message['processStatus']
      if status=='COMPLETE' or status=='CLOSED':
        self.keepLoopingFlag = False

    else:
      msgdict = message['message']
      self.timestamp = msgdict['timestamp']
      print('Processing simulation timestamp: ' + str(self.timestamp), flush=True)
      #print('Simulation message: ' + str(msgdict), flush=True)


class TestSim(GridAPPSD):
  def __init__(self, gapps, feeder_mrid, simulation_id):
    SPARQLManager = getattr(importlib.import_module('shared.sparql'), 'SPARQLManager')
    sparql_mgr = SPARQLManager(gapps, feeder_mrid, simulation_id)

    gapps_sim = GridAPPSD()

    self.simRap = SimWrapper(gapps_sim, feeder_mrid, simulation_id)

    print('Subscribing to simulation output topic: ' + simulation_output_topic(simulation_id) + '\n', flush=True)
    out_id = gapps_sim.subscribe(simulation_output_topic(simulation_id), self.simRap)
    print('Subscribing to simulation log topic: ' + simulation_log_topic(simulation_id) + '\n', flush=True)
    log_id = gapps_sim.subscribe(simulation_log_topic(simulation_id), self.simRap)

    print('Starting simulation monitoring loop...\n', flush=True)

    while self.simRap.keepLooping():
      #print('Sleeping...', flush=True)
      time.sleep(0.1)

    print('Finished simulation monitoring loop.\n', flush=True)

    gapps_sim.unsubscribe(out_id)
    gapps_sim.unsubscribe(log_id)

    return


def _main():
  # for loading modules (this works for finding static-ybus too)
  if (os.path.isdir('shared')):
    sys.path.append('.')
  elif (os.path.isdir('../shared')):
    sys.path.append('..')
  elif (os.path.isdir('gridappsd-toolbox/shared')):
    sys.path.append('gridappsd-toolbox')
  else:
    sys.path.append('/gridappsd/services/gridappsd-toolbox')
   
  parser = argparse.ArgumentParser()
  parser.add_argument("simulation_id", help="Simulation ID")
  parser.add_argument("request", help="Simulation Request")
  opts = parser.parse_args()

  sim_request = json.loads(opts.request.replace("\'",""))
  feeder_mrid = sim_request["power_system_config"]["Line_name"]
  simulation_id = opts.simulation_id

  # authenticate with GridAPPS-D Platform
  os.environ['GRIDAPPSD_APPLICATION_ID'] = 'gridappsd-dynamic-ybus-service'
  os.environ['GRIDAPPSD_APPLICATION_STATUS'] = 'STARTED'
  os.environ['GRIDAPPSD_USER'] = 'app_user'
  os.environ['GRIDAPPSD_PASSWORD'] = '1234App'

  gapps = GridAPPSD(simulation_id)
  assert gapps.connected

  test_sim = TestSim(gapps, feeder_mrid, simulation_id)


if __name__ == "__main__":
  _main()

