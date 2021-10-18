
# Copyright (c) 2021, Battelle Memorial Institute All rights reserved.
# Battelle Memorial Institute (hereinafter Battelle) hereby grants permission to any person or entity
# lawfully obtaining a copy of this software and associated documentation files (hereinafter the
# Software) to redistribute and use the Software in source and binary forms, with or without modification.
# Such person or entity may use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and may permit others to do so, subject to the following conditions:
# Redistributions of source code must retain the above copyright notice, this list of conditions and the
# following disclaimers.
# Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
# the following disclaimer in the documentation and/or other materials provided with the distribution.
# Other than as used herein, neither the name Battelle Memorial Institute or Battelle may be used in any
# form whatsoever without the express written consent of Battelle.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
# BATTELLE OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
# OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
# GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
# OF THE POSSIBILITY OF SUCH DAMAGE.
# General disclaimer for use with OSS licenses
#
# This material was prepared as an account of work sponsored by an agency of the United States Government.
# Neither the United States Government nor the United States Department of Energy, nor Battelle, nor any
# of their employees, nor any jurisdiction or organization that has cooperated in the development of these
# materials, makes any warranty, express or implied, or assumes any legal liability or responsibility for
# the accuracy, completeness, or usefulness or any information, apparatus, product, software, or process
# disclosed, or represents that its use would not infringe privately owned rights.
#
# Reference herein to any specific commercial product, process, or service by trade name, trademark, manufacturer,
# or otherwise does not necessarily constitute or imply its endorsement, recommendation, or favoring by the United
# States Government or any agency thereof, or Battelle Memorial Institute. The views and opinions of authors expressed
# herein do not necessarily state or reflect those of the United States Government or any agency thereof.
#
# PACIFIC NORTHWEST NATIONAL LABORATORY operated by BATTELLE for the
# UNITED STATES DEPARTMENT OF ENERGY under Contract DE-AC05-76RL01830
# ------------------------------------------------------------------------------
"""
Created on December 1, 2021

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
from gridappsd.topics import simulation_output_topic, simulation_log_topic


class SimWrapper(object):
  def __init__(self, gapps, simulation_id, Ybus, YbusOrig, SwitchMridToNode, TransformerMridToNode):
    self.gapps = gapps
    self.simulation_id = simulation_id
    self.Ybus = Ybus
    self.YbusOrig = YbusOrig
    self.SwitchMridToNode = SwitchMridToNode
    self.TransformerMridToNode = TransformerMridToNode
    self.LastValue = {}
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
        print('simulation FINISHED!', flush=True)
        self.keepLoopingFlag = False

    else:
      msgdict = message['message']
      ts = msgdict['timestamp']
      print('simulation timestamp: ' + str(ts), flush=True)

      # Ybus processing workflow:
      # 1. Iterate over all switch and transformer mrids
      # 2. Determine if the measurement value has changed since the last measurement.
      #    If so:
      # 3. Set a flag to indicate that a new Ybus message will be published
      # 4. Iterate over all Ybus entries for the row of the node for the
      #    mrid, updating values
      # 5. For tap changes, the value multiplier for entries is the
      #    tap position ratio.  Come back and apply the multiplier a 2nd time for
      #    the diagonal entry so it is squared
      # 6. For switch changes, a closed switch should restore the starting Ybus
      #    values and an open switch should set them to (-500,500)
      # 7. After all switch and transformer mrids are processed, if the change flag
      #    is set, publish updated Ybus sending separate messages for the full Ybus
      #    (maybe just lower diagonal) and just for changed elements

      changedFlag = False

      for mrid in self.SwitchMridToNode:
        if 'value' in msgdict['measurements'][mrid]:
          value = msgdict['measurements'][mrid]['value']
          node = self.SwitchMridToNode[mrid]
          print('Found switch mrid: ' + mrid + ', node: ' + node + ', value: ' + str(value), flush=True)
          if node not in self.LastValue:
            # just set last value to the current value and call it good
            self.LastValue[node] = value
          elif value != self.LastValue[node]:
            changedFlag = True
            print('Switch value changed for node: ' + node + ', old value: ' + str(self.LastValue[node]) + ', new value: ' + str(value), flush=True)
          #else:
          #  print('Switch value NOT changed for node: ' + node + ', old value: ' + str(self.LastValue[node]) + ', new value: ' + str(value), flush=True)

        else:
          print('*** WARNING: Did not find switch mrid: ' + mrid + ' in measurement for timestamp: ' + str(ts), flush=True)

      for mrid in self.TransformerMridToNode:
        if 'value' in msgdict['measurements'][mrid]:
          value = msgdict['measurements'][mrid]['value']
          node = self.TransformerMridToNode[mrid]
          print('Found transformer mrid: ' + mrid + ', node: ' + node + ', value: ' + str(value), flush=True)
          if node not in self.LastValue:
            # just set last value to the current value and call it good
            self.LastValue[node] = value
          elif value != self.LastValue[node]:
            changedFlag = True
            print('Transformer value changed for node: ' + node + ', old value: ' + str(self.LastValue[node]) + ', new value: ' + str(value), flush=True)
          #else:
          #  print('Transformer value NOT changed for node: ' + node + ', old value: ' + str(self.LastValue[node]) + ', new value: ' + str(value), flush=True)

        else:
          print('*** WARNING: Did not find transformer mrid: ' + mrid + ' in measurement for timestamp: ' + str(ts), flush=True)

      if changedFlag:
        print('changedFlag set so I will publish updated Ybus', flush=True)
      else:
        print('changedFlag NOT set so nothing to do', flush=True)

      # Start Friday:
      # Since every switch state and tap position is part of every measurement,
      # I need to compare the last state to the current one to determine when to
      # update Ybus


def cim_export(gapps, simulation_id):
    message = {
      "configurationType":"CIM Dictionary",
      "parameters": {
        "simulation_id": simulation_id }
    }

    results = gapps.get_response('goss.gridappsd.process.request.config', message, timeout=1200)

    phaseToIdx = {'A': '.1', 'B': '.2', 'C': '.3', 's1': '.1', 's2': '.2'}

    SwitchMridToNode = {}
    TransformerMridToNode = {}
    for feeders in results['data']['feeders']:
        for meas in feeders['measurements']:
            # Pos measurement type includes both switches and regulators
            if meas['measurementType'] == 'Pos':
                if meas['ConductingEquipment_type'] == 'LoadBreakSwitch':
                  node = meas['ConnectivityNode'] + phaseToIdx[meas['phases']]
                  SwitchMridToNode[meas['mRID']] = node.upper()
                  print('Switch mrid: ' + meas['mRID'] + ', node: ' + node.upper(), flush=True)
                elif meas['ConductingEquipment_type'] == 'PowerTransformer':
                  node = meas['ConnectivityNode'] + phaseToIdx[meas['phases']]
                  TransformerMridToNode[meas['mRID']] = node.upper()
                  print('Transformer mrid: ' + meas['mRID'] + ', node: ' + node.upper(), flush=True)
                # TODO: do we need to handle LinearShuntCompensator?
                #elif meas['ConductingEquipment_type'] == 'LinearShuntCompensator':

    print('Switches:', flush=True)
    pprint.pprint(SwitchMridToNode)
    print('Transformers:', flush=True)
    pprint.pprint(TransformerMridToNode)

    return SwitchMridToNode,TransformerMridToNode


def ybus_export(gapps, feeder_mrid):
  message = {
    "configurationType": "YBus Export",
    "parameters": {
      "model_id": feeder_mrid}
  }

  results = gapps.get_response("goss.gridappsd.process.request.config", message, timeout=1200)

  idx = 1
  Nodes = {}
  for obj in results['data']['nodeList']:
    Nodes[idx] = obj.strip('\"')
    idx += 1
  pprint.pprint(Nodes)

  Ybus = {}
  for obj in results['data']['yParse']:
    items = obj.split(',')
    if items[0] == 'Row':
      continue
    if Nodes[int(items[0])] not in Ybus:
      Ybus[Nodes[int(items[0])]] = {}
    if Nodes[int(items[1])] not in Ybus:
      Ybus[Nodes[int(items[1])]] = {}
    Ybus[Nodes[int(items[0])]][Nodes[int(items[1])]] = Ybus[Nodes[int(items[1])]][Nodes[int(items[0])]] = complex(float(items[2]), float(items[3]))
  pprint.pprint(Ybus)

  return Nodes,Ybus


def ybus_save_original(Ybus, SwitchMridToNode, TransformerMridToNode):
  YbusOrig = {}

  for noderow in SwitchMridToNode.values():
    if noderow not in YbusOrig:
      YbusOrig[noderow] = {}

    for nodecol,value in Ybus[noderow].items():
      if nodecol not in YbusOrig:
        YbusOrig[nodecol] = {}

      YbusOrig[noderow][nodecol] = YbusOrig[nodecol][noderow] = value

  for noderow in TransformerMridToNode.values():
    if noderow not in YbusOrig:
      YbusOrig[noderow] = {}

    for nodecol,value in Ybus[noderow].items():
      if nodecol not in YbusOrig:
        YbusOrig[nodecol] = {}

      YbusOrig[noderow][nodecol] = YbusOrig[nodecol][noderow] = value

  #pprint.pprint(YbusOrig)

  return YbusOrig


def start(log_file, feeder_mrid, model_api_topic, simulation_id):
  global logfile
  logfile = log_file

  gapps = GridAPPSD()

  SwitchMridToNode,TransformerMridToNode = cim_export(gapps, simulation_id)

  Nodes,Ybus = ybus_export(gapps, feeder_mrid)

  # Save the starting Ybus values for all the entries that could change based
  # on switch and transformer value changes (no reason to save the values that
  # will never change)
  YbusOrig = ybus_save_original(Ybus, SwitchMridToNode, TransformerMridToNode)

  simRap = SimWrapper(gapps, simulation_id, Ybus, YbusOrig, SwitchMridToNode, TransformerMridToNode)
  conn_id1 = gapps.subscribe(simulation_output_topic(simulation_id), simRap)
  conn_id2 = gapps.subscribe(simulation_log_topic(simulation_id), simRap)

  print('Starting simulation monitoring loop....', flush=True)

  while simRap.keepLooping():
    #print('Sleeping....', flush=True)
    time.sleep(0.1)

  print('Done simulation monitoring loop', flush=True)

  gapps.unsubscribe(conn_id1)
  gapps.unsubscribe(conn_id2)

  return


def _main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--request", help="Simulation Request")
  parser.add_argument("--simid", help="Simulation ID")

  opts = parser.parse_args()
  sim_request = json.loads(opts.request.replace("\'",""))
  feeder_mrid = sim_request["power_system_config"]["Line_name"]
  simulation_id = opts.simid

  model_api_topic = "goss.gridappsd.process.request.data.powergridmodel"
  log_file = open('dynamic_ybus.log', 'w')

  start(log_file, feeder_mrid, model_api_topic, simulation_id)


if __name__ == "__main__":
  _main()

