
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
  def __init__(self, gapps, simulation_id, Ybus, YbusOrig, NodeIndex, SwitchMridToNodes, TransformerMridToNode, TransformerLastPos):
    self.gapps = gapps
    self.simulation_id = simulation_id
    self.Ybus = Ybus
    self.YbusOrig = YbusOrig
    self.NodeIndex = NodeIndex
    self.SwitchMridToNodes = SwitchMridToNodes
    self.TransformerMridToNode = TransformerMridToNode
    self.TransformerLastPos = TransformerLastPos
    self.keepLoopingFlag = True


  def keepLooping(self):
    return self.keepLoopingFlag


  def checkSwitchOpen(self, nodes):
    try:
      Yval = self.Ybus[nodes[0]][nodes[1]].real
      return (abs(Yval) <= 0.001)
    except:
      return True


  def checkSwitchClosed(self, nodes):
    try:
      Yval = self.Ybus[nodes[0]][nodes[1]].real
      return (Yval>=-1000.0 and Yval<=-500.0)
    except:
      return False


  def printLower(self, YbusPrint):
    for noderow in YbusPrint:
      rowFlag = False
      for nodecol,value in YbusPrint[noderow].items():
        if self.NodeIndex[noderow] >= self.NodeIndex[nodecol]:
          rowFlag = True
          #print('['+noderow+','+nodecol+']='+str(value), flush=True)
          print('['+noderow+':'+str(self.NodeIndex[noderow])+','+nodecol+':'+str(self.NodeIndex[nodecol])+']='+str(value), flush=True)
      if rowFlag:
        print('', flush=True)


  def publish(self, YbusChanges):
    print('Ybus Changes lower diagonal:', flush=True)
    self.printLower(YbusChanges)
    print('Full Ybus lower diagonal:', flush=True)
    self.printLower(self.Ybus)


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
      ts = msgdict['timestamp']
      print('Processing simulation timestamp: ' + str(ts), flush=True)

      # Questions:
      # 1. HOLD Do I need to process changes to LinearShuntCompensator
      #    equipment? Ans: Yes, need guidance from Andy. Alex said I could
      #    get a CIM dictionary value to plug into the diagonal element and this
      #    would be the extent of what to change. Not sure how this relates to
      #    new values coming from simulation output. Shiva says that the MV
      #    code, meaning static Ybus, also needs to be updated for shunt
      #    elements and he will help with that. I'm sure he can give guidance
      #    on what to do for the dynamic Ybus as well when that's done.

      # 2. HOLD Should I keep publish the full Ybus or just the
      #    lower diagonal elements (same for YbusChanges)? Ans: Just lower diag

      # 3. HOLD Do we need to publish an index number based version of Ybus
      #    vs. just the node name based version? Ans:  Don't think so as index
      #    is just an artifact of the node list order and not meaningful.
      #    Shiva thinks I should publish an index based version so need to
      #    combe back to this

      # 4. HOLD Should the ActiveMQ message format for Ybus just be the "sparse"
      #    dictionary of dictionaries? Ans: Yes

      # 5. HOLD Should the real and imaginary components of complex Ybus values
      #    be two separate floating point values in the published message
      #    instead of some complex number representation? Ans: If JSON directly
      #    supports complex number representation vs. some ugly string
      #    conversion then do it as complex.  Otherwise, separate the
      #    components into floats. Based on googling, it looks like JSON has
      #    no direct support for complex numbers so need to separate components

      # 7. HOLD Andy talked about creating a separate Ybus for each feeder and
      #    island.  Right now I have only a monolithic Ybus so need to come
      #    back and get more guidance on this.  Perhaps this is related to
      #    making dynamic YBus aware of Topology Processor as I'm not sure
      #    of the need for this otherwise

      switchOpenValue = complex(0,0)
      switchClosedValue = complex(-500,500)

      YbusChanges = {} # minimal set of Ybus changes for timestamp

      # Switch processing
      for mrid in self.SwitchMridToNodes:
        try:
          value = msgdict['measurements'][mrid]['value']
          nodes = self.SwitchMridToNodes[mrid] # two endpoints for switch
          #print('Found switch mrid: ' + mrid + ', nodes: ' + str(nodes) + ', value: ' + str(value), flush=True)
          if value == 0: # open
            if not self.checkSwitchOpen(nodes):
              print('Switch value changed from closed to open for nodes: ' + str(nodes), flush=True)
              self.Ybus[nodes[0]][nodes[1]] = self.Ybus[nodes[1]][nodes[0]] = switchOpenValue
              # Modify diagnonal terms for both endpoints
              self.Ybus[nodes[0]][nodes[0]] -= switchClosedValue
              self.Ybus[nodes[1]][nodes[1]] -= switchClosedValue

              if nodes[0] not in YbusChanges:
                YbusChanges[nodes[0]] = {}
              if nodes[1] not in YbusChanges:
                YbusChanges[nodes[1]] = {}
              YbusChanges[nodes[0]][nodes[1]] = YbusChanges[nodes[1]][nodes[0]] = switchOpenValue
              YbusChanges[nodes[0]][nodes[0]] -= switchClosedValue
              YbusChanges[nodes[1]][nodes[1]] -= switchClosedValue

          else: # closed
            if not self.checkSwitchClosed(nodes):
              print('Switch value changed from open to closed for nodes: ' + str(nodes), flush=True)
              self.Ybus[nodes[0]][nodes[1]] = self.Ybus[nodes[1]][nodes[0]] = switchClosedValue
              self.Ybus[nodes[0]][nodes[0]] += switchClosedValue
              self.Ybus[nodes[1]][nodes[1]] += switchClosedValue

              if nodes[0] not in YbusChanges:
                YbusChanges[nodes[0]] = {}
              if nodes[1] not in YbusChanges:
                YbusChanges[nodes[1]] = {}
              YbusChanges[nodes[0]][nodes[1]] = YbusChanges[nodes[1]][nodes[0]] = switchClosedValue

              if nodes[0] not in YbusChanges[nodes[0]]:
                YbusChanges[nodes[0]][nodes[0]] = switchOpenValue
              YbusChanges[nodes[0]][nodes[0]] += switchClosedValue

              if nodes[1] not in YbusChanges[nodes[1]]:
                YbusChanges[nodes[1]][nodes[1]] = switchOpenValue
              YbusChanges[nodes[1]][nodes[1]] += switchClosedValue

        except:
          if mrid not in msgdict['measurements']:
            print('*** WARNING: Did not find switch mrid: ' + mrid + ' in measurement for timestamp: ' + str(ts), flush=True)
          elif 'value' not in msgdict['measurements'][mrid]:
            print('*** WARNING: Did not find value element for switch mrid: ' + mrid + ' in measurement for timestamp: ' + str(ts), flush=True)
          else:
            print('*** WARNING: Unknown exception processing switch mrid: ' + mrid + ' in measurement for timestamp: ' + str(ts), flush=True)

      # Transformer processing
      for mrid in self.TransformerMridToNode:
        try:
          value = msgdict['measurements'][mrid]['value']
          noderow = self.TransformerMridToNode[mrid]
          #print('Found transformer mrid: ' + mrid + ', node: ' + noderow + ', value: ' + str(value), flush=True)
          if value != self.TransformerLastPos[noderow]:
            print('Transformer value changed for node: ' + noderow + ', old value: ' + str(self.TransformerLastPos[noderow]) + ', new value: ' + str(value), flush=True)
            self.TransformerLastPos[noderow] = value

            if noderow not in YbusChanges:
              YbusChanges[noderow] = {}

            # calculate the admittance multiplier based on the change in the tap
            # position vs. the original zero position
            posMultiplier = 1.0 + value*0.0625

            # update Ybus based on the multiplier
            for nodecol in self.Ybus[noderow]:
              Yval = self.YbusOrig[noderow][nodecol] * posMultiplier
              self.Ybus[noderow][nodecol] = self.Ybus[nodecol][noderow] = Yval
              if nodecol not in YbusChanges:
                YbusChanges[nodecol] = {}
              YbusChanges[noderow][nodecol] = YbusChanges[nodecol][noderow] = Yval

            # for the diagonal element square the multiplier for YbusOrig
            Yval = self.YbusOrig[noderow][nodecol] * posMultiplier**2
            self.Ybus[noderow][noderow] = Yval
            YbusChanges[noderow][noderow] = Yval

        except:
          if mrid not in msgdict['measurements']:
            print('*** WARNING: Did not find transformer mrid: ' + mrid + ' in measurement for timestamp: ' + str(ts), flush=True)
          elif 'value' not in msgdict['measurements'][mrid]:
            print('*** WARNING: Did not find value element for transformer mrid: ' + mrid + ' in measurement for timestamp: ' + str(ts), flush=True)
          else:
            print('*** WARNING: Unknown exception processing transformer mrid: ' + mrid + ' in measurement for timestamp: ' + str(ts), flush=True)

      if len(YbusChanges) > 0: # Ybus changed if there are any entries
        print('*** Ybus changed so I will publish full Ybus and minimal YbusChanges!', flush=True)
        self.publish(YbusChanges)
      else:
        print('Ybus NOT changed\n', flush=True)


def nodes_to_update(sparql_mgr):
    bindings = sparql_mgr.SwitchingEquipment_switch_names()

    switchToBuses = {}
    for obj in bindings:
      sw_name = obj['sw_name']['value']
      bus1 = obj['bus1']['value'].upper()
      bus2 = obj['bus2']['value'].upper()
      switchToBuses[sw_name] = [bus1, bus2]

    feeders = sparql_mgr.cim_export()

    phaseToIdx = {'A': '.1', 'B': '.2', 'C': '.3', 's1': '.1', 's2': '.2'}

    SwitchMridToNodes = {}
    TransformerMridToNode = {}
    TransformerLastPos = {}
    for feeder in feeders:
      for meas in feeder['measurements']:
        # Pos measurement type includes both switches and regulators
        if meas['measurementType'] == 'Pos':
          mrid = meas['mRID']
          phase = meas['phases']
          if meas['ConductingEquipment_type'] == 'LoadBreakSwitch':
            sw_name = meas['ConductingEquipment_name']
            if sw_name in switchToBuses:
              buses = switchToBuses[sw_name]
              node1 = buses[0] + phaseToIdx[phase]
              node2 = buses[1] + phaseToIdx[phase]
              SwitchMridToNodes[mrid] = [node1, node2]
              print('Switch mrid: ' + mrid + ', nodes: ' + str(SwitchMridToNodes[mrid]), flush=True)
          elif meas['ConductingEquipment_type'] == 'PowerTransformer':
            node = meas['ConnectivityNode'] + phaseToIdx[phase]
            node = node.upper()
            TransformerMridToNode[mrid] = node
            TransformerLastPos[node] = 0
            print('Transformer mrid: ' + mrid + ', node: ' + node, flush=True)
          # TODO: Handle LinearShuntCompensator?
          #elif meas['ConductingEquipment_type'] == 'LinearShuntCompensator':

    #print('Switches:', flush=True)
    #pprint.pprint(SwitchMridToNodes)
    #print('Transformers:', flush=True)
    #pprint.pprint(TransformerMridToNode)

    return SwitchMridToNodes,TransformerMridToNode,TransformerLastPos


def opendss_ybus(sparql_mgr):
  yParse,nodeList = sparql_mgr.ybus_export()

  idx = 1
  #Nodes = {}
  NodeIndex = {}
  for obj in nodeList:
    #Nodes[idx] = obj.strip('\"')
    NodeIndex[obj.strip('\"')] = idx
    idx += 1
  #pprint.pprint(Nodes)
  #pprint.pprint(NodeIndex)

  #Ybus = {}
  #for obj in yParse:
  #  items = obj.split(',')
  #  if items[0] == 'Row':
  #    continue
  #  if Nodes[int(items[0])] not in Ybus:
  #    Ybus[Nodes[int(items[0])]] = {}
  #  if Nodes[int(items[1])] not in Ybus:
  #    Ybus[Nodes[int(items[1])]] = {}
  #  Ybus[Nodes[int(items[0])]][Nodes[int(items[1])]] = Ybus[Nodes[int(items[1])]][Nodes[int(items[0])]] = complex(float(items[2]), float(items[3]))
  #pprint.pprint(Ybus)

  #return Ybus
  return NodeIndex


def ybus_save_original_xfmrs(Ybus, TransformerMridToNode):
  YbusOrig = {}

  for noderow in TransformerMridToNode.values():
    if noderow not in YbusOrig:
      YbusOrig[noderow] = {}

    for nodecol,value in Ybus[noderow].items():
      # No need to reverse nodes for full Ybus because we are iterating over
      # all elements of a full Ybus
      YbusOrig[noderow][nodecol] = value

  #pprint.pprint(YbusOrig)

  return YbusOrig


def dynamic_ybus(log_file, feeder_mrid, simulation_id):
  global logfile
  logfile = log_file

  gapps = GridAPPSD()

  SPARQLManager = getattr(importlib.import_module('shared.sparql'), 'SPARQLManager')
  sparql_mgr = SPARQLManager(gapps, feeder_mrid, simulation_id)

  SwitchMridToNodes,TransformerMridToNode,TransformerLastPos = nodes_to_update(sparql_mgr)

  # Get starting Ybus from static_ybus module
  mod_import = importlib.import_module('static-ybus.static_ybus')
  static_ybus_func = getattr(mod_import, 'static_ybus')
  Ybus = static_ybus_func(feeder_mrid)

  # Get node to index mapping from OpenDSS
  NodeIndex = opendss_ybus(sparql_mgr)

  # Save the starting Ybus values for all the entries that could change based
  # on transformer value changes (no reason to save the starting values that
  # will never change)
  YbusOrig = ybus_save_original_xfmrs(Ybus, TransformerMridToNode)

  simRap = SimWrapper(gapps, simulation_id, Ybus, YbusOrig, NodeIndex, SwitchMridToNodes, TransformerMridToNode, TransformerLastPos)
  conn_id1 = gapps.subscribe(simulation_output_topic(simulation_id), simRap)
  conn_id2 = gapps.subscribe(simulation_log_topic(simulation_id), simRap)

  print('Starting simulation monitoring loop....', flush=True)

  while simRap.keepLooping():
    #print('Sleeping....', flush=True)
    time.sleep(0.1)

  print('Finished simulation monitoring loop and Dynamic Ybus\n', flush=True)

  gapps.unsubscribe(conn_id1)
  gapps.unsubscribe(conn_id2)

  return


def _main():
  # for loading modules (this works for finding static-ybus too)
  if (os.path.isdir('shared')):
    sys.path.append('.')
  elif (os.path.isdir('../shared')):
    sys.path.append('..')
   
  parser = argparse.ArgumentParser()
  parser.add_argument("--request", help="Simulation Request")
  parser.add_argument("--simid", help="Simulation ID")

  opts = parser.parse_args()
  sim_request = json.loads(opts.request.replace("\'",""))
  feeder_mrid = sim_request["power_system_config"]["Line_name"]
  simulation_id = opts.simid

  log_file = open('dynamic_ybus.log', 'w')

  dynamic_ybus(log_file, feeder_mrid, simulation_id)


if __name__ == "__main__":
  _main()

