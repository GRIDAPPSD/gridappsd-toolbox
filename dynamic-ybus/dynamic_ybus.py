
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
    self.OrigTapPos = {}
    self.keepLoopingFlag = True


  def keepLooping(self):
    return self.keepLoopingFlag


  def checkSwitchOpen(self, noderow):
    Yval = self.Ybus[noderow][noderow].real
    return (abs(Yval) <= 0.001)


  def checkSwitchClosed(self, noderow):
    Yval = self.Ybus[noderow][noderow].real
    return (Yval>=-1000.0 and Yval<=-500.0)


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

      # Questions:
      # 1. HOLD Do I need to process changes to LinearShuntCompensator conducting
      #    equipment? Ans: Yes, need guidance from Andy. Alex said I could
      #    get a CIM dictionary value to plug into the diagonal element and this
      #    would be the extent of what to change. Not sure how this relates to
      #    new values coming from simulation output.

      # 2. DONE Should I keep track of and publish the full Ybus or just the
      #    lower diagonal elements (same for YbusChanges)? Ans: Just lower diag

      # 3. DONE Do we need to keep/publish an index number based version of Ybus
      #    vs. just the node name based version? Ans:  Don't think so as index
      #    is just an artifact of the node list order and not meaningful

      # 4. HOLD Should the ActiveMQ message format for Ybus just be the "sparse"
      #    dictionary of dictionaries? Ans: Yes

      # 5. HOLD Should the real and imaginary components of complex Ybus values be
      #    given as two separate floating point values in the published message
      #    instead of some complex number representation? Ans: If JSON directly
      #    supports complex number representation vs. some ugly string
      #    conversion then do it as complex.  Otherwise, separate the
      #    components into floats. Based on googling, it looks like JSON has
      #    no direct support for complex numbers so need to separate components

      # 6. Need verification of what I'm updating in Ybus and if the values I'm
      #    updating to are correct for switches and transformers.  Ans:  Need
      #    lots of guidance from Andy to straighten out what do to both for
      #    switches and transformers.

      # 7. Aren't I missing at least updates to impacted nodes from switch state
      #    changes by only processing the Ybus row and not using Top Processor
      #    for keeping track of what nodes are controlled by a switch?  Doesn't
      #    just updating the same Ybus row only get the direct connections to
      #    the switch node where it could deenergize much beyond that?
      #    Ans: Yes, need TP awareness eventually.  Andy talked about creating
      #    a separate Y-bus for each feeder and island. Need more guidance.

      # 8. Is my approach of initializing the switch states and tap positions
      #    based on the first simulation output message legitimate or do I
      #    really need another way to do this?  For switches, I assume this
      #    would be that all switches start closed, but not sure on regulator
      #    tap positions.  Ans: tap positions all start at zero so I need one
      #    published Y-bus message just to establish the initial state even
      #    though it's not a change per se. Also, for open switches insert
      #    (0,0), closed is (-500,500)
      #
      #    DONE Poorva thinks it would be better to use the alarm service than
      #    determining changes from simulation output.  She said that the new
      #    switch state is part of the alarm message and that if tap position
      #    wasn't already, it could be easily added.  Andy thinks that being
      #    this reliant on the alarm service though isn't justified for this
      #    tool and we should stick with usingsimulation output.
      #
      #    DONE From Dec 13 meeting with Andy, he advised using the intial Y-bus
      #    that's determined from the Model Validator CIM Y-bus code to use as
      #    the basis for initial values for both switches and tap positions.
      #    For switches at least, this seems doable.  I can look at the Y-bus
      #    value to determine if the switch starts out open or closed based on
      #    the value.  Then I can continue to do this instead of keeping a
      #    "last value" dictionary to determine when I need to update Y-bus
      #    values.  Andy suggested if the values indicate a closed switch, but
      #    says it's -1000 instead of -500, that I update it to be -500.  This
      #    would only be at the start though and maybe never if I use the MV
      #    CIM code.

      #    I'm still not sure how this would work with regulator tap
      #    positions so code it for switches first to figure out the
      #    implications for tap positions and whether I need to ask some
      #    questions.
      #
      #    Other Andy guidance:
      #    * DONE Use try/except instead of checking 'value' in measurement.
      #      Also, it's not guaranteed that the mrid will exist either and this
      #      will catch that
      #    * My LastValue logic will go away and I can hopefully always use the
      #      same logic to determine changes both the first time through and
      #      after that.  Andy did figure out we'd need a last tap position
      #      though so this is not the same for regulators.
      #    * Get this working first for a monolithic Y-bus and then later come
      #      back to consider a separate Y-bus per feeder and island.  This
      #      version might need to be topology processor aware.

      YbusChanges = {}
      # Check if there are any entries in YbusChanges to see if anything changed

      switchOpenValue = complex(0,0)
      switchClosedValue = complex(-500,500)

      for mrid in self.SwitchMridToNode:
        try:
          value = msgdict['measurements'][mrid]['value']
          noderow = self.SwitchMridToNode[mrid]
          print('Found switch mrid: ' + mrid + ', node: ' + noderow + ', value: ' + str(value), flush=True)
          if value == 0: # open
            if not checkSwitchOpen(noderow):
              self.Ybus[noderow][noderow] = switchOpenValue
              if noderow not in YbusChanges:
                YbusChanges[noderow] = {}
              YbusChanges[noderow][noderow] = switchOpenValue

              # TODO Figure out if the nodes in the same row need to be updated
              # to decide whether the logic below is needed
              # If it is, I can eliminate the 4 lines above because this will
              # also update Ybus[noderow][noderow]
              #for nodecol in self.Ybus[noderow]:
              #  self.Ybus[noderow][nodecol] = switchOpenValue
              #  if noderow not in YbusChanges:
              #    YbusChanges[noderow] = {}
              #  YbusChanges[noderow][nodecol] = switchOpenValue

          else: # closed
            if not checkSwitchClosed(noderow):
              self.Ybus[noderow][noderow] = switchClosedValue
              if noderow not in YbusChanges:
                YbusChanges[noderow] = {}
              YbusChanges[noderow][noderow] = switchClosedValue

              # TODO Figure out if the nodes in the same row need to be updated
              # to decide whether the logic below is needed and, if so, whether
              # resetting to the original value is the right logic.  Seems like
              # this could lead to issues if the other nodes are transformers
              # or switches themselves
              #for nodecol in self.Ybus[noderow]:
              #  if nodecol != noderow:
              #    self.Ybus[noderow][nodecol] = self.YbusOrig[noderow][nodecol]
              #    YbusChanges[noderow][nodecol] = self.YbusOrig[noderow][nodecol]

        except:
          if mrid not in msgdict['measurements']:
            print('*** WARNING: Did not find switch mrid: ' + mrid + ' in measurement for timestamp: ' + str(ts), flush=True)
          elif 'value' not in msgdict['measurements'][mrid]:
            print('*** WARNING: Did not find value element for switch mrid: ' + mrid + ' in measurement for timestamp: ' + str(ts), flush=True)
          else:
            print('*** WARNING: Unknown exception processing switch mrid: ' + mrid + ' in measurement for timestamp: ' + str(ts), flush=True)

      # START HERE
      # For transformers I think I should start by assuming tap position is 0
      # and saving away the original value for this.  If this isn't the case
      # then hopefully I can devise a query to get the starting position or
      # somehow calculate it.  Anyway, with this some of my existing logic
      # below will be useful to get the new values and at least for transformers
      # I do know I need to update the other columns for the row
      # Either save away the initial tap position number so I can use that in
      # my calcuation to update the value or I could "normalize" the original
      # Ybus values to figuring out what it would be for position 0 and set
      # that as the original Ybus value so I don't need to bring the original
      # tap position number into the calculation

      for mrid in self.TransformerMridToNode:
        try:
          value = msgdict['measurements'][mrid]['value']
          noderow = self.TransformerMridToNode[mrid]
          print('Found transformer mrid: ' + mrid + ', node: ' + noderow + ', value: ' + str(value), flush=True)
          if noderow not in self.LastValue:
            # set last value to the current value and save the original tap position
            # to determine the admittance multiplier
            self.LastValue[noderow] = self.OrigTapPos[noderow] = value
          elif value != self.LastValue[noderow]:
            changedFlag = True
            print('Transformer value changed for node: ' + noderow + ', old value: ' + str(self.LastValue[noderow]) + ', new value: ' + str(value), flush=True)
            self.LastValue[noderow] = value # update last value with current value

            if noderow not in YbusChanges:
              YbusChanges[noderow] = {}

            # calculate the admittance multiplier based on the change in the tap
            # position vs. the original position
            tapPosMultiplier = 1.0 + (value - self.OrigTapPos[noderow])*0.0625

            # update Ybus based on the multiplier
            for nodecol in self.Ybus[noderow]:
              self.Ybus[noderow][nodecol] = self.YbusOrig[noderow][nodecol] * tapPosMultiplier
              YbusChanges[noderow][nodecol] = self.YbusOrig[noderow][nodecol] * tapPosMultiplier

            # for the diagonal element square the multiplier for YbusOrig
            self.Ybus[noderow][noderow] = self.YbusOrig[noderow][noderow] * tapPosMultiplier**2
            YbusChanges[noderow][noderow] = self.YbusOrig[noderow][noderow] * tapPosMultiplier**2

          #else:
          #  print('Transformer value NOT changed for node: ' + noderow + ', old value: ' + str(self.LastValue[noderow]) + ', new value: ' + str(value), flush=True)

        except:
          if mrid not in msgdict['measurements']:
            print('*** WARNING: Did not find transformer mrid: ' + mrid + ' in measurement for timestamp: ' + str(ts), flush=True)
          elif 'value' not in msgdict['measurements'][mrid]:
            print('*** WARNING: Did not find value element for transformer mrid: ' + mrid + ' in measurement for timestamp: ' + str(ts), flush=True)
          else:
            print('*** WARNING: Unknown exception processing transformer mrid: ' + mrid + ' in measurement for timestamp: ' + str(ts), flush=True)

      if len(YbusChanges) > 0:
        print('*** Ybus changed so I will publish full Ybus and minimal YbusChanges!', flush=True)
      else:
        print('Ybus NOT changed so nothing to do', flush=True)


def nodes_to_watch(sparql_mgr):
    feeders = sparql_mgr.cim_export()

    phaseToIdx = {'A': '.1', 'B': '.2', 'C': '.3', 's1': '.1', 's2': '.2'}

    SwitchMridToNode = {}
    TransformerMridToNode = {}
    for feeder in feeders:
        for meas in feeder['measurements']:
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
                # TODO: Handle LinearShuntCompensator?
                #elif meas['ConductingEquipment_type'] == 'LinearShuntCompensator':

    print('Switches:', flush=True)
    pprint.pprint(SwitchMridToNode)
    print('Transformers:', flush=True)
    pprint.pprint(TransformerMridToNode)

    return SwitchMridToNode,TransformerMridToNode


def opendss_ybus(sparql_mgr):
  yParse,nodeList = sparql_mgr.ybus_export()

  idx = 1
  Nodes = {}
  for obj in nodeList:
    Nodes[idx] = obj.strip('\"')
    idx += 1
  pprint.pprint(Nodes)

  Ybus = {}
  for obj in yParse:
    items = obj.split(',')
    if items[0] == 'Row':
      continue
    if Nodes[int(items[0])] not in Ybus:
      Ybus[Nodes[int(items[0])]] = {}
    Ybus[Nodes[int(items[0])]][Nodes[int(items[1])]] = complex(float(items[2]), float(items[3]))
  pprint.pprint(Ybus)

  return Ybus


def ybus_save_original(Ybus, SwitchMridToNode, TransformerMridToNode):
  YbusOrig = {}

  for noderow in SwitchMridToNode.values():
    if noderow not in YbusOrig:
      YbusOrig[noderow] = {}

    for nodecol,value in Ybus[noderow].items():
      # could store the value with row and col reversed as well, but don't
      # currently need it
      YbusOrig[noderow][nodecol] = value

  for noderow in TransformerMridToNode.values():
    if noderow not in YbusOrig:
      YbusOrig[noderow] = {}

    for nodecol,value in Ybus[noderow].items():
      # could store the value with row and col reversed as well, but don't
      # currently need it
      YbusOrig[noderow][nodecol] = value

  #pprint.pprint(YbusOrig)

  return YbusOrig


def ybus_save_original_xfmrs(Ybus, TransformerMridToNode):
  YbusOrig = {}

  for noderow in TransformerMridToNode.values():
    if noderow not in YbusOrig:
      YbusOrig[noderow] = {}

    for nodecol,value in Ybus[noderow].items():
      # could store the value with row and col reversed as well, but don't
      # currently need it
      YbusOrig[noderow][nodecol] = value

  #pprint.pprint(YbusOrig)

  return YbusOrig


def dynamic_ybus(log_file, feeder_mrid, simulation_id):
  global logfile
  logfile = log_file

  gapps = GridAPPSD()

  SPARQLManager = getattr(importlib.import_module('shared.sparql'), 'SPARQLManager')
  sparql_mgr = SPARQLManager(gapps, feeder_mrid, simulation_id)

  SwitchMridToNode,TransformerMridToNode = nodes_to_watch(sparql_mgr)

  # Get starting Ybus from static_ybus module
  mod_import = importlib.import_module('static-ybus.static_ybus')
  static_ybus_func = getattr(mod_import, 'static_ybus')
  Ybus = static_ybus_func(feeder_mrid)
  # Get starting Ybus from OpenDSS
  #Ybus = opendss_ybus(sparql_mgr)

  # Save the starting Ybus values for all the entries that could change based
  # on switch and transformer value changes (no reason to save the values that
  # will never change)
  # TODO use first version if admittances will be changed for nodes in the
  # same Ybus row of a switch will be changed and second call if only the
  # switch node itself will be changed
  YbusOrig = ybus_save_original(Ybus, SwitchMridToNode, TransformerMridToNode)
  #YbusOrig = ybus_save_original_xfmrs(Ybus, TransformerMridToNode)

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

