
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
import numpy as np

from gridappsd import GridAPPSD
from gridappsd.topics import simulation_output_topic, simulation_log_topic


class SimWrapper(object):
  def __init__(self, gapps, simulation_id):
    self.gapps = gapps
    self.simulation_id = simulation_id
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


def ybus_export(gapps, feeder_mrid):
  message = {
  "configurationType": "YBus Export",
  "parameters": {
    "model_id": feeder_mrid}
  }

  results = gapps.get_response("goss.gridappsd.process.request.config", message, timeout=1200)
  return results['data']['yParse'],results['data']['nodeList']


def start(log_file, feeder_mrid, model_api_topic, simulation_id):
  global logfile
  logfile = log_file

  gapps = GridAPPSD()

  ysparse,nodelist = ybus_export(gapps, feeder_mrid)

  idx = 1
  nodes = {}
  for obj in nodelist:
    nodes[idx] = obj.strip('\"')
    idx += 1
  print(nodes)

  Ybus = {}
  for obj in ysparse:
    items = obj.split(',')
    if items[0] == 'Row':
      continue
    if nodes[int(items[0])] not in Ybus:
      Ybus[nodes[int(items[0])]] = {}
    if nodes[int(items[1])] not in Ybus:
      Ybus[nodes[int(items[1])]] = {}
    Ybus[nodes[int(items[0])]][nodes[int(items[1])]] = Ybus[nodes[int(items[1])]][nodes[int(items[0])]] = complex(float(items[2]), float(items[3]))
  print(Ybus)

  simRap = SimWrapper(gapps, simulation_id)
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

