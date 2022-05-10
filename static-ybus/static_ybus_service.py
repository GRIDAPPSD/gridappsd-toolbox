#!/usr/bin/env python3
# ------------------------------------------------------------------------------
# Copyright (c) 2022, Battelle Memorial Institute All rights reserved.
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
Created on Jan 6, 2022

@author: Gary Black
"""""

import sys
import os
import time
import argparse
import json
import importlib
import math
import numpy as np

from gridappsd import GridAPPSD


class StaticYbus(GridAPPSD):

    def __init__(self, gapps):
        self.Ybuses = {}

        self.gapps = gapps

        # UGH!!!  Need to create a second GridAPPSD instance in order to
        # avoid deadlock if trying to use the existing one for the queries
        # done inside the static_ybus function from the request message
        # handler. This cost me about 6 hours of hitting my head on a wall
        # before I figured it out.
        self.query_gapps = GridAPPSD()

        topic = 'goss.gridappsd.request.data.static-ybus'
        req_id = self.gapps.subscribe(topic, self)

        print('Starting Static Ybus request processing loop...', flush=True)

        while True:
            #print('Sleeping....', flush=True)
            time.sleep(0.1)

        print('Finished Static Ybus\n', flush=True)

        gapps.unsubscribe(req_id)


    def lowerUncomplex(self, YbusComplex, NodeIndex):
        YbusUncomplex = {}

        for noderow in YbusComplex:
            for nodecol,value in YbusComplex[noderow].items():
                if NodeIndex[noderow] >= NodeIndex[nodecol]:
                    if noderow not in YbusUncomplex:
                        YbusUncomplex[noderow] = {}
                    YbusUncomplex[noderow][nodecol] = (value.real, value.imag)

        return YbusUncomplex


    def on_message(self, headers, message):
        reply_to = headers['reply-to']

        if message['requestType'] == 'GET_SNAPSHOT_YBUS':
            feeder_mrid = message['feeder_id']

            if feeder_mrid not in self.Ybuses:
                mod_import = importlib.import_module('static-ybus.static_ybus')
                static_ybus_func = getattr(mod_import, 'static_ybus')
                Ybus = static_ybus_func(self.query_gapps, feeder_mrid)

                # need node indices to create lower diagonal matrix
                message = {
                  "configurationType": "YBus Export",
                  "parameters": {
                      "model_id": feeder_mrid}
                }
                results = self.query_gapps.get_response("goss.gridappsd.process.request.config", message, timeout=1200)
                nodeList = results['data']['nodeList']

                idx = 1
                NodeIndex = {}
                for obj in nodeList:
                  NodeIndex[obj.strip('\"')] = idx
                  idx += 1

                lowerUncomplex = self.lowerUncomplex(Ybus, NodeIndex)
                self.Ybuses[feeder_mrid] = lowerUncomplex
                print('Responding with newly created static Ybus for feeder_id: ' + feeder_mrid + '\n', flush=True)
            else:
                lowerUncomplex = self.Ybuses[feeder_mrid]
                print('Responding with previously created static Ybus for feeder_id: ' + feeder_mrid + '\n', flush=True)

            message = {
                'feeder_id': feeder_mrid,
                'ybus': lowerUncomplex
            }
            self.gapps.send(reply_to, message)

        else:
            message = "No valid requestType specified"
            self.gapps.send(reply_to, message)


def _main():
    # for loading modules
    if (os.path.isdir('shared')):
        sys.path.append('.')
    elif (os.path.isdir('../shared')):
        sys.path.append('..')

    os.environ['GRIDAPPSD_APPLICATION_ID'] = 'gridappsd-static-ybus-service'
    os.environ['GRIDAPPSD_APPLICATION_STATUS'] = 'STARTED'
    os.environ['GRIDAPPSD_USER'] = 'app_user'
    os.environ['GRIDAPPSD_PASSWORD'] = '1234App'

    gapps = GridAPPSD()
    assert gapps.connected

    static_ybus = StaticYbus(gapps)


if __name__ == "__main__":
    _main()

