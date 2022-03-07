
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
Created on July 15, 2021

@author: Gary Black, Shiva Poudel, Rohit Jinsiwale
"""""

import sys
import time
import os
import argparse
import json
import importlib
import math
import numpy as np
from tabulate import tabulate

from gridappsd import GridAPPSD
from gridappsd import DifferenceBuilder
from gridappsd.topics import simulation_input_topic
from gridappsd.topics import simulation_output_topic


def greenCircle(colorFlag):
    return '\u001b[32m\u25cf\u001b[37m' if colorFlag else '\u25cb'


def redCircle(colorFlag):
    return '\u001b[31m\u25cf\u001b[37m' if colorFlag else '\u25cf'


def yellowCircle(colorFlag):
    return '\u001b[33m\u25cf\u001b[37m' if colorFlag else '\u25d1'


def pol2cart(rho, phi):
    return complex(rho*math.cos(phi), rho*math.sin(phi))


def cart2pol(cart):
    rho = np.sqrt(np.real(cart)**2 + np.imag(cart)**2)
    phi = np.arctan2(np.imag(cart), np.real(cart))
    return (rho, phi)


class SimSetWrapper(object):
    def __init__(self, gapps, simulation_id, Rids):
        self.gapps = gapps
        self.simulation_id = simulation_id
        self.Rids = Rids
        self.rid_idx = 0
        self.keepLoopingFlag = True
        self.publish_to_topic = simulation_input_topic(simulation_id)


    def keepLooping(self):
        return self.keepLoopingFlag


    def on_message(self, header, message):
        # TODO workaround for broken unsubscribe method
        if not self.keepLoopingFlag:
            return

        msgdict = message['message']
        ts = msgdict['timestamp']
        print('simulation timestamp: ' + str(ts), flush=True)

        rid = self.Rids[self.rid_idx]
        reg_diff = DifferenceBuilder(self.simulation_id)
        reg_diff.add_difference(rid, 'TapChanger.step', 0, 1)
        msg = reg_diff.get_message()
        print(msg)
        self.gapps.send(self.publish_to_topic, json.dumps(msg))
        reg_diff.clear()

        self.rid_idx += 1
        if self.rid_idx == len(self.Rids):
            self.keepLoopingFlag = False


class SimCheckWrapper(object):
    def __init__(self, Sinj, PNVmag, RegMRIDs, CondMRIDs, PNVmRIDs, PNVdict):
        self.Sinj = Sinj
        self.PNVmag = PNVmag
        self.RegMRIDs = RegMRIDs
        self.CondMRIDs = CondMRIDs
        self.PNVmRIDs = PNVmRIDs
        self.PNVdict = PNVdict
        self.keepLoopingFlag = True


    def keepLooping(self):
        return self.keepLoopingFlag


    def on_message(self, header, message):
        # TODO workaround for broken unsubscribe method
        if not self.keepLoopingFlag:
            return

        msgdict = message['message']
        ts = msgdict['timestamp']
        print('simulation timestamp: ' + str(ts), flush=True)
        #print(msgdict['measurements'])
        measurements = msgdict['measurements']
        #for mrid in measurements:
        #    print('simulation mrid: ' + mrid)
        #    print('simulation data: ' + str(measurements[mrid]))
        # check RegMRIDs for 0 tap positions
        allZeroFlag = True
        for mrid in self.RegMRIDs:
            meas = measurements[mrid]
            print(meas, flush=True)
            if meas['value'] != 0:
                allZeroFlag = False
                # TODO uncomment the following line to not check all mRIDs
                break

        if allZeroFlag:
            for mrid, condType, idx1, idx2 in self.CondMRIDs:
                #if idx==27 or idx==29 or idx==33 or idx==34:
                #    print('This is one of the multiple Sinj conducting equipment types for idx: ' + str(idx) + ', condType: ' + condType, flush=True)
                #if self.Sinj[idx] != 0j:
                #    print('This is the second contributor to Sinj for idx: ' + str(idx) + ', condType: ' + condType, flush=True)
                meas = measurements[mrid]
                if condType == 'EnergyConsumer':
                    if idx2 == None:
                        self.Sinj[idx1] += -1.0*pol2cart(meas['magnitude'], math.radians(meas['angle']))

                    elif idx1 in self.PNVdict and idx2 in self.PNVdict:
                        measv1 = measurements[self.PNVdict[idx1]]
                        v1 = pol2cart(measv1['magnitude'], math.radians(measv1['angle']))
                        measv2 = measurements[self.PNVdict[idx2]]
                        v2 = pol2cart(measv2['magnitude'], math.radians(measv2['angle']))
                        S12 = pol2cart(meas['magnitude'], math.radians(meas['angle']))
                        I12 = np.conj(S12/(v1-v2))
                        self.Sinj[idx1] += -v1*np.conj(I12)
                        self.Sinj[idx2] += v2*np.conj(I12)

                    else:
                        print('*** WARNING: required voltage measurements for computing nodal injection for delta load not available!')
                        sys.exit(0)

                else:
                    self.Sinj[idx1] += pol2cart(meas['magnitude'], math.radians(meas['angle']))

            for mrid, idx in self.PNVmRIDs:
                meas = measurements[mrid]
                self.PNVmag[idx] = meas['magnitude']

            self.keepLoopingFlag = False

def nodes_to_update(sparql_mgr):
    print('\nFinding dynamic Ybus nodes to track for simulation updates...', flush=True)

    phaseToIdx = {'A': '.1', 'B': '.2', 'C': '.3', 's1': '.1', 's2': '.2'}

    bindings = sparql_mgr.SwitchingEquipment_switch_names()
    switchToBuses = {}
    for obj in bindings:
      sw_name = obj['sw_name']['value']
      bus1 = obj['bus1']['value'].upper()
      bus2 = obj['bus2']['value'].upper()
      switchToBuses[sw_name] = [bus1, bus2]

    bindings = sparql_mgr.TransformerTank_xfmr_names()
    xfmrtoBuses = {}
    Buses = {}
    Phases = {}
    baseV = {}
    for obj in bindings:
      xfmr_name = obj['xfmr_name']['value']
      enum = int(obj['enum']['value'])
      if xfmr_name not in Buses:
        Buses[xfmr_name] = {}
        Phases[xfmr_name] = {}
        baseV[xfmr_name] = {}
      Buses[xfmr_name][enum] = obj['bus']['value'].upper()
      Phases[xfmr_name] = obj['phase']['value']
      baseV[xfmr_name][enum] = obj['baseV']['value']

    Nodes = {}
    for bus in Buses:
      # For regulator, the terminal base voltage should be equal
      if baseV[bus][1] == baseV[bus][2]:
        node1 = Buses[bus][1] + phaseToIdx[Phases[bus]]
        node2 = Buses[bus][2] + phaseToIdx[Phases[bus]]
        Nodes[node1] = Nodes[node2] = {}
        Nodes[node1]['conn'] = node2
        Nodes[node2]['conn'] = node1

    # Get the per capacitor Ybus contributions
    bindings = sparql_mgr.ShuntElement_cap_names()
    CapToYbusContrib = {}
    for obj in bindings:
      cap_name = obj['cap_name']['value']
      b_per_section = float(obj['b_per_section']['value'])
      CapToYbusContrib[cap_name] = complex(0.0, b_per_section)
      #print('cap_name: ' + cap_name + ', b_per_section: ' + str(b_per_section))

    feeders = sparql_mgr.cim_export()

    SwitchMridToNodes = {}
    TransformerMridToNodes = {}
    TransformerLastPos = {}
    CapacitorMridToNode = {}
    CapacitorLastValue = {}
    CapacitorMridToYbusContrib = {}

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
            node =  meas['ConnectivityNode'] + phaseToIdx[phase]
            node2 = node.upper()
            node1 = Nodes[node2]['conn']
            TransformerMridToNodes[mrid] = [node1, node2]
            TransformerLastPos[node2] = 0
            print('Transformer mrid: ' + mrid + ', nodes: ' + str(TransformerMridToNodes[mrid]), flush=True)
          elif meas['ConductingEquipment_type'] == 'LinearShuntCompensator':
            node = meas['ConnectivityNode'] + phaseToIdx[phase]
            node = node.upper()
            CapacitorMridToNode[mrid] = node
            CapacitorLastValue[node] = 1
            print('Capacitor mrid: ' + mrid + ', node: ' + node, flush=True)
            cap_name = meas['ConductingEquipment_name']
            if cap_name in CapToYbusContrib:
              CapacitorMridToYbusContrib[mrid] = CapToYbusContrib[cap_name]
              #print('Capacitor mrid: ' + mrid + ', node: ' + node + ', Ybus contribution: ' + str(CapToYbusContrib[cap_name]), flush=True)
            else:
              #print('Capacitor mrid: ' + mrid + ', node: ' + node, flush=True)
              print('*** WARNING: CIM dictionary capacitor name not found from b_per_section query: ' + cap_name, flush=True)
            #print(meas)

    #print('Switches:', flush=True)
    #pprint.pprint(SwitchMridToNodes)
    #print('Transformers:', flush=True)
    #pprint.pprint(TransformerMridToNode)
    #print('Capacitors:', flush=True)
    #pprint.pprint(CapacitorMridToNode)
    #pprint.pprint(CapacitorMridToYbusContrib)

    # Hold here for demo
    #text = input('\nWait here...')

    return SwitchMridToNodes,TransformerMridToNodes,TransformerLastPos,CapacitorMridToNode,CapacitorMridToYbusContrib,CapacitorLastValue


def start(log_file, feeder_mrid, model_api_topic, simulation_id):
    global logfile
    logfile = log_file

    SPARQLManager = getattr(importlib.import_module('shared.sparql'), 'SPARQLManager')

    gapps = GridAPPSD()

    #sparql_mgr = SPARQLManager(gapps, feeder_mrid, model_api_topic,simulation_id)
    sparql_mgr = SPARQLManager(gapps, feeder_mrid, simulation_id)

    #SwitchMridToNodes,TransformerMridToNodes,TransformerLastPos,CapacitorMridToNode,CapacitorMridToYbusContrib,CapacitorLastValue = nodes_to_update(sparql_mgr)
    Ysys={}
  # Get starting Ybus from static_ybus module
    
    ### Option 1
    #'''
    
    mod_import = importlib.import_module('static-ybus.static_ybus')
    static_ybus_func = getattr(mod_import, 'static_ybus')
    Ysys = static_ybus_func(feeder_mrid)
    #print(Ysys)
    #print(Ysys[1])
    #'''
    ### Option 2 
    '''
    Unsupported = {}

    mod_import = importlib.import_module('line_model_validator.line_model_validator')
    start_func = getattr(mod_import, 'start')
    start_func(log_file, feeder_mrid, model_api_topic, False, Ysys, Unsupported)
    #print('line_model_validator Ysys...')
    #print(Ysys)
    #line_count = 0
    #for bus1 in Ysys:
    #    line_count += len(Ysys[bus1])
    #print('\nLine_model # entries: ' + str(line_count) + '\n', flush=True)
    #print('\nLine_model # entries: ' + str(line_count) + '\n', file=logfile)

    mod_import = importlib.import_module('power_transformer_validator.power_transformer_validator')
    start_func = getattr(mod_import, 'start')
    start_func(log_file, feeder_mrid, model_api_topic, False, Ysys, Unsupported)
    #print('power_transformer_validator Ysys...')
    #print(Ysys)
    #count = 0
    #for bus1 in Ysys:
    #    count += len(Ysys[bus1])
    #xfmr_count = count - line_count
    #print('Power_transformer # entries: ' + str(xfmr_count) + '\n', flush=True)
    #print('Power_transformer # entries: ' + str(xfmr_count) + '\n', file=logfile)

    mod_import = importlib.import_module('switching_equipment_validator.switching_equipment_validator')
    start_func = getattr(mod_import, 'start')
    start_func(log_file, feeder_mrid, model_api_topic, False, Ysys, Unsupported)

    print(Ysys)
    '''
    ysysCount = 0
    for bus1 in Ysys:
        ysysCount += len(Ysys[bus1])

    Node2idx = {}
    N = 0
    for bus1 in list(Ysys):
        if bus1 not in Node2idx:
            Node2idx[bus1] = N
            N += 1
        for bus2 in list(Ysys[bus1]):
            if bus2 not in Node2idx:
                Node2idx[bus2] = N
                N += 1
    print('Node2idx size: ' + str(N))
    print('Node2idx dictionary:')
    print(Node2idx)

    sourcebus, sourcevang = sparql_mgr.sourcebus_query()
    sourcebus = sourcebus.upper()
    #print('\nquery results sourcebus: ' + sourcebus)
    #print('query results sourcevang: ' + str(sourcevang))

    bindings = sparql_mgr.nomv_query()
    #print('\nnomv query results:')
    #print(bindings)

    sqrt3 = math.sqrt(3.0)
    Vmag = {}

    for obj in bindings:
        busname = obj['busname']['value'].upper()
        nomv = float(obj['nomv']['value'])
        Vmag[busname] = nomv/sqrt3

    Vang = {}
    Vang['1'] = math.radians(0.0)
    Vang['2'] = math.radians(-120.0)
    Vang['3'] = math.radians(120.0)

    # calculate CandidateVnom
    CandidateVnom = {}
    CandidateVnomPolar = {}
    for node in Node2idx:
        bus = node[:node.find('.')]
        phase = node[node.find('.')+1:]

        # source bus is a special case for the angle
        if node.startswith(sourcebus+'.'):
            CandidateVnom[node] = pol2cart(Vmag[bus], sourcevang+Vang[phase])
            CandidateVnomPolar[node] = (Vmag[bus], math.degrees(sourcevang+Vang[phase]))
        else:
            if bus in Vmag:
                CandidateVnom[node] = pol2cart(Vmag[bus], Vang[phase])
                CandidateVnomPolar[node] = (Vmag[bus], math.degrees(Vang[phase]))
            else:
                print('*** WARNING:  no nomv value for bus: ' + bus + ' for node: ' + node)

    #print('\nCandidateVnom dictionary:')
    #print(CandidateVnom)

    src_idxs = []
    if sourcebus+'.1' in Node2idx:
        src_idxs.append(Node2idx[sourcebus+'.1'])
    if sourcebus+'.2' in Node2idx:
        src_idxs.append(Node2idx[sourcebus+'.2'])
    if sourcebus+'.3' in Node2idx:
        src_idxs.append(Node2idx[sourcebus+'.3'])
    print('\nsrc_idxs: ' + str(src_idxs))

    YsysMatrix = np.zeros((N,N), dtype=complex)
    # next, remap into a numpy array
    for bus1 in list(Ysys):
        for bus2 in list(Ysys[bus1]):
            YsysMatrix[Node2idx[bus2],Node2idx[bus1]] = YsysMatrix[Node2idx[bus1],Node2idx[bus2]] = Ysys[bus1][bus2]
    # dump YsysMatrix for MATLAB comparison
    #print('\nYsysMatrix for MATLAB:')
    #for row in range(N):
    #    for col in range(N):
    #        print(str(row+1) + ',' + str(col+1) + ',' + str(YsysMatrix[row,col].real) + ',' + str(YsysMatrix[row,col].imag))

    np.set_printoptions(threshold=sys.maxsize)
    #print('\nYsys numpy array:')
    #print(YsysMatrix)

    # create the CandidateVnom numpy vector for computations below
    CandidateVnomVec = np.zeros((N), dtype=complex)
    for node in Node2idx:
        if node in CandidateVnom:
            print('CandidateVnomVec node: ' + node + ', index: ' + str(Node2idx[node]) + ', cartesian value: ' + str(CandidateVnom[node]) + ', polar value: ' + str(CandidateVnomPolar[node]))
            CandidateVnomVec[Node2idx[node]] = CandidateVnom[node]
        else:
            print('*** WARNING: no CandidateVnom value for populating node: ' + node + ', index: ' + str(Node2idx[node]))
    #print('\nCandidateVnom:')
    #print(CandidateVnomVec)
    # dump CandidateVnomVec to CSV file for MATLAB comparison
    #print('\nCandidateVnom for MATLAB:')
    #for row in range(N):
    #    print(str(CandidateVnomVec[row].real) + ',' + str(CandidateVnomVec[row].imag))

    # time to get the source injection terms
    # first, get the dictionary of regulator ids
    bindings = sparql_mgr.regid_query()
    Rids = []
    for obj in bindings:
        Rids.append(obj['rid']['value'])
    print('\nRegulator IDs: ' + str(Rids))

    # second, subscribe to simulation output so we can start setting tap
    # positions to 0
    simSetRap = SimSetWrapper(gapps, simulation_id, Rids)
    conn_id = gapps.subscribe(simulation_output_topic(simulation_id), simSetRap)

    while simSetRap.keepLooping():
        #print('Sleeping....', flush=True)
        time.sleep(0.1)

    gapps.unsubscribe(conn_id)

    bindings = sparql_mgr.query_energyconsumer_lf()
    #print(bindings)

    phaseIdx = {'A': '.1', 'B': '.2', 'C': '.3', 's1': '.1', 's2': '.2'}
    DeltaList = []
    #print("\nDelta connected load EnergyConsumer query:")
    for obj in bindings:
        #name = obj['name']['value'].upper()
        bus = obj['bus']['value'].upper()
        conn = obj['conn']['value']
        phases = obj['phases']['value']
        #print('bus: ' + bus + ', conn: ' + conn + ', phases: ' + phases)
        if conn == 'D':
           if phases == '':
               DeltaList.append(bus+'.1')
               DeltaList.append(bus+'.2')
               DeltaList.append(bus+'.3')
           else:
               DeltaList.append(bus+phaseIdx[phases])

    PNVmag = np.zeros((N), dtype=float)

    # third, verify all tap positions are 0
    config_api_topic = 'goss.gridappsd.process.request.config'
    message = {
        'configurationType': 'CIM Dictionary',
        'parameters': {'model_id': feeder_mrid}
        }
    cim_dict = gapps.get_response(config_api_topic, message, timeout=10)
    #print('\nCIM Dictionary:')
    #print(cim_dict)
    # get list of regulator mRIDs
    RegMRIDs = []
    CondMRIDs = []
    PNVmRIDs = []
    PNVdict = {}
    condTypes = set(['EnergyConsumer', 'LinearShuntCompensator', 'PowerElectronicsConnection', 'SynchronousMachine'])
    phaseIdx2 = {'A': '.2', 'B': '.3', 'C': '.1'}

    for feeder in cim_dict['data']['feeders']:
        for measurement in feeder['measurements']:
            if measurement['name'].startswith('RatioTapChanger') and measurement['measurementType']=='Pos':
                RegMRIDs.append(measurement['mRID'])

            elif measurement['measurementType']=='VA' and (measurement['ConductingEquipment_type'] in condTypes):
                node = measurement['ConnectivityNode'].upper() + phaseIdx[measurement['phases']]
                if node in DeltaList:
                    node2 = measurement['ConnectivityNode'].upper() + phaseIdx2[measurement['phases']]
                    #print('Appending CondMRID tuple: (' + measurement['mRID'] + ', ' + measurement['ConductingEquipment_type'] + ', ' + str(Node2idx[node]) + ', ' + str(Node2idx[node2]) + ') for node: ' + node, flush=True)
                    CondMRIDs.append((measurement['mRID'], measurement['ConductingEquipment_type'], Node2idx[node], Node2idx[node2]))
                else:
                    #print('Appending CondMRID tuple: (' + measurement['mRID'] + ', ' + measurement['ConductingEquipment_type'] + ', ' + str(Node2idx[node]) + ', None) for node: ' + node, flush=True)
                    CondMRIDs.append((measurement['mRID'], measurement['ConductingEquipment_type'], Node2idx[node], None))

            elif measurement['measurementType'] == 'PNV':
                # save PNV measurements in Andy's mixing bowl for later
                node = measurement['ConnectivityNode'].upper() + phaseIdx[measurement['phases']]
                #print('Appending PNVmRID tuple: (' + measurement['mRID'] + ', ' + measurement['ConductingEquipment_type'] + ', ' + str(Node2idx[node]) + ') for node: ' + node, flush=True)
                PNVmRIDs.append((measurement['mRID'], Node2idx[node]))
                PNVdict[Node2idx[node]] = measurement['mRID']

    print('Found RatioTapChanger mRIDs: ' + str(RegMRIDs), flush=True)
    print('Found ConductingEquipment mRIDs: ' + str(CondMRIDs), flush=True)
    print('Found PNV dictionary: ' + str(PNVdict), flush=True)
    print('PNV dictionary size: ' + str(len(PNVdict)), flush=True)

    # fourth, verify tap ratios are all 0 and then set Sinj values for the
    # conducting equipment mRIDs by listening to simulation output

    # start with Sinj as zero vector and we will come back to this later
    Sinj = np.zeros((N), dtype=complex)
    Sinj[src_idxs] = complex(0.0,1.0)
    print('\nInitial Sinj:')
    print(Sinj)

    # subscribe to simulation output so we can start checking tap positions
    # and then setting Sinj
    simCheckRap = SimCheckWrapper(Sinj, PNVmag, RegMRIDs, CondMRIDs, PNVmRIDs, PNVdict)
    conn_id = gapps.subscribe(simulation_output_topic(simulation_id), simCheckRap)

    while simCheckRap.keepLooping():
        #print('Sleeping....', flush=True)
        time.sleep(0.1)

    gapps.unsubscribe(conn_id)

    print('\nFinal Sinj:')
    #print(Sinj)
    for key,value in Node2idx.items():
        print(key + ': ' + str(Sinj[value]))

    vsrc = np.zeros((3), dtype=complex)
    vsrc = CandidateVnomVec[src_idxs]
    #print('\nvsrc:')
    #print(vsrc)

    Iinj_nom = np.conj(Sinj/CandidateVnomVec)
    #print('\nIinj_nom:')
    #print(Iinj_nom)

    Yinj_nom = -Iinj_nom/CandidateVnomVec
    #print('\nYinj_nom:')
    #print(Yinj_nom)

    Yaug = YsysMatrix + np.diag(Yinj_nom)
    #print('\nYaug:')
    #print(Yaug)

    Zaug = np.linalg.inv(Yaug)
    #print('\nZaug:')
    #print(Zaug)

    tolerance = 0.01
    Nfpi = 10
    Nfpi = 15
    Isrc_vec = np.zeros((N), dtype=complex)
    Vfpi = np.zeros((N,Nfpi), dtype=complex)

    # start with the CandidateVnom for Vfpi
    Vfpi[:,0] = CandidateVnomVec
    #print('\nVfpi:')
    #print(Vfpi)

    k = 1
    maxdiff = 1.0

    while k<Nfpi and maxdiff>tolerance:
        Iload_tot = np.conj(Sinj / Vfpi[:,k-1])
        Iload_z = -Yinj_nom * Vfpi[:,k-1]
        Iload_comp = Iload_tot - Iload_z
        #print('\nIload_comp numpy matrix:')
        #print(Iload_comp)

        term1 = np.linalg.inv(Zaug[np.ix_(src_idxs,src_idxs)])
        term2 = vsrc - np.matmul(Zaug[np.ix_(src_idxs,list(range(N)))], Iload_comp)
        Isrc_vec[src_idxs] = np.matmul(term1, term2)
        #print("\nIsrc_vec:")
        #print(Isrc_vec)

        Icomp = Isrc_vec + Iload_comp
        Vfpi[:,k] = np.matmul(Zaug, Icomp)
        #print("\nVfpi:")
        #print(Vfpi)
        #print(Vfpi[:,k])

        maxlist = abs(abs(Vfpi[:,k]) - abs(Vfpi[:,k-1]))
        print("\nmaxlist:")
        for i in range(41):
          print(str(i) + ": " + str(maxlist[i]))

        maxdiff = max(abs(abs(Vfpi[:,k]) - abs(Vfpi[:,k-1])))
        print("\nk: " + str(k) + ", maxdiff: " + str(maxdiff))
        k += 1

    if k == Nfpi:
        print("\nDid not converge with k: " + str(k))
        return

    # set the final Vpfi index
    k -= 1
    print("\nconverged k: " + str(k))
    print("\nVfpi:")
    for key, value in Node2idx.items():
        rho, phi = cart2pol(Vfpi[value,k])
        print(key + ': rho: ' + str(rho) + ', phi: ' + str(math.degrees(phi)))
        print('index: ' + str(value) + ', sim mag: ' + str(PNVmag[value]))

    print("\nVfpi rho to sim magnitude CSV:")
    for key, value in Node2idx.items():
        mag = PNVmag[value]
        if mag != 0.0:
            rho, phi = cart2pol(Vfpi[value,k])
            print(str(value) + ',' + key + ',' + str(rho) + ',' + str(mag))

    return



def _main():
    # for loading modules
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

    model_api_topic = "goss.gridappsd.process.request.data.powergridmodel"
    log_file = open('ysystem_validator.log', 'w')

    start(log_file, feeder_mrid, model_api_topic, simulation_id)


if __name__ == "__main__":
    _main()