
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
Created on June 6, 2022

@author: Rohit Jinsiwale
"""""

#from platform import freedesktop_os_release
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
from gridappsd.topics import service_output_topic, service_input_topic,simulation_log_topic


def pol2cart(rho, phi):
    return complex(rho*math.cos(phi), rho*math.sin(phi))

def cart2pol(cart):
    rho = np.sqrt(np.real(cart)**2 + np.imag(cart)**2)
    phi = np.arctan2(np.imag(cart), np.real(cart))
    return (rho, phi)

class SimWrapper(object):
    def __init__(self,gapps,feeder_mrid,simulation_id,PFobject):
        self.gapps=gapps
        self.feeder_mrid=feeder_mrid
        self.simulation_id=simulation_id
        self.timestamp=0
        #self.Ybusinit=False
        self.publish_to_topic=service_output_topic('gridappsd-power-flow',simulation_id)
        self.keepLoopingFlag=True
        self.PFobject=PFobject
        self.newchange=False
    
    def on_message(self,header,message):
        if not self.keepLoopingFlag:
            return
        
        
        if 'processStatus' in message:
            status = message['processStatus']
            if status=='COMPLETE' or status=='CLOSED':
                self.keepLoopingFlag = False
                message={
                    'feeder_id':self.feeder_mrid,
                    'simulation_id':self.simulation_id,
                    'processStatus':status
                }
                self.gapps.send(self.publish_to_topic,message)
                print('\nStatus published message:',flush=True)
                print(message,flush=True)
                print('')
        
        else:
            
            msgdict=message['message']
            self.timestamp=msgdict['timestamp']
            print('Processing simulation timestamp: '+str(self.timestamp),flush=True)
            if self.PFobject.PFcomplete and self.newchange:
                message={
                    'feeder_id':self.feeder_mrid,
                    'simulation_id':self.simulation_id,
                    'timestamp':self.timestamp,
                    'powerflow':self.PFobject.PFsolution
                }
                self.gapps.send(self.publish_to_topic,message)
                print('\nPower Flow published message:',flush=True)
                print(message,flush=True)
                print('')
            self.newchange=False

        



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
        
        measurements = msgdict['measurements']
        
        # check RegMRIDs for 0 tap positions
        for mrid, condType, idx1, idx2 in self.CondMRIDs:
                
            meas = measurements[mrid]
                #print(meas)
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

class PowerFlow(object):
    
    def __init__(self,gapps,feeder_mrid, simulation_id):
        
        self.Ylast={}
        self.SimNotComplete=True
        self.feeder_mrid=feeder_mrid
        self.simulation_id=simulation_id
        self.Ybusinit=False
        self.PFcomplete=False
        print('Requesting Ybus from Dynamic-Ybus Service\n')
        gapps=GridAPPSD()
        gapps.subscribe(service_output_topic('gridappsd-dynamic-ybus', simulation_id),self.ybusChangesCallback)
        request = {
        "requestType": "GET_SNAPSHOT_YBUS"
        }
        topic = 'goss.gridappsd.request.data.dynamic-ybus.' + simulation_id
        message = gapps.get_response(topic, request, timeout=90)
        self.Ylast=self.tupletocomplex(message['ybus'])

        gapps_sim=GridAPPSD()

        self.simtrack=SimWrapper(gapps_sim,self.feeder_mrid,self.simulation_id,self)
        topic = 'goss.gridappsd.request.data.power-flow.' + simulation_id
        req_id=gapps.subscribe(topic,self)
        
        out_id=gapps_sim.subscribe(simulation_output_topic(self.simulation_id),self.simtrack)
        log_id=gapps_sim.subscribe(simulation_log_topic(self.simulation_id),self.simtrack)

        if(len(self.Ylast)>0):
            self.Ybusinit=True
            print('Ybus has been initialized')
        
        print('Solving Power Flow\n')
        self.runpowerflow()

        while self.SimNotComplete:
            time.sleep(0.1)
        
        gapps.unsubscribe(req_id)
        gapps_sim.unsubscribe(out_id)
        gapps_sim.unsubscribe(log_id)
        gapps.disconnect()

        return
        

    def tupletocomplex(self,Ydyn):
        
        for bus1 in list(Ydyn):
            for bus2 in list(Ydyn[bus1]):
                a=Ydyn[bus1][bus2][0]
                b=Ydyn[bus1][bus2][1]
                Ydyn[bus1][bus2]=complex(a,b)
        return Ydyn

    def ybusChangesCallback(self,header, message):
    #print(message)
        if 'processStatus' in message:
            if message['processStatus']=='COMPLETE' or message['processStatus']=='CLOSED':
                
                self.SimNotComplete = False
        else:
                
                if self.Ybusinit:
                    Ychanges=self.tupletocomplex(message['ybusChanges'])
                    print('Recieved changes to the Ybus from the Dynamic Y-bus service at time stamp '+ str(message['timestamp'])+'\n')
                    print(Ychanges)
                    print('\n\n')
                    for bus1 in Ychanges:
                        for bus2 in Ychanges[bus1]:
                            self.Ylast[bus1][bus2]=Ychanges[bus1][bus2]
                    #print('Recieved Ybus update at time stamp'+ str(message['timestamp']))
                    print('Re-solving power flow for updated system')
                    
                    self.runpowerflow()

                    self.simtrack.newchange=True
                    
                    
    
    def runpowerflow(self):
        
        if not self.Ybusinit:
            return
        
        SPARQLManager = getattr(importlib.import_module('shared.sparql'), 'SPARQLManager')

        gapps = GridAPPSD()
        
        sparql_mgr = SPARQLManager(gapps, self.feeder_mrid, self.simulation_id)
        
        Node2idx = {}
        N = 0
        for bus1 in list(self.Ylast):
            if bus1 not in Node2idx:
                Node2idx[bus1] = N
                N += 1
            for bus2 in list(self.Ylast[bus1]):
                if bus2 not in Node2idx:
                    Node2idx[bus2] = N
                    N += 1
        #print('Node2idx size: ' + str(N))
        #print('Node2idx dictionary:')
        #print(Node2idx)

        sourcebus, sourcevang = sparql_mgr.sourcebus_query()
        sourcebus = sourcebus.upper()
        
        bindings = sparql_mgr.nomv_query()
        
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
        #print('\nsrc_idxs: ' + str(src_idxs))

        YsysMatrix = np.zeros((N,N), dtype=complex)
        # next, remap into a numpy array
        for bus1 in list(self.Ylast):
            for bus2 in list(self.Ylast[bus1]):
                YsysMatrix[Node2idx[bus2],Node2idx[bus1]] = YsysMatrix[Node2idx[bus1],Node2idx[bus2]] = self.Ylast[bus1][bus2]
        
        np.set_printoptions(threshold=sys.maxsize)
        
        # create the CandidateVnom numpy vector for computations below
        CandidateVnomVec = np.zeros((N), dtype=complex)
        for node in Node2idx:
            if node in CandidateVnom:
                #print('CandidateVnomVec node: ' + node + ', index: ' + str(Node2idx[node]) + ', cartesian value: ' + str(CandidateVnom[node]) + ', polar value: ' + str(CandidateVnomPolar[node]))
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
            if conn == 'A':
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
            'parameters': {'model_id': self.feeder_mrid}
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
                #if measurement['name'].startswith('RatioTapChanger') and measurement['measurementType']=='Pos':
                #    RegMRIDs.append(measurement['mRID'])

                if measurement['measurementType']=='VA' and (measurement['ConductingEquipment_type'] in condTypes):
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

        #print('Found RatioTapChanger mRIDs: ' + str(RegMRIDs), flush=True)
        #print('Found ConductingEquipment mRIDs: ' + str(CondMRIDs), flush=True)
        #print('Found PNV dictionary: ' + str(PNVdict), flush=True)
        #print('PNV dictionary size: ' + str(len(PNVdict)), flush=True)

        # start with Sinj as zero vector and we will come back to this later
        Sinj = np.zeros((N), dtype=complex)
        Sinj[src_idxs] = complex(0.0,1.0)
        #print('\nInitial Sinj:')
        #print(Sinj)

        # subscribe to simulation output so we can start checking tap positions
        # and then setting Sinj
        simCheckRap = SimCheckWrapper(Sinj, PNVmag, RegMRIDs, CondMRIDs, PNVmRIDs, PNVdict)
        conn_id = gapps.subscribe(simulation_output_topic(self.simulation_id), simCheckRap)
        
        while simCheckRap.keepLooping():
            #print('Sleeping....', flush=True)
            time.sleep(0.1)

        gapps.unsubscribe(conn_id)
        #gapps.disconnect()
        #print('\nFinal Sinj:')
        #print(Sinj)
        #for key,value in Node2idx.items():
        #    print(key + ': ' + str(Sinj[value]))

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
        Nfpi = 25
        Isrc_vec = np.zeros((N), dtype=complex)
        Vfpi = np.zeros((N,Nfpi), dtype=complex)

        # start with the CandidateVnom for Vfpi
        Vfpi[:,0] = CandidateVnomVec
        #print('\nVfpi:')
        #print(Vfpi)

        k = 1
        maxdiff = 1.0
        self.PFsolution={}
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
            #print("\nmaxlist:")
            #for i in range(41):
            #print(str(i) + ": " + str(maxlist[i]))

            maxdiff = max(abs(abs(Vfpi[:,k]) - abs(Vfpi[:,k-1])))
            #print("\nk: " + str(k) + ", maxdiff: " + str(maxdiff))
            k += 1

        if k == Nfpi:
            print("\nDid not converge with k: " + str(k))
            self.PFcomplete="Did not converge"
            return

        # set the final Vpfi index
        k -= 1
        
        #print("\nconverged k: " + str(k),flush=True)
        print("\nVfpi:",flush=True)
        for key, value in Node2idx.items():
            rho, phi = cart2pol(Vfpi[value,k])
            print(key + ': rho: ' + str(rho) + ', phi: ' + str(math.degrees(phi)),flush=True)
            self.PFsolution[key]={}
            self.PFsolution[key]=(str(rho),str(math.degrees(phi)))
            #print('index: ' + str(value) + ', sim mag: ' + str(PNVmag[value]),flush=True)

        #print("\nVfpi rho to sim magnitude CSV:")
        for key, value in Node2idx.items():
            mag = PNVmag[value]
            if mag != 0.0:
                rho, phi = cart2pol(Vfpi[value,k])
                #print(str(value) + ',' + key + ',' + str(rho) + ',' + str(mag),flush=True)
                #print(str(value) + ',' + key + ',' + str(rho),flush=True)
        print('\n \n')

        self.PFcomplete=True
        gapps.disconnect()
        return
    
    def on_message(self,headers,message):
        
        reply_to = headers['reply-to']
        #print(reply_to)
        if message['requestType'] == 'GET_SNAPSHOT_POWERFLOW':
            #print('Waiting for PF')
            while not self.PFcomplete:
                time.sleep(0.1)
            message={
                'feeder_id':self.feeder_mrid,
                'simulation_id':self.simulation_id,
                'timestamp':self.simtrack.timestamp,
                'powerflow':self.PFsolution
            }
            #print(message)
            print('Sending last power flow solution for timestamp'+ str(self.simtrack.timestamp),flush=True)
            self.simtrack.gapps.send(reply_to,message)
        else:
            message="No valid requestType specified"


def _main():
      
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
    
    gapps=GridAPPSD()
    assert gapps.connected

    runpf=PowerFlow(gapps,feeder_mrid, simulation_id)
 

if __name__ == "__main__":
    _main()