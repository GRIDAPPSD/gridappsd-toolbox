
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
from operator import contains
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

Sinjglob={}
Yglobal={}

def pol2cart(rho, phi):
    return complex(rho*math.cos(phi), rho*math.sin(phi))

def cart2pol(cart):
    rho = np.sqrt(np.real(cart)**2 + np.imag(cart)**2)
    phi = np.arctan2(np.imag(cart), np.real(cart))
    return (rho, phi)

## Sections copied from Static Ybus to calculate Line impedances

def CN_dist_R(dim, i, j, wire_spacing_info, wire_cn_ts, XCoord, YCoord, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket):
    dist = (CN_diameter_jacket[wire_cn_ts] - CN_strand_radius[wire_cn_ts]*2.0)/2.0
    return dist


def CN_dist_D(dim, i, j, wire_spacing_info, wire_cn_ts, XCoord, YCoord, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket):
    ii,jj = CN_dist_ij[dim][i][j]
    dist = math.sqrt(math.pow(XCoord[wire_spacing_info][ii]-XCoord[wire_spacing_info][jj],2) + math.pow(YCoord[wire_spacing_info][ii]-YCoord[wire_spacing_info][jj],2))
    return dist


def CN_dist_DR(dim, i, j, wire_spacing_info, wire_cn_ts, XCoord, YCoord, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket):
    ii,jj = CN_dist_ij[dim][i][j]
    d = math.sqrt(math.pow(XCoord[wire_spacing_info][ii]-XCoord[wire_spacing_info][jj],2) + math.pow(YCoord[wire_spacing_info][ii]-YCoord[wire_spacing_info][jj],2))
    k = CN_strand_count[wire_cn_ts]
    R = (CN_diameter_jacket[wire_cn_ts] - CN_strand_radius[wire_cn_ts]*2.0)/2.0
    dist = math.pow(math.pow(d,k) - math.pow(R,k), 1.0/k)

    return dist

# global constants for determining Zprim values
u0 = math.pi * 4.0e-7
w = math.pi*2.0 * 60.0
p = 100.0
f = 60.0
Rg = (u0 * w)/8.0
X0 = (u0 * w)/(math.pi*2.0)
Xg = X0 * math.log(658.5 * math.sqrt(p/f))

CN_dist_func = {}
CN_dist_ij = {}

# 2x2 distance function mappings
CN_dist_func[1] = {}
CN_dist_func[1][2] = {}
CN_dist_func[1][2][1] = CN_dist_R

# 4x4 distance function mappings
CN_dist_func[2] = {}
CN_dist_ij[2] = {}
CN_dist_func[2][2] = {}
CN_dist_ij[2][2] = {}
CN_dist_func[2][2][1] = CN_dist_D
CN_dist_ij[2][2][1] = (2,1)
CN_dist_func[2][3] = {}
CN_dist_ij[2][3] = {}
CN_dist_func[2][3][1] = CN_dist_R
CN_dist_func[2][3][2] = CN_dist_DR
CN_dist_ij[2][3][2] = (2,1)
CN_dist_func[2][4] = {}
CN_dist_ij[2][4] = {}
CN_dist_func[2][4][1] = CN_dist_DR
CN_dist_ij[2][4][1] = (2,1)
CN_dist_func[2][4][2] = CN_dist_R
CN_dist_func[2][4][3] = CN_dist_D
CN_dist_ij[2][4][3] = (2,1)

# 6x6 distance function mappings
CN_dist_func[3] = {}
CN_dist_ij[3] = {}
CN_dist_func[3][2] = {}
CN_dist_ij[3][2] = {}
CN_dist_func[3][2][1] = CN_dist_D
CN_dist_ij[3][2][1] = (2,1)
CN_dist_func[3][3] = {}
CN_dist_ij[3][3] = {}
CN_dist_func[3][3][1] = CN_dist_D
CN_dist_ij[3][3][1] = (3,1)
CN_dist_func[3][3][2] = CN_dist_D
CN_dist_ij[3][3][2] = (3,2)
CN_dist_func[3][4] = {}
CN_dist_ij[3][4] = {}
CN_dist_func[3][4][1] = CN_dist_R
CN_dist_func[3][4][2] = CN_dist_DR
CN_dist_ij[3][4][2] = (2,1)
CN_dist_func[3][4][3] = CN_dist_DR
CN_dist_ij[3][4][3] = (3,1)
CN_dist_func[3][5] = {}
CN_dist_ij[3][5] = {}
CN_dist_func[3][5][1] = CN_dist_DR
CN_dist_ij[3][5][1] = (2,1)
CN_dist_func[3][5][2] = CN_dist_R
CN_dist_func[3][5][3] = CN_dist_DR
CN_dist_ij[3][5][3] = (3,2)
CN_dist_func[3][5][4] = CN_dist_D
CN_dist_ij[3][5][4] = (2,1)
CN_dist_func[3][6] = {}
CN_dist_ij[3][6] = {}
CN_dist_func[3][6][1] = CN_dist_DR
CN_dist_ij[3][6][1] = (3,1)
CN_dist_func[3][6][2] = CN_dist_DR
CN_dist_ij[3][6][2] = (3,2)
CN_dist_func[3][6][3] = CN_dist_R
CN_dist_func[3][6][4] = CN_dist_D
CN_dist_ij[3][6][4] = (3,1)
CN_dist_func[3][6][5] = CN_dist_D
CN_dist_ij[3][6][5] = (3,2)

def diagZprim(wireinfo, wire_cn_ts, neutralFlag, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen):
    if wireinfo=='ConcentricNeutralCableInfo' and neutralFlag:
        R = (CN_diameter_jacket[wire_cn_ts] - CN_strand_radius[wire_cn_ts]*2.0)/2.0
        k = CN_strand_count[wire_cn_ts]
        dist = math.pow(CN_strand_gmr[wire_cn_ts]*k*math.pow(R,k-1),1.0/k)
        Zprim = complex(CN_strand_rdc[wire_cn_ts]/k + Rg, X0*math.log(1.0/dist) + Xg)

        # this situation won't normally occur so we are just using neutralFlag to recognize the
        # row 2 diagonal for the shield calculation vs. row1 and row3 that are handled below
    elif wireinfo=='TapeShieldCableInfo' and neutralFlag:
        T = TS_tape_thickness[wire_cn_ts]
        ds = TS_diameter_screen[wire_cn_ts] + 2.0*T
        Rshield = 0.3183 * 2.3718e-8/(ds*T*math.sqrt(50.0/(100.0-20.0)))
        Dss = 0.5*(ds - T)
        Zprim = complex(Rshield + Rg, X0*math.log(1.0/Dss) + Xg)

    else:
        Zprim = complex(R25[wire_cn_ts] + Rg, X0*math.log(1.0/GMR[wire_cn_ts]) + Xg)

    return Zprim

def offDiagZprim(i, j, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen):
    if wireinfo == 'OverheadWireInfo':
        dist = math.sqrt(math.pow(XCoord[wire_spacing_info][i]-XCoord[wire_spacing_info][j],2) + math.pow(YCoord[wire_spacing_info][i]-YCoord[wire_spacing_info][j],2))

    elif wireinfo == 'ConcentricNeutralCableInfo':
        dim = len(XCoord[wire_spacing_info]) # 1=2x2, 2=4x4, 3=6x6
        dist = CN_dist_func[dim][i][j](dim, i, j, wire_spacing_info, wire_cn_ts, XCoord, YCoord, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket)

    elif wireinfo == 'TapeShieldCableInfo':
            # this should only be hit for i==2
        T = TS_tape_thickness[wire_cn_ts]
        ds = TS_diameter_screen[wire_cn_ts] + 2.0*T
        dist = 0.5*(ds - T)

    Zprim = complex(Rg, X0*math.log(1.0/dist) + Xg)

    return Zprim

def fill_Ybus_WireInfo_and_WireSpacingInfo_lines(sparql_mgr):
    # WireSpacingInfo query
    Line_imp={}
    bindings = sparql_mgr.WireInfo_spacing()
    #print('LINE_MODEL_FILL_YBUS WireInfo spacing query results:', flush=True)
    #print(bindings, flush=True)

    XCoord = {}
    YCoord = {}
    for obj in bindings:
        wire_spacing_info = obj['wire_spacing_info']['value']
        cableFlag = obj['cable']['value'].upper() == 'TRUE' # don't depend on lowercase
        #usage = obj['usage']['value']
        #bundle_count = int(obj['bundle_count']['value'])
        #bundle_sep = int(obj['bundle_sep']['value'])
        seq = int(obj['seq']['value'])
        if seq == 1:
            XCoord[wire_spacing_info] = {}
            YCoord[wire_spacing_info] = {}

        XCoord[wire_spacing_info][seq] = float(obj['xCoord']['value'])
        YCoord[wire_spacing_info][seq] = float(obj['yCoord']['value'])
        #print('wire_spacing_info: ' + wire_spacing_info + ', cable: ' + str(cableFlag) + ', seq: ' + str(seq) + ', XCoord: ' + str(XCoord[wire_spacing_info][seq]) + ', YCoord: ' + str(YCoord[wire_spacing_info][seq]))

    # OverheadWireInfo specific query
    bindings = sparql_mgr.WireInfo_overhead()
    #print('LINE_MODEL_FILL_YBUS WireInfo overhead query results:', flush=True)
    #print(bindings, flush=True)

    GMR = {}
    R25 = {}
    for obj in bindings:
        wire_cn_ts = obj['wire_cn_ts']['value']
        #radius = float(obj['radius']['value'])
        #coreRadius = float(obj['coreRadius']['value'])
        GMR[wire_cn_ts] = float(obj['gmr']['value'])
        #rdc = float(obj['rdc']['value'])
        R25[wire_cn_ts] = float(obj['r25']['value'])
        #r50 = float(obj['r50']['value'])
        #r75 = float(obj['r75']['value'])
        #amps = int(obj['amps']['value'])
        #print('overhead wire_cn_ts: ' + wire_cn_ts + ', gmr: ' + str(GMR[wire_cn_ts]) + ', r25: ' + str(R25[wire_cn_ts]))

    # ConcentricNeutralCableInfo specific query
    bindings = sparql_mgr.WireInfo_concentricNeutral()
    #print('LINE_MODEL_FILL_YBUS WireInfo concentricNeutral query results:', flush=True)
    #print(bindings, flush=True)

    CN_diameter_jacket = {}
    CN_strand_count = {}
    CN_strand_radius = {}
    CN_strand_gmr = {}
    CN_strand_rdc = {}
    for obj in bindings:
        wire_cn_ts = obj['wire_cn_ts']['value']
        #radius = float(obj['radius']['value'])
        #coreRadius = float(obj['coreRadius']['value'])
        GMR[wire_cn_ts] = float(obj['gmr']['value'])
        #rdc = float(obj['rdc']['value'])
        R25[wire_cn_ts] = float(obj['r25']['value'])
        #r50 = float(obj['r50']['value'])
        #r75 = float(obj['r75']['value'])
        #amps = int(obj['amps']['value'])
        #insulationFlag = obj['amps']['value'].upper() == 'TRUE'
        #insulation_thickness = float(obj['insulation_thickness']['value'])
        #diameter_core = float(obj['diameter_core']['value'])
        #diameter_insulation = float(obj['diameter_insulation']['value'])
        #diameter_screen = float(obj['diameter_screen']['value'])
        CN_diameter_jacket[wire_cn_ts] = float(obj['diameter_jacket']['value'])
        #diameter_neutral = float(obj['diameter_neutral']['value'])
        #sheathneutral = obj['sheathneutral']['value'].upper()=='TRUE'
        CN_strand_count[wire_cn_ts] = int(obj['strand_count']['value'])
        CN_strand_radius[wire_cn_ts] = float(obj['strand_radius']['value'])
        CN_strand_gmr[wire_cn_ts] = float(obj['strand_gmr']['value'])
        CN_strand_rdc[wire_cn_ts] = float(obj['strand_rdc']['value'])
        #print('concentric wire_cn_ts: ' + wire_cn_ts + ', gmr: ' + str(GMR[wire_cn_ts]) + ', r25: ' + str(R25[wire_cn_ts]) + ', diameter_jacket: ' + str(CN_diameter_jacket[wire_cn_ts]) + ', strand_count: ' + str(CN_strand_count[wire_cn_ts]) + ', strand_radius: ' + str(CN_strand_radius[wire_cn_ts]) + ', strand_gmr: ' + str(CN_strand_gmr[wire_cn_ts]) + ', strand_rdc: ' + str(CN_strand_rdc[wire_cn_ts]))

    # TapeShieldCableInfo specific query
    bindings = sparql_mgr.WireInfo_tapeShield()
    #print('LINE_MODEL_FILL_YBUS WireInfo tapeShield query results:', flush=True)
    #print(bindings, flush=True)

    TS_diameter_screen = {}
    TS_tape_thickness = {}
    for obj in bindings:
        wire_cn_ts = obj['wire_cn_ts']['value']
        #radius = float(obj['radius']['value'])
        #coreRadius = float(obj['coreRadius']['value'])
        GMR[wire_cn_ts] = float(obj['gmr']['value'])
        #rdc = float(obj['rdc']['value'])
        R25[wire_cn_ts] = float(obj['r25']['value'])
        #r50 = float(obj['r50']['value'])
        #r75 = float(obj['r75']['value'])
        #amps = int(obj['amps']['value'])
        #insulationFlag = obj['amps']['value'].upper() == 'TRUE'
        #insulation_thickness = float(obj['insulation_thickness']['value'])
        #diameter_core = float(obj['diameter_core']['value'])
        #diameter_insulation = float(obj['diameter_insulation']['value'])
        TS_diameter_screen[wire_cn_ts] = float(obj['diameter_screen']['value'])
        #diameter_jacket = float(obj['diameter_jacket']['value'])
        #sheathneutral = obj['sheathneutral']['value'].upper()=='TRUE'
        #tapelap = int(obj['tapelap']['value'])
        TS_tape_thickness[wire_cn_ts] = float(obj['tapethickness']['value'])
        #print('tape wire_cn_ts: ' + wire_cn_ts + ', gmr: ' + str(GMR[wire_cn_ts]) + ', r25: ' + str(R25[wire_cn_ts]) + ', diameter_screen: ' + str(TS_diameter_screen[wire_cn_ts]) + ', tape_thickness: ' + str(TS_tape_thickness[wire_cn_ts]))
    #print('*')
    #print(TS_diameter_screen)
    #print('*')
    # line_names query for all types
    bindings = sparql_mgr.WireInfo_line_names()
    #print('LINE_MODEL_FILL_YBUS WireInfo line_names query results:', flush=True)
    #print(bindings, flush=True)

    if len(bindings) == 0:
        return

    # map line_name query phase values to nodelist indexes
    ybusPhaseIdx = {'A': '.1', 'B': '.2', 'C': '.3', 'N': '.4', 's1': '.1', 's2': '.2'}

    # map between 0-base numpy array indices and 1-based formulas so everything lines up
    i1 = j1 = 0
    i2 = j2 = 1
    i3 = j3 = 2
    i4 = j4 = 3
    i5 = j5 = 4
    i6 = j6 = 5

    tape_line = None
    tape_skip = False
    phaseIdx = 0
    CN_done = False
    for obj in bindings:
        line_name = obj['line_name']['value']
        #basev = float(obj['basev']['value'])
        bus1 = obj['bus1']['value'].upper()
        bus2 = obj['bus2']['value'].upper()
        length = float(obj['length']['value'])
        wire_spacing_info = obj['wire_spacing_info']['value']
        phase = obj['phase']['value']
        wire_cn_ts = obj['wire_cn_ts']['value']
        wireinfo = obj['wireinfo']['value']
        #print('line_name: ' + line_name + ', bus1: ' + bus1 + ', bus2: ' + bus2 + ', length: ' + str(length) + ', wire_spacing_info: ' + wire_spacing_info + ', phase: ' + phase + ', wire_cn_ts: ' + wire_cn_ts + ', wireinfo: ' + wireinfo)

        # TapeShieldCableInfo is special so it needs some special processing
        # first, the wireinfo isn't always TapeShieldCableInfo so need to match on line_name instead
        # second, only a single phase is implemented so need a way to skip processing multiple phases
        
        if wireinfo=='TapeShieldCableInfo' or line_name==tape_line:
            tape_line = line_name
            if tape_skip:
                continue
        else:
            tape_line = None
            tape_skip = False

        if phaseIdx == 0:
            pair_i0b1 = bus1 + ybusPhaseIdx[phase]
            pair_i0b2 = bus2 + ybusPhaseIdx[phase]

            dim = len(XCoord[wire_spacing_info])
            if wireinfo == 'OverheadWireInfo':
                if dim == 2:
                    Zprim = np.empty((2,2), dtype=complex)
                elif dim == 3:
                    Zprim = np.empty((3,3), dtype=complex)
                elif dim == 4:
                    Zprim = np.empty((4,4), dtype=complex)

            elif wireinfo == 'ConcentricNeutralCableInfo':
                if dim == 1:
                    Zprim = np.empty((2,2), dtype=complex)
                elif dim == 2:
                    Zprim = np.empty((4,4), dtype=complex)
                elif dim == 3:
                    Zprim = np.empty((6,6), dtype=complex)

            elif wireinfo == 'TapeShieldCableInfo':
                if dim == 2:
                    Zprim = np.empty((3,3), dtype=complex)
                else:
                    tape_skip = True
                    continue

            # row 1
            Zprim[i1,j1] = diagZprim(wireinfo, wire_cn_ts, False, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)

            if wireinfo=='ConcentricNeutralCableInfo' and dim==1:
                CN_done = True

                # row 2
                Zprim[i2,j1] = Zprim[i1,j2] = offDiagZprim(2, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i2,j2] = diagZprim(wireinfo, wire_cn_ts, True, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)

            elif wireinfo == 'TapeShieldCableInfo':
                # row 2
                Zprim[i2,j1] = Zprim[i1,j2] = offDiagZprim(2, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                # neutralFlag is passed as True as a flag indicating to use the 2nd row shield calculation
                Zprim[i2,j2] = diagZprim(wireinfo, wire_cn_ts, True, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)

        elif phaseIdx == 1:
            pair_i1b1 = bus1 + ybusPhaseIdx[phase]
            pair_i1b2 = bus2 + ybusPhaseIdx[phase]

            # row 2
            if line_name != tape_line:
                Zprim[i2,j1] = Zprim[i1,j2] = offDiagZprim(2, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i2,j2] = diagZprim(wireinfo, wire_cn_ts, False, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)

            if wireinfo=='ConcentricNeutralCableInfo' and dim==2:
                CN_done = True

                # row 3
                Zprim[i3,j1] = Zprim[i1,j3] = offDiagZprim(3, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i3,j2] = Zprim[i2,j3] = offDiagZprim(3, 2, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i3,j3] = diagZprim(wireinfo, wire_cn_ts, True, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)

                # row 4
                Zprim[i4,j1] = Zprim[i1,j4] = offDiagZprim(4, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i4,j2] = Zprim[i2,j4] = offDiagZprim(4, 2, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i4,j3] = Zprim[i3,j4] = offDiagZprim(4, 3, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i4,j4] = diagZprim(wireinfo, wire_cn_ts, True, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)

            elif line_name == tape_line:
                # row 3
                # coordinates for neutral are stored in index 2 for TapeShieldCableInfo
                Zprim[i3,j1] = Zprim[i1,j3] = Zprim[i3,j2] = Zprim[i2,j3] = offDiagZprim(2, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i3,j3] = diagZprim(wireinfo, wire_cn_ts, True, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)

        elif phaseIdx == 2:
            pair_i2b1 = bus1 + ybusPhaseIdx[phase]
            pair_i2b2 = bus2 + ybusPhaseIdx[phase]

            # row 3
            Zprim[i3,j1] = Zprim[i1,j3] = offDiagZprim(3, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
            Zprim[i3,j2] = Zprim[i2,j3] = offDiagZprim(3, 2, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
            Zprim[i3,j3] = diagZprim(wireinfo, wire_cn_ts, False, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)

            if wireinfo == 'ConcentricNeutralCableInfo':
                CN_done = True

                # row 4
                Zprim[i4,j1] = Zprim[i1,j4] = offDiagZprim(4, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i4,j2] = Zprim[i2,j4] = offDiagZprim(4, 2, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i4,j3] = Zprim[i3,j4] = offDiagZprim(4, 3, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i4,j4] = diagZprim(wireinfo, wire_cn_ts, True, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                # row 5
                Zprim[i5,j1] = Zprim[i1,j5] = offDiagZprim(5, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i5,j2] = Zprim[i2,j5] = offDiagZprim(5, 2, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i5,j3] = Zprim[i3,j5] = offDiagZprim(5, 3, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i5,j4] = Zprim[i4,j5] = offDiagZprim(5, 4, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i5,j5] = diagZprim(wireinfo, wire_cn_ts, True, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)

                # row 6
                Zprim[i6,j1] = Zprim[i1,j6] = offDiagZprim(6, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i6,j2] = Zprim[i2,j6] = offDiagZprim(6, 2, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i6,j3] = Zprim[i3,j6] = offDiagZprim(6, 3, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i6,j4] = Zprim[i4,j6] = offDiagZprim(6, 4, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i6,j5] = Zprim[i5,j6] = offDiagZprim(6, 5, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
                Zprim[i6,j6] = diagZprim(wireinfo, wire_cn_ts, True, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)

        elif phaseIdx == 3:
            # this can only be phase 'N' so no need to store 'pair' values
            # row 4
            Zprim[i4,j1] = Zprim[i1,j4] = offDiagZprim(4, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
            Zprim[i4,j2] = Zprim[i2,j4] = offDiagZprim(4, 2, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
            Zprim[i4,j3] = Zprim[i3,j4] = offDiagZprim(4, 3, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
            Zprim[i4,j4] = diagZprim(wireinfo, wire_cn_ts, phase, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)

        # for OverheadWireInfo, take advantage that there is always a phase N
        # and it's always the last item processed for a line_name so a good way
        # to know when to trigger the Ybus comparison code
        # for ConcentricNeutralCableInfo, a flag is the easiest
        if (wireinfo=='OverheadWireInfo' and phase == 'N') or (wireinfo=='ConcentricNeutralCableInfo' and CN_done):
            if wireinfo == 'ConcentricNeutralCableInfo':
                # the Z-hat slicing below is based on having an 'N' phase so need to
                # account for that when it doesn't exist
                phaseIdx += 1
                CN_done = False

            # create the Z-hat matrices to then compute Zabc for Ybus comparisons
            Zij = Zprim[:phaseIdx,:phaseIdx]
            Zin = Zprim[:phaseIdx,phaseIdx:]
            Znj = Zprim[phaseIdx:,:phaseIdx]
            #Znn = Zprim[phaseIdx:,phaseIdx:]
            invZnn = np.linalg.inv(Zprim[phaseIdx:,phaseIdx:])

            # finally, compute Zabc from Z-hat matrices
            Zabc = np.subtract(Zij, np.matmul(np.matmul(Zin, invZnn), Znj))

            # multiply by scalar length
            lenZabc = Zabc * length
            # invert the matrix
            invZabc = np.linalg.inv(lenZabc)
            # test if the inverse * original = identity
            #identityTest = np.dot(lenZabc, invZabc)
            #print('identity test for ' + line_name + ': ' + str(identityTest))
            # negate the matrix and assign it to Ycomp
            Ycomp = invZabc * -1

            Line_imp[line_name]={}
            Line_imp[line_name]['bus1']=bus1
            Line_imp[line_name]['bus2']=bus2
            Line_imp[line_name]['Yline']=Ycomp 

            phaseIdx = 0
        else:
            phaseIdx += 1
    return Line_imp

def fill_Ybus_PerLengthPhaseImpedance_lines(sparql_mgr): #, Line_impedances):
    Line_imp={}
    bindings = sparql_mgr.PerLengthPhaseImpedance_line_configs()
    #print('LINE_MODEL_FILL_YBUS PerLengthPhaseImpedance line_configs query results:', flush=True)
    #print(bindings, flush=True)

    if len(bindings) == 0:
        return

    Zabc = {}
    for obj in bindings:
        line_config = obj['line_config']['value']
        count = int(obj['count']['value'])
        row = int(obj['row']['value'])
        col = int(obj['col']['value'])
        r_ohm_per_m = float(obj['r_ohm_per_m']['value'])
        x_ohm_per_m = float(obj['x_ohm_per_m']['value'])
        #b_S_per_m = float(obj['b_S_per_m']['value'])
        #print('line_config: ' + line_config + ', count: ' + str(count) + ', row: ' + str(row) + ', col: ' + str(col) + ', r_ohm_per_m: ' + str(r_ohm_per_m) + ', x_ohm_per_m: ' + str(x_ohm_per_m) + ', b_S_per_m: ' + str(b_S_per_m))

        if line_config not in Zabc:
            if count == 1:
                Zabc[line_config] = np.zeros((1,1), dtype=complex)
            elif count == 2:
                Zabc[line_config] = np.zeros((2,2), dtype=complex)
            elif count == 3:
                Zabc[line_config] = np.zeros((3,3), dtype=complex)

        Zabc[line_config][row-1,col-1] = complex(r_ohm_per_m, x_ohm_per_m)
        if row != col:
            Zabc[line_config][col-1,row-1] = complex(r_ohm_per_m, x_ohm_per_m)

    #for line_config in Zabc:
    #    print('Zabc[' + line_config + ']: ' + str(Zabc[line_config]))
    #print('')

    bindings = sparql_mgr.PerLengthPhaseImpedance_line_names()
    #print('LINE_MODEL_FILL_YBUS PerLengthPhaseImpedance line_names query results:', flush=True)
    #print(bindings, flush=True)

    if len(bindings) == 0:
        return

    # map line_name query phase values to nodelist indexes
    ybusPhaseIdx = {'A': '.1', 'B': '.2', 'C': '.3', 's1': '.1', 's2': '.2'}

    last_name = ''
    for obj in bindings:
        line_name = obj['line_name']['value']
        bus1 = obj['bus1']['value'].upper()
        bus2 = obj['bus2']['value'].upper()
        length = float(obj['length']['value'])
        line_config = obj['line_config']['value']
        phase = obj['phase']['value']
        #print('line_name: ' + line_name + ', line_config: ' + line_config + ', length: ' + str(length) + ', bus1: ' + bus1 + ', bus2: ' + bus2 + ', phase: ' + phase)

        if line_name!=last_name and line_config in Zabc:
            last_name = line_name
            line_idx = 0

            # multiply by scalar length
            lenZabc = Zabc[line_config] * length
            # invert the matrix
            invZabc = np.linalg.inv(lenZabc)
            # test if the inverse * original = identity
            #identityTest = np.dot(lenZabc, invZabc)
            #print('identity test for ' + line_name + ': ' + str(identityTest))
            # negate the matrix and assign it to Ycomp
            Ycomp = invZabc * -1
            
            Line_imp[line_name]={}
            Line_imp[line_name]['bus1']=bus1
            Line_imp[line_name]['bus2']=bus2
            Line_imp[line_name]['Yline']=Ycomp

    return Line_imp 
        # we now have the negated inverted matrix for comparison

def fill_Ybus_PerLengthSequenceImpedance_lines(sparql_mgr):#, Line_impedances):
    Line_imp={}
    bindings = sparql_mgr.PerLengthSequenceImpedance_line_configs()
    #print('LINE_MODEL_FILL_YBUS PerLengthSequenceImpedance line_configs query results:', flush=True)
    #print(bindings, flush=True)

    if len(bindings) == 0:
        return Line_imp

    Zabc = {}
    for obj in bindings:
        line_config = obj['line_config']['value']
        r1 = float(obj['r1_ohm_per_m']['value'])
        x1 = float(obj['x1_ohm_per_m']['value'])
        #b1 = float(obj['b1_S_per_m']['value'])
        r0 = float(obj['r0_ohm_per_m']['value'])
        x0 = float(obj['x0_ohm_per_m']['value'])
        #b0 = float(obj['b0_S_per_m']['value'])
        #print('line_config: ' + line_config + ', r1: ' + str(r1) + ', x1: ' + str(x1) + ', b1: ' + str(b1) + ', r0: ' + str(r0) + ', x0: ' + str(x0) + ', b0: ' + str(b0))

        Zs = complex((r0 + 2.0*r1)/3.0, (x0 + 2.0*x1)/3.0)
        Zm = complex((r0 - r1)/3.0, (x0 - x1)/3.0)

        Zabc[line_config] = np.array([(Zs, Zm, Zm), (Zm, Zs, Zm), (Zm, Zm, Zs)], dtype=complex)

    #for line_config in Zabc:
    #    print('Zabc[' + line_config + ']: ' + str(Zabc[line_config]))
    #print('')

    bindings = sparql_mgr.PerLengthSequenceImpedance_line_names()
    #print('LINE_MODEL_FILL_YBUS PerLengthSequenceImpedance line_names query results:', flush=True)
    #print(bindings, flush=True)

    if len(bindings) == 0:
        return

    for obj in bindings:
        line_name = obj['line_name']['value']
        bus1 = obj['bus1']['value'].upper()
        bus2 = obj['bus2']['value'].upper()
        length = float(obj['length']['value'])
        line_config = obj['line_config']['value']
        #print('line_name: ' + line_name + ', line_config: ' + line_config + ', length: ' + str(length) + ', bus1: ' + bus1 + ', bus2: ' + bus2)

        # multiply by scalar length
        lenZabc = Zabc[line_config] * length
        # invert the matrix
        invZabc = np.linalg.inv(lenZabc)
        # test if the inverse * original = identity
        #identityTest = np.dot(lenZabc, invZabc)
        #print('identity test for ' + line_name + ': ' + str(identityTest))
        # negate the matrix and assign it to Ycomp
        Ycomp = invZabc * -1

        Line_imp[line_name]={}
        Line_imp[line_name]['bus1']=bus1
        Line_imp[line_name]['bus2']=bus2
        Line_imp[line_name]['Yline']=Ycomp 

    return Line_imp
                            
def fill_Ybus_ACLineSegment_lines(sparql_mgr):#, Line_impedances):
    Line_imp={}
    bindings = sparql_mgr.ACLineSegment_line_names()
    #print('LINE_MODEL_FILL_YBUS ACLineSegment line_names query results:', flush=True)
    #print(bindings, flush=True)

    if len(bindings) == 0:
        return Line_imp

    for obj in bindings:
        line_name = obj['line_name']['value']
        #basev = float(obj['basev']['value'])
        bus1 = obj['bus1']['value'].upper()
        bus2 = obj['bus2']['value'].upper()
        length = float(obj['length']['value'])
        r1 = float(obj['r1_Ohm']['value'])
        x1 = float(obj['x1_Ohm']['value'])
        #b1 = float(obj['b1_S']['value'])
        r0 = float(obj['r0_Ohm']['value'])
        x0 = float(obj['x0_Ohm']['value'])
        #b0 = float(obj['b0_S']['value'])
        #print('line_name: ' + line_name + ', length: ' + str(length) + ', bus1: ' + bus1 + ', bus2: ' + bus2 + ', r1: ' + str(r1) + ', x1: ' + str(x1) + ', r0: ' + str(r0) + ', x0: ' + str(x0))

        Zs = complex((r0 + 2.0*r1)/3.0, (x0 + 2.0*x1)/3.0)
        Zm = complex((r0 - r1)/3.0, (x0 - x1)/3.0)

        Zabc = np.array([(Zs, Zm, Zm), (Zm, Zs, Zm), (Zm, Zm, Zs)], dtype=complex)
        #print('Zabc: ' + str(Zabc) + '\n')

        # multiply by scalar length
        lenZabc = Zabc * length
        #lenZabc = Zabc * length * 3.3 # Kludge to get arount units issue (ft vs. m)
        # invert the matrix
        invZabc = np.linalg.inv(lenZabc)
        # test if the inverse * original = identity
        #identityTest = np.dot(lenZabc, invZabc)
        #print('identity test for ' + line_name + ': ' + str(identityTest))
        # negate the matrix and assign it to Ycomp
        Ycomp = invZabc * -1

        Line_imp[line_name]={}
        Line_imp[line_name]['bus1']=bus1
        Line_imp[line_name]['bus2']=bus2
        Line_imp[line_name]['Yline']=Ycomp 

    return Line_imp                

## Resume Power Flow Algorithm

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
                    'powerflow':self.PFobject.PFsolution,
                    'losses':self.PFobject.Line_losses
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
        self.Line_impedances={}
        self.Sinj={}
        
        
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
        time.sleep(10)
        # OVerwrite the Ybus if User supplies one
        
        global Yglobal
        
        if(len(Yglobal)>0):
            self.Ylast={}
            self.Ylast=self.tupletocomplex(Yglobal)
        else:
            print('Requesting Ybus from Dynamic-Ybus Service\n')
            
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
                    global Yglobal
                    if(len(Yglobal)==0):
                        print('Re-solving power flow for updated system')
                                        
                        
                        self.runpowerflow()

                        self.simtrack.newchange=True

    

    
   
    def runpowerflow(self):
        
        if not self.Ybusinit:
            return
        
        SPARQLManager = getattr(importlib.import_module('shared.sparql'), 'SPARQLManager')

        gapps = GridAPPSD()
        
        sparql_mgr = SPARQLManager(gapps, self.feeder_mrid, self.simulation_id)
        #sparql_mgr = SPARQLManager(gapps, self.feeder_mrid)#, self.simulation_id)

        self.Line_impedances={}
        Line_imp1=fill_Ybus_PerLengthPhaseImpedance_lines(sparql_mgr) #, self.Line_impedances)
        if len(Line_imp1)>0:
            self.Line_impedances.update(Line_imp1)
        #print(self.Line_impedances)
        #print('*')

        Line_imp2=fill_Ybus_PerLengthSequenceImpedance_lines(sparql_mgr) #, self.Line_impedances)
        if len(Line_imp2)>0:
            self.Line_impedances.update(Line_imp2)
        #print(self.Line_impedances)
        #print('*')
        
        Line_imp3=fill_Ybus_ACLineSegment_lines(sparql_mgr)#, self.Line_impedances)
        if len(Line_imp3)>0:
            self.Line_impedances.update(Line_imp3)
        #print(self.Line_impedances)
        #print('*')
        
        Line_imp4=fill_Ybus_WireInfo_and_WireSpacingInfo_lines(sparql_mgr)#, self.Line_impedances)
        if len(Line_imp4)>0:
            self.Line_impedances.update(Line_imp4)
        #print(self.Line_impedances)
        
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
        #print(src_idxs)
        #print('\nInitial Sinj:')
        #print(N)
        if(len(Sinjglob)>0):
            #Sinj = np.zeros((len(Sinjglob)), dtype=complex)
            
            for sg in Sinjglob:
                
                Sinj[int(sg)-1]=complex(Sinjglob[sg][0],Sinjglob[sg][1])
                
        Sinj[src_idxs] = complex(0.0,1.0)

        # subscribe to simulation output so we can start checking tap positions
        # and then setting Sinj
        simCheckRap = SimCheckWrapper(Sinj, PNVmag, RegMRIDs, CondMRIDs, PNVmRIDs, PNVdict)
        conn_id = gapps.subscribe(simulation_output_topic(self.simulation_id), simCheckRap)
        #print('Sinj')
        #print(Sinj)
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
        Order={}
        for key, value in Node2idx.items():
            rho, phi = cart2pol(Vfpi[value,k])
            Order[str(key)]={}
            Order[str(key)]['ind']=value
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
        self.Line_losses={}
        for obj in self.Line_impedances:
            name=obj
            b1=self.Line_impedances[obj]['bus1']
            b2=self.Line_impedances[obj]['bus2']
            Yline=self.Line_impedances[obj]['Yline']
            #print(type(Yline))
            b1found=0
            b2found=0
            Linekeysb1=[]
            Linekeysb2=[]
            aphase1=0
            bphase1=0
            cphase1=0
            aphase2=0
            bphase2=0
            cphase2=0

            for key,value in Node2idx.items():
                if b1 in key:
                    Linekeysb1.append(key)
                    if('.1' in key):
                        aphase1=1
                    elif ('.2' in key):
                        bphase1=1
                    else:
                        cphase1=1
                    b1found=1
                if b2 in key:
                    Linekeysb2.append(key)
                    if('.1' in key):
                        aphase2=1
                    elif ('.2' in key):
                        bphase2=1
                    else:
                        cphase2=1
                    b2found=1
            Linekeysb1order=[]
            Linekeysb2order=[]

            if (aphase1==1 and aphase2==1):
                for i in range(len(Linekeysb1)):
                    if('.1' in Linekeysb1[i]):
                        Linekeysb1order.append(Linekeysb1[i])
                for i in range(len(Linekeysb2)):
                    if('.1' in Linekeysb2[i]):
                        Linekeysb2order.append(Linekeysb2[i])
            if (bphase1==1 and bphase2==1):
                for i in range(len(Linekeysb1)):
                    if('.2' in Linekeysb1[i]):
                        Linekeysb1order.append(Linekeysb1[i])
                for i in range(len(Linekeysb2)):
                    if('.2' in Linekeysb2[i]):
                        Linekeysb2order.append(Linekeysb2[i])
            if (cphase1==1 and cphase2==1):
                for i in range(len(Linekeysb1)):
                    if('.3' in Linekeysb1[i]):
                        Linekeysb1order.append(Linekeysb1[i])
                for i in range(len(Linekeysb2)):
                    if('.3' in Linekeysb2[i]):
                        Linekeysb2order.append(Linekeysb2[i])
            
            #Linekeysb1order.append(Linekeysb2order)
            #print(Linekeysb1order)
            # Calculate I1, I2 and create conjugates
            Vdiff=np.empty((len(Linekeysb1order),1), dtype=complex)
            V1=np.empty((len(Linekeysb1order),1), dtype=complex)
            V2=np.empty((len(Linekeysb1order),1), dtype=complex)
            for i in range(len(Linekeysb1order)):
                #print(i)
                v1=Order[Linekeysb1order[i]]['ind']
                v2=Order[Linekeysb2order[i]]['ind']
                #print(Vfpi[v1,k])
                #print(Vfpi[v2,k])
                V1[i,0]=Vfpi[v1,k]
                V2[i,0]=Vfpi[v2,k]
                Vdiff[i,0]=np.subtract(Vfpi[v1,k],Vfpi[v2,k])
            #print(V1)
            #print(V2)
            V1=np.transpose(V1)
            V2=np.transpose(V2)
            I1=np.matmul(Yline,Vdiff)
            I2=I1*(-1)
            I1conj=np.conjugate(I1)
            I2conj=np.conjugate(I2)
            #print(V1)
            #print(I1)
            S12=np.matmul(V1,I1conj)
            S21=np.matmul(V2,I2conj)
            #print(S12)
            Loss=np.add(S12,S21)
            Active_loss=np.absolute(np.real(Loss))
            Reactive_loss=np.absolute(np.imag(Loss))
            self.Line_losses[b1+'-'+b2]={}
            self.Line_losses[b1+'-'+b2]=(str(Active_loss),str(Reactive_loss))
        
        print('Line Losses: \n')
        for k in self.Line_losses:    
            print('Line '+str(k)+': Active Loss: '+str(self.Line_losses[k][0])+': Reactive Loss: '+str(self.Line_losses[k][1]))   
        
        self.PFcomplete=True
        gapps.disconnect()
        return
    
    def on_message(self,headers,message):
        
        reply_to = headers['reply-to']
        if len(message['Ybus'])>0:
            global Yglobal
            Yglobal=message['Ybus']
            print('Recieved User generated Ybus\n')
        if len(message['Sinj'])>0:
            global Sinjglob
            Sinjglob=message['Sinj']
            print('Recieved User generated Injections\n')
        #print(reply_to)
        if message['requestType'] == 'GET_SNAPSHOT_POWERFLOW':
            #print('Waiting for PF')
            while not self.PFcomplete:
                time.sleep(0.1)
            message={
                'feeder_id':self.feeder_mrid,
                'simulation_id':self.simulation_id,
                'timestamp':self.simtrack.timestamp,
                'powerflow':self.PFsolution,
                'losses':self.Line_losses
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
    parser.add_argument("simulation_id", help="Simulation ID")
    parser.add_argument("request", help="Simulation Request")

    opts = parser.parse_args()
    sim_request = json.loads(opts.request.replace("\'",""))
    feeder_mrid = sim_request["power_system_config"]["Line_name"]
    simulation_id = opts.simulation_id
    
    gapps=GridAPPSD()
    assert gapps.connected
    #time.sleep(15)
    runpf=PowerFlow(gapps,feeder_mrid, simulation_id)
 

if __name__ == "__main__":
    _main()