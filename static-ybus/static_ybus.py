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
import argparse
import json
import importlib
import math
import numpy as np

from gridappsd import GridAPPSD


def fillYbusUnique(bus1, bus2, Yval, Ybus):
    if Yval == 0j:
        return

    if bus1 not in Ybus:
        Ybus[bus1] = {}
    if bus2 not in Ybus:
        Ybus[bus2] = {}

    if bus2 in Ybus[bus1]:
        print('    *** WARNING: Unexpected existing value found for Ybus[' + bus1 + '][' + bus2 + '] when filling model value\n', flush=True)

    Ybus[bus1][bus2] = Ybus[bus2][bus1] = Yval


def fillYbusAdd(bus1, bus2, Yval, Ybus):
    if Yval == 0j:
        return

    if bus1 not in Ybus:
        Ybus[bus1] = {}
    if bus2 not in Ybus:
        Ybus[bus2] = {}

    if bus2 in Ybus[bus1]:
        Ybus[bus1][bus2] += Yval
        Ybus[bus2][bus1] = Ybus[bus1][bus2]
    else:
        Ybus[bus1][bus2] = Ybus[bus2][bus1] = Yval


# START LINES
def fillYbusUniqueUpper_lines(bus1, bus2, Yval, Ybus):
    if Yval == 0j:
        return

    if bus1 not in Ybus:
        Ybus[bus1] = {}
    if bus2 not in Ybus:
        Ybus[bus2] = {}

    if bus2 in Ybus[bus1]:
        print('    *** WARNING: Unexpected existing value found for Ybus[' + bus1 + '][' + bus2 + '] when filling line model value\n', flush=True)

    # extract the node and phase from bus1 and bus2
    node1,phase1 = bus1.split('.')
    node2,phase2 = bus2.split('.')
    bus3 = node1 + '.' + phase2
    bus4 = node2 + '.' + phase1

    if bus3 not in Ybus:
        Ybus[bus3] = {}
    if bus4 not in Ybus:
        Ybus[bus4] = {}

    Ybus[bus1][bus2] = Ybus[bus2][bus1] = Ybus[bus3][bus4] = Ybus[bus4][bus3] = Yval


def fillYbusNoSwap_lines(bus1, bus2, Yval, Ybus):
    #print('fillYbusNoSwap_lines bus1: ' + bus1 + ', bus2: ' + bus2, flush=True)
    fillYbusUnique(bus2, bus1, Yval, Ybus)
    fillYbusAdd(bus1, bus1, -Yval, Ybus)
    fillYbusAdd(bus2, bus2, -Yval, Ybus)


def fillYbusSwap_lines(bus1, bus2, Yval, Ybus):
    #print('fillYbusSwap_lines bus1: ' + bus1 + ', bus2: ' + bus2, flush=True)
    fillYbusUniqueUpper_lines(bus2, bus1, Yval, Ybus)

    # extract the node and phase from bus1 and bus2
    node1,phase1 = bus1.split('.')
    node2,phase2 = bus2.split('.')

    # mix-and-match nodes and phases for filling Ybus
    fillYbusAdd(bus1, node1+'.'+phase2, -Yval, Ybus)
    fillYbusAdd(node2+'.'+phase1, bus2, -Yval, Ybus)


def fill_Ybus_PerLengthPhaseImpedance_lines(sparql_mgr, Ybus):
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

        # we now have the negated inverted matrix for comparison
        line_idx += 1

        if Ycomp.size == 1:
            fillYbusNoSwap_lines(bus1+ybusPhaseIdx[phase], bus2+ybusPhaseIdx[phase], Ycomp[0,0], Ybus)

        elif Ycomp.size == 4:
            if line_idx == 1:
                pair_i0b1 = bus1 + ybusPhaseIdx[phase]
                pair_i0b2 = bus2 + ybusPhaseIdx[phase]
            else:
                pair_i1b1 = bus1 + ybusPhaseIdx[phase]
                pair_i1b2 = bus2 + ybusPhaseIdx[phase]

                fillYbusNoSwap_lines(pair_i0b1, pair_i0b2, Ycomp[0,0], Ybus)
                fillYbusSwap_lines(pair_i1b1, pair_i0b2, Ycomp[1,0], Ybus)
                fillYbusNoSwap_lines(pair_i1b1, pair_i1b2, Ycomp[1,1], Ybus)

        elif Ycomp.size == 9:
            if line_idx == 1:
                pair_i0b1 = bus1 + ybusPhaseIdx[phase]
                pair_i0b2 = bus2 + ybusPhaseIdx[phase]
            elif line_idx == 2:
                pair_i1b1 = bus1 + ybusPhaseIdx[phase]
                pair_i1b2 = bus2 + ybusPhaseIdx[phase]
            else:
                pair_i2b1 = bus1 + ybusPhaseIdx[phase]
                pair_i2b2 = bus2 + ybusPhaseIdx[phase]

                fillYbusNoSwap_lines(pair_i0b1, pair_i0b2, Ycomp[0,0], Ybus)
                fillYbusSwap_lines(pair_i1b1, pair_i0b2, Ycomp[1,0], Ybus)
                fillYbusNoSwap_lines(pair_i1b1, pair_i1b2, Ycomp[1,1], Ybus)
                fillYbusSwap_lines(pair_i2b1, pair_i0b2, Ycomp[2,0], Ybus)
                fillYbusSwap_lines(pair_i2b1, pair_i1b2, Ycomp[2,1], Ybus)
                fillYbusNoSwap_lines(pair_i2b1, pair_i2b2, Ycomp[2,2], Ybus)


def fill_Ybus_PerLengthSequenceImpedance_lines(sparql_mgr, Ybus):
    bindings = sparql_mgr.PerLengthSequenceImpedance_line_configs()
    #print('LINE_MODEL_FILL_YBUS PerLengthSequenceImpedance line_configs query results:', flush=True)
    #print(bindings, flush=True)

    if len(bindings) == 0:
        return

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

        fillYbusNoSwap_lines(bus1+'.1', bus2+'.1', Ycomp[0,0], Ybus)
        fillYbusSwap_lines(bus1+'.2', bus2+'.1', Ycomp[1,0], Ybus)
        fillYbusNoSwap_lines(bus1+'.2', bus2+'.2', Ycomp[1,1], Ybus)
        fillYbusSwap_lines(bus1+'.3', bus2+'.1', Ycomp[2,0], Ybus)
        fillYbusSwap_lines(bus1+'.3', bus2+'.2', Ycomp[2,1], Ybus)
        fillYbusNoSwap_lines(bus1+'.3', bus2+'.3', Ycomp[2,2], Ybus)


def fill_Ybus_ACLineSegment_lines(sparql_mgr, Ybus):
    bindings = sparql_mgr.ACLineSegment_line_names()
    #print('LINE_MODEL_FILL_YBUS ACLineSegment line_names query results:', flush=True)
    #print(bindings, flush=True)

    if len(bindings) == 0:
        return

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
        #print('Ycomp: ' + str(Ycomp) + '\n')

        fillYbusNoSwap_lines(bus1+'.1', bus2+'.1', Ycomp[0,0], Ybus)
        fillYbusSwap_lines(bus1+'.2', bus2+'.1', Ycomp[1,0], Ybus)
        fillYbusNoSwap_lines(bus1+'.2', bus2+'.2', Ycomp[1,1], Ybus)
        fillYbusSwap_lines(bus1+'.3', bus2+'.1', Ycomp[2,0], Ybus)
        fillYbusSwap_lines(bus1+'.3', bus2+'.2', Ycomp[2,1], Ybus)
        fillYbusNoSwap_lines(bus1+'.3', bus2+'.3', Ycomp[2,2], Ybus)


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


def fill_Ybus_WireInfo_and_WireSpacingInfo_lines(sparql_mgr, Ybus):
    # WireSpacingInfo query
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

            if Ycomp.size == 1:
                fillYbusNoSwap_lines(pair_i0b1, pair_i0b2, Ycomp[0,0], Ybus)

            elif Ycomp.size == 4:
                fillYbusNoSwap_lines(pair_i0b1, pair_i0b2, Ycomp[0,0], Ybus)
                fillYbusSwap_lines(pair_i1b1, pair_i0b2, Ycomp[1,0], Ybus)
                fillYbusNoSwap_lines(pair_i1b1, pair_i1b2, Ycomp[1,1], Ybus)

            elif Ycomp.size == 9:
                fillYbusNoSwap_lines(pair_i0b1, pair_i0b2, Ycomp[0,0], Ybus)
                fillYbusSwap_lines(pair_i1b1, pair_i0b2, Ycomp[1,0], Ybus)
                fillYbusNoSwap_lines(pair_i1b1, pair_i1b2, Ycomp[1,1], Ybus)
                fillYbusSwap_lines(pair_i2b1, pair_i0b2, Ycomp[2,0], Ybus)
                fillYbusSwap_lines(pair_i2b1, pair_i1b2, Ycomp[2,1], Ybus)
                fillYbusNoSwap_lines(pair_i2b1, pair_i2b2, Ycomp[2,2], Ybus)

            phaseIdx = 0
        else:
            phaseIdx += 1

# FINISH LINES

# START TRANSFORMERS
def fillYbus_6x6_xfmrs(bus1, bus2, DY_flag, Ycomp, Ybus):
    # fill Ybus directly from Ycomp
    # first fill the ones that are independent of DY_flag
    # either because the same bus is used or the same phase
    fillYbusAdd(bus1+'.1', bus1+'.1', Ycomp[0,0], Ybus)
    fillYbusAdd(bus1+'.2', bus1+'.1', Ycomp[1,0], Ybus)
    fillYbusAdd(bus1+'.2', bus1+'.2', Ycomp[1,1], Ybus)
    fillYbusAdd(bus1+'.3', bus1+'.1', Ycomp[2,0], Ybus)
    fillYbusAdd(bus1+'.3', bus1+'.2', Ycomp[2,1], Ybus)
    fillYbusAdd(bus1+'.3', bus1+'.3', Ycomp[2,2], Ybus)
    fillYbusUnique(bus2+'.1', bus1+'.1', Ycomp[3,0], Ybus)
    fillYbusAdd(bus2+'.1', bus2+'.1', Ycomp[3,3], Ybus)
    fillYbusUnique(bus2+'.2', bus1+'.2', Ycomp[4,1], Ybus)
    fillYbusAdd(bus2+'.2', bus2+'.1', Ycomp[4,3], Ybus)
    fillYbusAdd(bus2+'.2', bus2+'.2', Ycomp[4,4], Ybus)
    fillYbusUnique(bus2+'.3', bus1+'.3', Ycomp[5,2], Ybus)
    fillYbusAdd(bus2+'.3', bus2+'.1', Ycomp[5,3], Ybus)
    fillYbusAdd(bus2+'.3', bus2+'.2', Ycomp[5,4], Ybus)
    fillYbusAdd(bus2+'.3', bus2+'.3', Ycomp[5,5], Ybus)

    # now fill the ones that are dependent on DY_flag, which
    # are different bus and different phase
    if DY_flag:
        fillYbusUnique(bus2+'.1', bus1+'.2', Ycomp[4,0], Ybus)
        fillYbusUnique(bus2+'.1', bus1+'.3', Ycomp[5,0], Ybus)
        fillYbusUnique(bus2+'.2', bus1+'.1', Ycomp[3,1], Ybus)
        fillYbusUnique(bus2+'.2', bus1+'.3', Ycomp[5,1], Ybus)
        fillYbusUnique(bus2+'.3', bus1+'.1', Ycomp[3,2], Ybus)
        fillYbusUnique(bus2+'.3', bus1+'.2', Ycomp[4,2], Ybus)

    else:
        fillYbusUnique(bus2+'.2', bus1+'.1', Ycomp[4,0], Ybus)
        fillYbusUnique(bus2+'.3', bus1+'.1', Ycomp[5,0], Ybus)
        fillYbusUnique(bus2+'.1', bus1+'.2', Ycomp[3,1], Ybus)
        fillYbusUnique(bus2+'.3', bus1+'.2', Ycomp[5,1], Ybus)
        fillYbusUnique(bus2+'.1', bus1+'.3', Ycomp[3,2], Ybus)
        fillYbusUnique(bus2+'.2', bus1+'.3', Ycomp[4,2], Ybus)


def fill_Ybus_PowerTransformerEnd_xfmrs(sparql_mgr, Ybus):
    bindings = sparql_mgr.PowerTransformerEnd_xfmr_impedances()
    #print('POWER_TRANSFORMER_FILL_YBUS PowerTransformerEnd xfmr_impedances query results:', flush=True)
    #print(bindings, flush=True)

    if len(bindings) == 0:
        return

    Mesh_x_ohm = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        #from_end = int(obj['from_end']['value'])
        #to_end = int(obj['to_end']['value'])
        #r_ohm = float(obj['r_ohm']['value'])
        Mesh_x_ohm[xfmr_name] = float(obj['mesh_x_ohm']['value'])
        #print('xfmr_name: ' + xfmr_name + ', from_end: ' + str(from_end) + ', to_end: ' + str(to_end) + ', r_ohm: ' + str(r_ohm) + ', mesh_x_ohm: ' + str(Mesh_x_ohm[xfmr_name]))

    # Admittances query not currently used
    #bindings = sparql_mgr.PowerTransformerEnd_xfmr_admittances()
    #print('POWER_TRANSFORMER_FILL_YBUS PowerTransformerEnd xfmr_admittances query results:', flush=True)
    #print(bindings, flush=True)

    #if len(bindings) == 0:
    #    return

    bindings = sparql_mgr.PowerTransformerEnd_xfmr_names()
    #print('POWER_TRANSFORMER_FILL_YBUS PowerTransformerEnd xfmr_names query results:', flush=True)
    #print(bindings, flush=True)

    if len(bindings) == 0:
        return

    Bus = {}
    Connection = {}
    RatedS = {}
    RatedU = {}
    R_ohm = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        #vector_group = obj['vector_group']['value']
        end_number = int(obj['end_number']['value'])
        # can't handle 3-winding transformers so issue a warning and skip
        # to the next transformer in that case
        if end_number == 3:
            bus1 = Bus[xfmr_name][1]
            bus2 = Bus[xfmr_name][2]
            bus3 = obj['bus']['value'].upper()
            print('\n*** WARNING: 3-winding, 3-phase PowerTransformerEnd transformers are not supported, xfmr: ' + xfmr_name + ', bus1: ' + bus1 + ', bus2: ' + bus2 + ', bus3: ' + bus3 + '\n', flush=True)
            Unsupported[bus1] = Unsupported[bus2] = Unsupported[bus3] = [(bus1, bus2, bus3), '3-winding 3-phase transformer']

            # need to clear out the previous dictionary entries for this
            # 3-winding transformer so it isn't processed below
            Bus.pop(xfmr_name, None)
            Connection.pop(xfmr_name, None)
            RatedS.pop(xfmr_name, None)
            RatedU.pop(xfmr_name, None)
            R_ohm.pop(xfmr_name, None)
            continue

        if xfmr_name not in Bus:
            Bus[xfmr_name] = {}
            Connection[xfmr_name] = {}
            RatedS[xfmr_name] = {}
            RatedU[xfmr_name] = {}
            R_ohm[xfmr_name] = {}

        Bus[xfmr_name][end_number] = obj['bus']['value'].upper()
        #base_voltage = int(obj['base_voltage']['value'])
        Connection[xfmr_name][end_number] = obj['connection']['value']
        RatedS[xfmr_name][end_number] = int(float(obj['ratedS']['value']))
        RatedU[xfmr_name][end_number] = int(obj['ratedU']['value'])
        R_ohm[xfmr_name][end_number] = float(obj['r_ohm']['value'])
        #angle = int(obj['angle']['value'])
        #grounded = obj['grounded']['value']
        #r_ground = obj['r_ground']['value']
        #x_ground = obj['x_ground']['value']
        #print('xfmr_name: ' + xfmr_name + ', end_number: ' + str(end_number) + ', bus: ' + Bus[xfmr_name][end_number] + ', connection: ' + Connection[xfmr_name][end_number] + ', ratedS: ' + str(RatedS[xfmr_name][end_number]) + ', ratedU: ' + str(RatedU[xfmr_name][end_number]) + ', r_ohm: ' + str(R_ohm[xfmr_name][end_number]))

    # initialize B upfront because it's constant
    B = np.zeros((6,3))
    B[0,0] = B[2,1] = B[4,2] =  1.0
    B[1,0] = B[3,1] = B[5,2] = -1.0
    #print(B)

    # initialize Y and D matrices, also constant, used to set A later
    Y1 = np.zeros((4,12))
    Y1[0,0] = Y1[1,4] = Y1[2,8] = Y1[3,1] = Y1[3,5] = Y1[3,9] = 1.0
    Y2 = np.zeros((4,12))
    Y2[0,2] = Y2[1,6] = Y2[2,10] = Y2[3,3] = Y2[3,7] = Y2[3,11] = 1.0
    D1 = np.zeros((4,12))
    D1[0,0] = D1[0,9] = D1[1,1] = D1[1,4] = D1[2,5] = D1[2,8] = 1.0
    D2 = np.zeros((4,12))
    D2[0,2] = D2[0,11] = D2[1,3] = D2[1,6] = D2[2,7] = D2[2,10] = 1.0

    for xfmr_name in Bus:
        # Note that division is always floating point in Python 3 even if
        # operands are integer
        zBaseP = (RatedU[xfmr_name][1]*RatedU[xfmr_name][1])/RatedS[xfmr_name][1]
        #zBaseS = (RatedU[xfmr_name][2]*RatedU[xfmr_name][2])/RatedS[xfmr_name][2]
        r_ohm_pu = R_ohm[xfmr_name][1]/zBaseP
        mesh_x_ohm_pu = Mesh_x_ohm[xfmr_name]/zBaseP
        zsc_1V = complex(2.0*r_ohm_pu, mesh_x_ohm_pu) * (3.0/RatedS[xfmr_name][1])
        #print('xfmr_name: ' + xfmr_name + ', zBaseP: ' + str(zBaseP) + ', r_ohm_pu: ' + str(r_ohm_pu) + ', mesh_x_ohm_pu: ' + str(mesh_x_ohm_pu) + ', zsc_1V: ' + str(zsc_1V))

        # initialize ZB
        ZB = np.zeros((3,3), dtype=complex)
        ZB[0,0] = ZB[1,1] = ZB[2,2] = zsc_1V
        #print(ZB)

        # set both Vp/Vs for N and top/bottom for A
        if Connection[xfmr_name][1] == 'Y':
            Vp = RatedU[xfmr_name][1]/math.sqrt(3.0)
            top = Y1
        else:
            Vp = RatedU[xfmr_name][1]
            top = D1

        if Connection[xfmr_name][2] == 'Y':
            Vs = RatedU[xfmr_name][2]/math.sqrt(3.0)
            bottom = Y2
        else:
            Vs = RatedU[xfmr_name][2]
            bottom = D2

        # initialize N
        N = np.zeros((12,6))
        N[0,0] = N[4,2] = N[8,4] =   1.0/Vp
        N[1,0] = N[5,2] = N[9,4] =  -1.0/Vp
        N[2,1] = N[6,3] = N[10,5] =  1.0/Vs
        N[3,1] = N[7,3] = N[11,5] = -1.0/Vs
        #print(N)

        # initialize A
        A = np.vstack((top, bottom))
        #print(A)

        # compute Ycomp = A x N x B x inv(ZB) x B' x N' x A'
        # there are lots of ways to break this up including not at all, but
        # here's one way that keeps it from looking overly complex
        ANB = np.matmul(np.matmul(A, N), B)
        ANB_invZB = np.matmul(ANB, np.linalg.inv(ZB))
        ANB_invZB_Bp = np.matmul(ANB_invZB, np.transpose(B))
        ANB_invZB_BpNp = np.matmul(ANB_invZB_Bp, np.transpose(N))
        Ycomp = np.matmul(ANB_invZB_BpNp, np.transpose(A))
        #print(Ycomp)

        bus1 = Bus[xfmr_name][1]
        bus2 = Bus[xfmr_name][2]

        # set special case flag that indicates if we need to swap the phases
        # for each bus to do the Ybus matching
        connect_DY_flag = Connection[xfmr_name][1]=='D' and Connection[xfmr_name][2]=='Y'

        # delete row and column 8 and 4 making a 6x6 matrix
        Ycomp = np.delete(Ycomp, 7, 0)
        Ycomp = np.delete(Ycomp, 7, 1)
        Ycomp = np.delete(Ycomp, 3, 0)
        Ycomp = np.delete(Ycomp, 3, 1)

        fillYbus_6x6_xfmrs(bus1, bus2, connect_DY_flag, Ycomp, Ybus)


def fill_Ybus_TransformerTank_xfmrs(sparql_mgr, Ybus):
    bindings = sparql_mgr.TransformerTank_xfmr_rated()
    #print('POWER_TRANSFORMER_FILL_YBUS TransformerTank xfmr_rated query results:', flush=True)
    #print(bindings, flush=True)

    if len(bindings) == 0:
        return

    RatedS = {}
    RatedU = {}
    Connection = {}
    R_ohm = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        #xfmr_code = obj['xfmr_code']['value']
        enum = int(obj['enum']['value'])

        if xfmr_name not in RatedS:
            RatedS[xfmr_name] = {}
            RatedU[xfmr_name] = {}
            Connection[xfmr_name] = {}
            R_ohm[xfmr_name] = {}

        RatedS[xfmr_name][enum] = int(float(obj['ratedS']['value']))
        RatedU[xfmr_name][enum] = int(obj['ratedU']['value'])
        Connection[xfmr_name][enum] = obj['connection']['value']
        #angle = int(obj['angle']['value'])
        R_ohm[xfmr_name][enum] = float(obj['r_ohm']['value'])
        #print('xfmr_name: ' + xfmr_name + ', xfmr_code: ' + xfmr_code + ', enum: ' + str(enum) + ', ratedS: ' + str(RatedS[xfmr_name][enum]) + ', ratedU: ' + str(RatedU[xfmr_name][enum]) + ', connection: ' + Connection[xfmr_name][enum] + ', angle: ' + str(angle) + ', r_ohm: ' + str(R_ohm[xfmr_name][enum]))

    bindings = sparql_mgr.TransformerTank_xfmr_sct()
    #print('POWER_TRANSFORMER_FILL_YBUS TransformerTank xfmr_sct query results:', flush=True)
    #print(bindings, flush=True)

    Leakage_z = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        enum = int(obj['enum']['value'])
        #gnum = int(obj['gnum']['value'])
        if xfmr_name not in Leakage_z:
            Leakage_z[xfmr_name] = {}

        Leakage_z[xfmr_name][enum] = float(obj['leakage_z']['value'])
        #loadloss = float(obj['loadloss']['value'])
        #print('xfmr_name: ' + xfmr_name + ', enum: ' + str(enum) + ', gnum: ' + str(gnum) + ', leakage_z: ' + str(Leakage_z[xfmr_name][enum]) + ', loadloss: ' + str(loadloss))

    #bindings = sparql_mgr.TransformerTank_xfmr_nlt()
    #print('POWER_TRANSFORMER_FILL_YBUS TransformerTank xfmr_nlt query results:', flush=True)
    #print(bindings, flush=True)

    bindings = sparql_mgr.TransformerTank_xfmr_names()
    #print('POWER_TRANSFORMER_FILL_YBUS TransformerTank xfmr_names query results:', flush=True)
    #print(bindings, flush=True)

    if len(bindings) == 0:
        return

    Bus = {}
    Phase = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        #xfmr_code = obj['xfmr_code']['value']
        #vector_group = obj['vector_group']['value']
        enum = int(obj['enum']['value'])

        if xfmr_name not in Bus:
            Bus[xfmr_name] = {}
            Phase[xfmr_name] = {}

        Bus[xfmr_name][enum] = obj['bus']['value'].upper()
        #baseV = int(obj['baseV']['value'])
        Phase[xfmr_name][enum] = obj['phase']['value']
        #grounded = obj['grounded']['value']
        #rground = obj['rground']['value']
        #xground = obj['xground']['value']
        #print('xfmr_name: ' + xfmr_name + ', enum: ' + str(enum) + ', bus: ' + Bus[xfmr_name][enum] + ', phase: ' + Phase[xfmr_name][enum])
        #print('xfmr_name: ' + xfmr_name + ', xfmr_code: ' + xfmr_code + ', vector_group: ' + vector_group + ', enum: ' + str(enum) + ', bus: ' + Bus[xfmr_name][enum] + ', baseV: ' + str(baseV) + ', phase: ' + Phase[xfmr_name][enum] + ', grounded: ' + grounded)

    # initialize different variations of B upfront and then figure out later
    # which to use for each transformer
    # 3-phase
    B = {}
    B['3p'] = np.zeros((6,3))
    B['3p'][0,0] = B['3p'][2,1] = B['3p'][4,2] =  1.0
    B['3p'][1,0] = B['3p'][3,1] = B['3p'][5,2] = -1.0
    #print(B['3p'])
    # 1-phase, 2-windings
    B['2w'] = np.zeros((2,1))
    B['2w'][0,0] =  1.0
    B['2w'][1,0] = -1.0
    # 1-phase, 3-windings
    B['3w'] = np.zeros((3,2))
    B['3w'][0,0] = B['3w'][0,1] = B['3w'][2,1] = 1.0
    B['3w'][1,0] = -1.0

    # initialize Y and D matrices, also constant, used to set A for
    # 3-phase transformers
    Y1_3p = np.zeros((4,12))
    Y1_3p[0,0] = Y1_3p[1,4] = Y1_3p[2,8] = Y1_3p[3,1] = Y1_3p[3,5] = Y1_3p[3,9] = 1.0
    Y2_3p = np.zeros((4,12))
    Y2_3p[0,2] = Y2_3p[1,6] = Y2_3p[2,10] = Y2_3p[3,3] = Y2_3p[3,7] = Y2_3p[3,11] = 1.0
    D1_3p = np.zeros((4,12))
    D1_3p[0,0] = D1_3p[0,9] = D1_3p[1,1] = D1_3p[1,4] = D1_3p[2,5] = D1_3p[2,8] = 1.0
    D2_3p = np.zeros((4,12))
    D2_3p[0,2] = D2_3p[0,11] = D2_3p[1,3] = D2_3p[1,6] = D2_3p[2,7] = D2_3p[2,10] = 1.0

    # initialize A for each transformer variation
    A = {}
    A['2w'] = np.identity(4)
    A['3w'] = np.identity(6)
    A['3p_YY'] = np.vstack((Y1_3p, Y2_3p))
    A['3p_DD'] = np.vstack((D1_3p, D2_3p))
    A['3p_YD'] = np.vstack((Y1_3p, D2_3p))
    A['3p_DY'] = np.vstack((D1_3p, Y2_3p))

    # map transformer query phase values to nodelist indexes
    ybusPhaseIdx = {'A': '.1', 'B': '.2', 'C': '.3', 's1': '.1', 's2': '.2'}

    for xfmr_name in Bus:
        # determine the type of transformer to drive the computation
        if Phase[xfmr_name][1] == 'ABC':
            # 3-phase
            Bkey = '3p'
            Akey = Bkey + '_' + Connection[xfmr_name][1] + Connection[xfmr_name][2]
        elif 3 in Phase[xfmr_name]:
            # 1-phase, 3-winding
            Bkey = '3w'
            Akey = Bkey
        else:
            # 1-phase, 2-winding
            Bkey = '2w'
            Akey = Bkey

        # note that division is always floating point in Python 3 even if
        # operands are integer
        zBaseP = (RatedU[xfmr_name][1]*RatedU[xfmr_name][1])/RatedS[xfmr_name][1]
        zBaseS = (RatedU[xfmr_name][2]*RatedU[xfmr_name][2])/RatedS[xfmr_name][2]
        r_ohm_pu = R_ohm[xfmr_name][1]/zBaseP
        mesh_x_ohm_pu = Leakage_z[xfmr_name][1]/zBaseP

        if Bkey == '3p':
            zsc_1V = complex(2.0*r_ohm_pu, mesh_x_ohm_pu) * (3.0/RatedS[xfmr_name][1])
            # initialize ZB
            ZB = np.zeros((3,3), dtype=complex)
            ZB[0,0] = ZB[1,1] = ZB[2,2] = zsc_1V

            # initialize N
            if Connection[xfmr_name][1] == 'Y':
                Vp = RatedU[xfmr_name][1]/math.sqrt(3.0)
            else:
                Vp = RatedU[xfmr_name][1]

            if Connection[xfmr_name][2] == 'Y':
                Vs = RatedU[xfmr_name][2]/math.sqrt(3.0)
            else:
                Vs = RatedU[xfmr_name][2]

            N = np.zeros((12,6))
            N[0,0] = N[4,2] = N[8,4] =   1.0/Vp
            N[1,0] = N[5,2] = N[9,4] =  -1.0/Vp
            N[2,1] = N[6,3] = N[10,5] =  1.0/Vs
            N[3,1] = N[7,3] = N[11,5] = -1.0/Vs

        elif Bkey == '3w':
            zsc_1V = complex(3.0*r_ohm_pu, mesh_x_ohm_pu) * (1.0/RatedS[xfmr_name][1])
            zod_1V = complex(2.0*R_ohm[xfmr_name][2], Leakage_z[xfmr_name][2])/zBaseS * (1.0/RatedS[xfmr_name][2])
            # initialize ZB
            ZB = np.zeros((2,2), dtype=complex)
            ZB[0,0] = ZB[1,1] = zsc_1V
            ZB[1,0] = ZB[0,1] = 0.5*(zsc_1V + zsc_1V - zod_1V)

            #initialize N
            Vp = RatedU[xfmr_name][1]
            Vs1 = RatedU[xfmr_name][2]
            Vs2 = RatedU[xfmr_name][3]

            N = np.zeros((6,3))
            N[0,0] =  1.0/Vp
            N[1,0] = -1.0/Vp
            N[2,1] =  1.0/Vs1
            N[3,1] = -1.0/Vs1
            N[4,2] = -1.0/Vs2
            N[5,2] =  1.0/Vs2

        else:
            zsc_1V = complex(2.0*r_ohm_pu, mesh_x_ohm_pu) * (1.0/RatedS[xfmr_name][1])
            # initialize ZB
            ZB = np.zeros((1,1), dtype=complex)
            ZB[0,0] = zsc_1V

            #initialize N
            Vp = RatedU[xfmr_name][1]
            Vs = RatedU[xfmr_name][2]

            N = np.zeros((4,2))
            N[0,0] =  1.0/Vp
            N[1,0] = -1.0/Vp
            N[2,1] =  1.0/Vs
            N[3,1] = -1.0/Vs

        # compute Ycomp = A x N x B x inv(ZB) x B' x N' x A'
        # there are lots of ways to break this up including not at all, but
        # here's one way that keeps it from looking overly complex
        ANB = np.matmul(np.matmul(A[Akey], N), B[Bkey])
        ANB_invZB = np.matmul(ANB, np.linalg.inv(ZB))
        ANB_invZB_Bp = np.matmul(ANB_invZB, np.transpose(B[Bkey]))
        ANB_invZB_BpNp = np.matmul(ANB_invZB_Bp, np.transpose(N))
        Ycomp = np.matmul(ANB_invZB_BpNp, np.transpose(A[Akey]))
        #print(Ycomp)

        # do Ybus comparisons and determine overall transformer status color
        xfmrColorIdx = 0

        if Bkey == '3p':
            bus1 = Bus[xfmr_name][1]
            bus2 = Bus[xfmr_name][2]

            # delete row and column 8 and 4 making a 6x6 matrix
            Ycomp = np.delete(Ycomp, 7, 0)
            Ycomp = np.delete(Ycomp, 7, 1)
            Ycomp = np.delete(Ycomp, 3, 0)
            Ycomp = np.delete(Ycomp, 3, 1)

            fillYbus_6x6_xfmrs(bus1, bus2, Akey=='3p_DY', Ycomp, Ybus)

        elif Bkey == '3w':
            bus1 = Bus[xfmr_name][1] + ybusPhaseIdx[Phase[xfmr_name][1]]
            bus2 = Bus[xfmr_name][2] + ybusPhaseIdx[Phase[xfmr_name][2]]
            bus3 = Bus[xfmr_name][3] + ybusPhaseIdx[Phase[xfmr_name][3]]

            # split phase transformers are a bit tricky, but Shiva
            # figured out how it needs to be done with reducing the
            # matrix and how the 3 buses come into it

            # delete row and column 5, 4, and 2 making a 3x3 matrix
            Ycomp = np.delete(Ycomp, 4, 0)
            Ycomp = np.delete(Ycomp, 4, 1)
            Ycomp = np.delete(Ycomp, 3, 0)
            Ycomp = np.delete(Ycomp, 3, 1)
            Ycomp = np.delete(Ycomp, 1, 0)
            Ycomp = np.delete(Ycomp, 1, 1)

            fillYbusAdd(bus1, bus1, Ycomp[0,0], Ybus)
            fillYbusUnique(bus2, bus1, Ycomp[1,0], Ybus)
            fillYbusAdd(bus2, bus2, Ycomp[1,1], Ybus)
            fillYbusUnique(bus3, bus1, Ycomp[2,0], Ybus)
            fillYbusAdd(bus3, bus2, Ycomp[2,1], Ybus)
            fillYbusAdd(bus3, bus3, Ycomp[2,2], Ybus)

        else:
            bus1 = Bus[xfmr_name][1] + ybusPhaseIdx[Phase[xfmr_name][1]]
            bus2 = Bus[xfmr_name][2] + ybusPhaseIdx[Phase[xfmr_name][2]]
            Yval = Ycomp[2,0]

            # delete row and column 4 and 2 making a 2x2 matrix
            Ycomp = np.delete(Ycomp, 3, 0)
            Ycomp = np.delete(Ycomp, 3, 1)
            Ycomp = np.delete(Ycomp, 1, 0)
            Ycomp = np.delete(Ycomp, 1, 1)

            fillYbusAdd(bus1, bus1, Ycomp[0,0], Ybus)
            fillYbusUnique(bus2, bus1, Ycomp[1,0], Ybus)
            fillYbusAdd(bus2, bus2, Ycomp[1,1], Ybus)
# FINISH TRANSFORMERS

# START SWITCHES
def fillYbusUnique_switches(bus1, bus2, Ybus):
    if bus1 not in Ybus:
        Ybus[bus1] = {}
    if bus2 not in Ybus:
        Ybus[bus2] = {}

    if bus2 in Ybus[bus1]:
        print('    *** WARNING: Unexpected existing value found for Ybus[' + bus1 + '][' + bus2 + '] when filling switching equipment value\n', flush=True)

    # if needed, here's how to find the two immediate calling functions
    #if bus2=='X2673305B.1' and bus1=='X2673305B.2':
    #    print('*** fillYbusUnique bus1: ' + bus1 + ', bus2: ' + bus2 + ', caller: ' + str(inspect.stack()[1].function) + ', ' + str(inspect.stack()[2].function), flush=True)

    Ybus[bus1][bus2] = Ybus[bus2][bus1] = complex(-500.0, 500.0)


def fillYbusAdd_switches(bus1, bus2, Ybus):
    if bus1 not in Ybus:
        Ybus[bus1] = {}
    if bus2 not in Ybus:
        Ybus[bus2] = {}

    if bus2 in Ybus[bus1]:
        Ybus[bus1][bus2] += complex(500.0, -500.0)
        Ybus[bus2][bus1] = Ybus[bus1][bus2]
    else:
        Ybus[bus1][bus2] = Ybus[bus2][bus1] = complex(500.0, -500.0)


def fillYbusNoSwap_switches(bus1, bus2, is_Open, Ybus):
    #print('fillYbusNoSwap bus1: ' + bus1 + ', bus2: ' + bus2 + ', is_Open: ' + str(is_Open), flush=True)
    if not is_Open:
        fillYbusUnique_switches(bus2, bus1, Ybus)
        fillYbusAdd_switches(bus1, bus1, Ybus)
        fillYbusAdd_switches(bus2, bus2, Ybus)

def fill_Ybus_SwitchingEquipment_switches(sparql_mgr, Ybus):
    bindings = sparql_mgr.SwitchingEquipment_switch_names()
    #print('SWITCHING_EQUIPMENT_FILL_YBUS switch_names query results:', flush=True)
    #print(bindings, flush=True)

    if len(bindings) == 0:
        return

    # map transformer query phase values to nodelist indexes
    ybusPhaseIdx = {'A': '.1', 'B': '.2', 'C': '.3'}

    for obj in bindings:
        sw_name = obj['sw_name']['value']
        #base_V = int(obj['base_V']['value'])
        is_Open = obj['is_Open']['value'].upper() == 'TRUE'
        #rated_Current = int(obj['rated_Current']['value'])
        #breaking_Capacity = int(obj['breaking_Capacity']['value'])
        #sw_ph_status = obj['sw_ph_status']['value']
        bus1 = obj['bus1']['value'].upper()
        bus2 = obj['bus2']['value'].upper()
        phases_side1 = obj['phases_side1']['value']
        #phases_side2 = obj['phases_side2']['value']
        #print('sw_name: ' + sw_name + ', is_Open: ' + str(is_Open) + ', bus1: ' + bus1 + ', bus2: ' + bus2 + ', phases_side1: (' + phases_side1 + ')' + ', phases_side2: (' + phases_side2 + ')')

        if phases_side1 == '':
            # 3-phase switch
            #print('3-phase switch found bus1: ' + bus1 + ', bus2: ' + bus2, flush=True)
            fillYbusNoSwap_switches(bus1+'.1', bus2+'.1', is_Open, Ybus)
            fillYbusNoSwap_switches(bus1+'.2', bus2+'.2', is_Open, Ybus)
            fillYbusNoSwap_switches(bus1+'.3', bus2+'.3', is_Open, Ybus)

        else:
            # 1- or 2-phase switch
            switchColorIdx = 0
            for phase in phases_side1:
                #print('1/2-phase switch found phase: ' + phase + ', bus1: ' + bus1 + ', bus2: ' + bus2, flush=True)
                if phase in ybusPhaseIdx:
                    fillYbusNoSwap_switches(bus1+ybusPhaseIdx[phase], bus2+ybusPhaseIdx[phase], is_Open, Ybus)
# FINISH SWITCHES

# START SHUNTS
def fillYbusOnlyAdd_shunts(bus, Yval, Ybus):
    if Yval == 0j:
        return

    if bus not in Ybus or bus not in Ybus[bus]:
        print('    *** WARNING: Existing value not found for Ybus[' + bus + '][' + bus + '] when adding shunt element model contribution\n', flush=True)
        return

    Ybus[bus][bus] += Yval


def fill_Ybus_ShuntElement_shunts(sparql_mgr, Ybus):
    # map query phase values to nodelist indexes
    ybusPhaseIdx = {'A': '.1', 'B': '.2', 'C': '.3', 's1': '.1', 's2': '.2'}

    # CAPACITORS DATA STRUCTURES INITIALIZATION
    bindings = sparql_mgr.ShuntElement_cap_names()
    #print('SHUNT_ELEMENT_FILL_YBUS ShuntElement cap_names query results:', flush=True)
    #print(bindings, flush=True)

    Cap_name = {}
    B_per_section = {}
    for obj in bindings:
        cap_name = obj['cap_name']['value']
        B_per_section[cap_name] = float(obj['b_per_section']['value'])
        bus = obj['bus']['value'].upper()
        phase = 'ABC' # no phase specified indicates 3-phase
        if 'phase' in obj:
            phase = obj['phase']['value']
        mode = 'voltage'
        if 'mode' in obj:
            mode = obj['mode']['value']
        #print('cap_name: ' + cap_name + ', b_per_section: ' + str(b_per_section) + ', phase: ' + phase + ', mode: ' + mode)

        if mode != 'timeScheduled':
            if phase == 'ABC': # 3-phase
                if bus+'.1' not in Cap_name:
                    Cap_name[bus+'.1'] = []
                    Cap_name[bus+'.2'] = []
                    Cap_name[bus+'.3'] = []
                Cap_name[bus+'.1'].append(cap_name)
                Cap_name[bus+'.2'].append(cap_name)
                Cap_name[bus+'.3'].append(cap_name)
            else: # specified phase only
                if bus+ybusPhaseIdx[phase] not in Cap_name:
                    Cap_name[bus+ybusPhaseIdx[phase]] = []
                Cap_name[bus+ybusPhaseIdx[phase]].append(cap_name)
    #print(Cap_name)

    # TRANSFORMERS DATA STRUCTURES INITIALIZATION
    bindings = sparql_mgr.TransformerTank_xfmr_rated()
    #print('SHUNT_ELEMENT_FILL_YBUS TransformerTank xfmr_rated query results:', flush=True)
    #print(bindings, flush=True)

    # TransformerTank queries
    RatedS_tank = {}
    RatedU_tank = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        enum = int(obj['enum']['value'])
        if xfmr_name not in RatedS_tank:
            RatedS_tank[xfmr_name] = {}
            RatedU_tank[xfmr_name] = {}

        RatedS_tank[xfmr_name][enum] = int(float(obj['ratedS']['value']))
        RatedU_tank[xfmr_name][enum] = int(obj['ratedU']['value'])
        #print('xfmr_name: ' + xfmr_name + ', enum: ' + str(enum) + ', ratedS: ' + str(RatedS_tank[xfmr_name][enum]) + ', ratedU: ' + str(RatedU_tank[xfmr_name][enum]))

    bindings = sparql_mgr.TransformerTank_xfmr_nlt()
    #print('SHUNT_ELEMENT_FILL_YBUS TransformerTank xfmr_nlt query results:', flush=True)
    #print(bindings, flush=True)

    Noloadloss = {}
    I_exciting = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        Noloadloss[xfmr_name] = float(obj['noloadloss_kW']['value'])
        I_exciting[xfmr_name] = float(obj['i_exciting']['value'])
        #print('xfmr_name: ' + xfmr_name + ', noloadloss: ' + str(Noloadloss[xfmr_name]) + ', i_exciting: ' + str(I_exciting[xfmr_name]))

    bindings = sparql_mgr.TransformerTank_xfmr_names()
    #print('SHUNT_ELEMENT_FILL_YBUS TransformerTank xfmr_names query results:', flush=True)
    #print(bindings, flush=True)

    Xfmr_tank_name = {}
    Enum_tank = {}
    BaseV_tank = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        enum = int(obj['enum']['value'])
        bus = obj['bus']['value'].upper()
        baseV = float(obj['baseV']['value'])

        if xfmr_name not in Enum_tank:
            Enum_tank[xfmr_name] = {}
            BaseV_tank[xfmr_name] = {}

        Enum_tank[xfmr_name][bus] = enum
        BaseV_tank[xfmr_name][bus] = baseV

        phase = obj['phase']['value']
        if phase == 'ABC':
            if bus+'.1' not in Xfmr_tank_name:
                Xfmr_tank_name[bus+'.1'] = []
                Xfmr_tank_name[bus+'.2'] = []
                Xfmr_tank_name[bus+'.3'] = []
            Xfmr_tank_name[bus+'.1'].append(xfmr_name)
            Xfmr_tank_name[bus+'.2'].append(xfmr_name)
            Xfmr_tank_name[bus+'.3'].append(xfmr_name)
        else:
            if bus+ybusPhaseIdx[phase] not in Xfmr_tank_name:
                Xfmr_tank_name[bus+ybusPhaseIdx[phase]] = []
            Xfmr_tank_name[bus+ybusPhaseIdx[phase]].append(xfmr_name)

        #print('xfmr_tank_name: ' + xfmr_name + ', enum: ' + str(enum) + ', bus: ' + bus + ', phase: ' + phase)

    # TransformerEnd queries
    bindings = sparql_mgr.PowerTransformerEnd_xfmr_admittances()
    #print('SHUNT_ELEMENT_FILL_YBUS PowerTransformerEnd xfmr_admittances query results:', flush=True)
    #print(bindings, flush=True)

    B_S = {}
    G_S = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        B_S[xfmr_name] = float(obj['b_S']['value'])
        G_S[xfmr_name] = float(obj['g_S']['value'])
        #print('xfmr_name: ' + xfmr_name + ', b_S: ' + str(B_S[xfmr_name]) + ', g_S: ' + str(G_S[xfmr_name])

    bindings = sparql_mgr.PowerTransformerEnd_xfmr_names()
    #print('SHUNT_ELEMENT_FILL_YBUS PowerTransformerEnd xfmr_names query results:', flush=True)
    #print(bindings, flush=True)

    Xfmr_end_name = {}
    RatedU_end = {}
    Enum_end = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        enum = int(obj['end_number']['value'])
        bus = obj['bus']['value'].upper()
        if bus+'.1' not in Xfmr_end_name:
            Xfmr_end_name[bus+'.1'] = []
            Xfmr_end_name[bus+'.2'] = []
            Xfmr_end_name[bus+'.3'] = []
        Xfmr_end_name[bus+'.1'].append(xfmr_name)
        Xfmr_end_name[bus+'.2'].append(xfmr_name)
        Xfmr_end_name[bus+'.3'].append(xfmr_name)

        if xfmr_name not in Enum_end:
            Enum_end[xfmr_name] = {}
            RatedU_end[xfmr_name] = {}

        Enum_end[xfmr_name][bus] = enum
        RatedU_end[xfmr_name][enum] = int(obj['ratedU']['value'])
        #print('xfmr_end_name: ' + xfmr_name + ', end_number: ' + str(enum) + ', bus: ' + bus + ', ratedU: ' + str(RatedU_end[xfmr_name][enum]))
  
    for node in Cap_name:
        sum_shunt_imag = 0.0
        for cap in Cap_name[node]:
            # no real component contribution for capacitors
            sum_shunt_imag += B_per_section[cap]
        #print(node, sum_shunt_imag)
        fillYbusOnlyAdd_shunts(node, complex(0.0, sum_shunt_imag), Ybus)

    for node in Xfmr_tank_name:
        sum_shunt_imag = sum_shunt_real = 0.0
        bus = node.split('.')[0]
        for xfmr in Xfmr_tank_name[node]:
            if Enum_tank[xfmr][bus] >= 2:
                # num_elem += 1
                ratedU_sq = RatedU_tank[xfmr][2]*RatedU_tank[xfmr][2]
                zBaseS = ratedU_sq/RatedS_tank[xfmr][2]
                sum_shunt_real += (Noloadloss[xfmr]*1000.0)/ratedU_sq
                #print('Adding tank transformer real contribution: ' + str((Noloadloss[xfmr]*1000.0)/ratedU_sq))
                G_c = (Noloadloss[xfmr]*1000.0)/ratedU_sq
                Ym = I_exciting[xfmr]/(100.0) * RatedS_tank[xfmr][2]/RatedU_tank[xfmr][2] * 1/(RatedU_tank[xfmr][2])
                try:
                    B_m = math.sqrt(Ym ** 2 - G_c ** 2)
                except:
                    B_m = Ym

                sum_shunt_imag += -B_m
        #print(node, sum_shunt_real, sum_shunt_imag)
        fillYbusOnlyAdd_shunts(node, complex(sum_shunt_real, sum_shunt_imag), Ybus)

    for node in Xfmr_end_name:
        sum_shunt_imag = sum_shunt_real = 0.0
        bus = node.split('.')[0]
        for xfmr in Xfmr_end_name[node]:
            if Enum_end[xfmr][bus] == 2:
                ratedU_ratio = RatedU_end[xfmr][1]/RatedU_end[xfmr][2]
                ratedU_sq = ratedU_ratio*ratedU_ratio
                sum_shunt_real += G_S[xfmr]*ratedU_sq
                #print('Adding tank transformer real contribution: ' + str(G_S[xfmr]*ratedU_sq))
                sum_shunt_imag += -B_S[xfmr]*ratedU_sq
        #print(node, sum_shunt_real, sum_shunt_imag)
        fillYbusOnlyAdd_shunts(node, complex(sum_shunt_real, sum_shunt_imag), Ybus)
# FINISH SHUNTS

def count_unique_ybus(Ybus):
    count = 0

    for bus1 in Ybus:
        for bus2 in Ybus[bus1]:
            if bus2 != bus1:
                count += 1

    count = int(count/2) # halve the total for duplicated entries

    for bus1 in Ybus:
        if bus1 in Ybus[bus1]:
            count += 1

    return count


def static_ybus(feeder_mrid):
    SPARQLManager = getattr(importlib.import_module('shared.sparql'), 'SPARQLManager')

    gapps = GridAPPSD()

    sparql_mgr = SPARQLManager(gapps, feeder_mrid)

    print('\nStarting to build static Ybus...', flush=True)

    Ybus = {}

    fill_Ybus_PerLengthPhaseImpedance_lines(sparql_mgr, Ybus)
    fill_Ybus_PerLengthSequenceImpedance_lines(sparql_mgr, Ybus)
    fill_Ybus_ACLineSegment_lines(sparql_mgr, Ybus)
    fill_Ybus_WireInfo_and_WireSpacingInfo_lines(sparql_mgr, Ybus)
    #print('line_model static Ybus...')
    #print(Ybus)
    line_count = count_unique_ybus(Ybus)
    print('\nLine_model # entries: ' + str(line_count), flush=True)

    fill_Ybus_PowerTransformerEnd_xfmrs(sparql_mgr, Ybus)
    fill_Ybus_TransformerTank_xfmrs(sparql_mgr, Ybus)
    #print('power_transformer static Ybus...')
    #print(Ybus)
    count = count_unique_ybus(Ybus)
    xfmr_count = count - line_count
    print('\nPower_transformer # entries: ' + str(xfmr_count), flush=True)

    fill_Ybus_SwitchingEquipment_switches(sparql_mgr, Ybus)
    #print('switching_equipment static Ybus...')
    #print(Ybus)
    count = count_unique_ybus(Ybus)
    switch_count = count - line_count - xfmr_count
    print('\nSwitching_equipment # entries: ' + str(switch_count), flush=True)

    fill_Ybus_ShuntElement_shunts(sparql_mgr, Ybus)
    #print('shunt_element (final) static Ybus...')
    #print(Ybus)
    count = count_unique_ybus(Ybus)
    shunt_count = count - line_count - xfmr_count - switch_count
    print('\nShunt_element # entries: ' + str(shunt_count), flush=True)

    print('\nFull static Ybus:')
    for bus1 in Ybus:
        for bus2 in Ybus[bus1]:
            print(bus1 + ',' + bus2 + ',' + str(Ybus[bus1][bus2].real) + ',' + str(Ybus[bus1][bus2].imag))

    ysysCount = count_unique_ybus(Ybus)
    print('\nTotal static Ybus # entries: ' + str(ysysCount) + '\n', flush=True)

    return Ybus


def _main():
    # for loading modules
    if (os.path.isdir('shared')):
        sys.path.append('.')
    elif (os.path.isdir('../shared')):
        sys.path.append('..')

    parser = argparse.ArgumentParser()
    parser.add_argument("--request", help="Simulation Request")

    opts = parser.parse_args()
    sim_request = json.loads(opts.request.replace("\'",""))
    feeder_mrid = sim_request["power_system_config"]["Line_name"]

    Ybus = static_ybus(feeder_mrid)


if __name__ == "__main__":
    _main()

