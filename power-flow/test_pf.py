#!/usr/bin/env python3

import sys
import time
import os

# gridappsd-python module
from gridappsd import GridAPPSD
from gridappsd.topics import service_output_topic

class PFTester(object):
    def __init__(self,gapps,simulation_id):
        self.simulation_id=simulation_id
        self.keepLoopingFlag=True
        self.initflag=False
        gapps.subscribe(service_output_topic('gridappsd-power-flow',self.simulation_id),self)

        topic='goss.gridappsd.request.data.power-flow.'+self.simulation_id
        request={
            'requestType': "GET_SNAPSHOT_POWERFLOW"
        }
        print('Requesting power flow solution for sim_id'+self.simulation_id+'\n',flush=True)
        message=gapps.get_response(topic,request,timeout=90)
        print('Got response'+str(message)+'\n',flush=True)
        print('\n\n')
        self.initflag=True

    def on_message(self,header,message):
        if 'processStatus' in message:
            if message['processStatus']=='COMPLETE' or message['processStatus']=='CLOSED':
                self.keepLoopingFlag = False
        elif self.initflag:
            print('\n')
            print('Received full PF solution with timestamp: '+str(message['timestamp'])+'\n')
            print(message['powerflow'])

def _main():
    if (os.path.isdir('shared')):
        sys.path.append('.')
    elif (os.path.isdir('../shared')):
        sys.path.append('..')

    if len(sys.argv) < 2:
        usestr =  '\nUsage: ' + sys.argv[0] + ' simulation_id\n'
        print(usestr, flush=True)
        exit()

    simulation_id = sys.argv[1]

    gapps = GridAPPSD() 

    pf=PFTester(gapps,simulation_id)

    print('Monitoring pf solutions')

    while pf.keepLoopingFlag:
        time.sleep(0.1)

    print('Finished monitoring PF results')

    gapps.disconnect()

if __name__ == '__main__':
  _main()

