#!/usr/bin/env python3

# gridappsd-python module
from gridappsd import GridAPPSD


def fullComplex(lowerUncomplex):
  YbusComplex = {}

  for noderow in lowerUncomplex:
    for nodecol,value in lowerUncomplex[noderow].items():
      if noderow not in YbusComplex:
        YbusComplex[noderow] = {}
      if nodecol not in YbusComplex:
        YbusComplex[nodecol] = {}
      YbusComplex[noderow][nodecol] = YbusComplex[nodecol][noderow] = complex(value[0], value[1])

  return YbusComplex


def _main():
  feeder_mrid = '_5B816B93-7A5F-B64C-8460-47C17D6E4B0F' # 13assets
  #feeder_mrid = '_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62' # 13
  #feeder_mrid = '_C1C3E687-6FFD-C753-582B-632A27E28507' # 123
  #feeder_mrid = '_AAE94E4A-2465-6F5E-37B1-3E72183A4E44' # 9500

  gapps = GridAPPSD()

  # request/response for snapshot Ybus
  topic = 'goss.gridappsd.request.data.static-ybus'
  request = {
    "requestType": "GET_SNAPSHOT_YBUS",
    "feeder_id": feeder_mrid
  }
  print('Requesting Static Ybus for feeder_id: ' + feeder_mrid + '\n', flush=True)
  message = gapps.get_response(topic, request, timeout=90)
  print('Got Static Ybus snapshot response: ' + str(message) + '\n', flush=True)

  Ybus = fullComplex(message['ybus'])
  print('Ybus converted from lower diagonal to full with complex values:', flush=True)
  print(Ybus, flush=True)

  gapps.disconnect()


if __name__ == '__main__':
  _main()

