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

  gapps = GridAPPSD()

  # request/response for snapshot Ybus
  topic = 'goss.gridappsd.request.data.static-ybus'
  request = {
    "requestType": "GET_SNAPSHOT_YBUS",
    "feeder_id": feeder_mrid
  }
  print('Requesting static Ybus for feeder_id: ' + feeder_mrid + '\n', flush=True)
  message = gapps.get_response(topic, request, timeout=90)
  print('Got static Ybus snapshot response: ' + str(message) + '\n', flush=True)

  Ybus = fullComplex(message['ybus'])
  print('Ybus converted from lower diagonal to full with complex values:', flush=True)
  print(Ybus, flush=True)

  gapps.disconnect()


if __name__ == '__main__':
  _main()

