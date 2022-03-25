#!/usr/bin/env python3

import sys
import time

# gridappsd-python module
from gridappsd import GridAPPSD

def _main():
  feeder_mrid = '_5B816B93-7A5F-B64C-8460-47C17D6E4B0F' # 13assets

  gapps = GridAPPSD()

  # request/response for snapshot Ybus
  topic = 'goss.gridappsd.request.data.static-ybus'
  request = {
    "requestType": "GET_SNAPSHOT_YBUS",
    "feeder_id": feeder_mrid
  }
  message = gapps.get_response(topic, request, timeout=90)
  print('Got Ybus Snapshot response: ' + str(message) + '\n', flush=True)

  gapps.disconnect()


if __name__ == '__main__':
  _main()

