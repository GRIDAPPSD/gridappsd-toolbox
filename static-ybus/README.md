# GridAPPS-D Toolbox Static Y-bus

Static Y-bus is a lightweight service that provides Y-bus complex system admittance sparse matrices represented in a Python dictionary of dimension 2, also referred to as a dictionary of a dictionary. Y-bus matrices are constructed from the underlying network equipment model using a method originally developed for the GridAPPS-D Model Validator application. Applications processing as-built models (e.g., outside of a field or simulation context) should use the Static Y-bus service, while applications within an as-operated (e.g., field or simulation) context should use the related Dynamic Y-bus service. The Static Y-bus service supports request/response queries for returning Y-bus matrices.

## Static Y-bus Overview

Static Y-bus operates through the GridAPPS-D request/response messaging pattern. Applications request a Y-bus admittance matrix for a specified feeder, which is then constructed and returned by the GridAPPS-D API get_response call.

Static Y-bus is started automatically when the GridAPPS-D platform is launched and therefore is always available to applications connected to the platform. Internally Y-bus admittance matrices are cached so that only the first request for a given model within a GridAPPS-D platform instance will result in the construction of the matrix. Subsequent requests from any application communicating with that platform instance will immediately return the cached Y-bus matrix that was previously built.

Because of the complexity in deriving the Y-bus matrix directly from the network equipment model, even for simple models it takes 15 or more seconds for an application to receive the Y-bus response when it must be constructed for the initial request, longer for a large model. Caching of matrices therefore results in significantly better performance when there are repeated Y-bus requests.

An example Static Y-bus request/response is provided in the gridappsd-toolbox GitHub repo in static-ybus/test_sybus.py. The following documentation describes the usage of the Static Y-bus service, both the request and response including some limited response processing.

## Service Output Request

The following code snippet shows the topic and request format for returning the Static Y-bus for a model as given by the feeder id:

```
from gridappds import GridAPPSD

gapps = GridAPPSD()

feeder_mrid = "_5B186B93-7A5F-B64C-8640-47C17D6E4B0F" #ieee13assets

topic = "goss.gridappsd.request.data.static-ybus"

request = {
  "requestType": "GET_SNAPSHOT_YBUS",
  "feeder_id": feeder_mrid
}

message = gapps.get_response(topic, request, timeout=90)
```

## Service Output Response

The Static Y-bus response message format, with the ybus element being shortened from the full value,  is as follows:

```
message = {
  "feeder_id": "_5B816B93-7A5F-B64C-8460-47C17D6E4B0F",
  "ybus": {"646.2": {"645.2": [-7.177543473834806, 6.6777250702760815], "646.2": [7.177543473834806, -6.6777250702760815], "645.3": [2.408564072415804, -0.9996874529053109]}, "645.2": {"645.2": [11.468753101398052, -10.603388331034477]}, "645.3": {"645.2": [-3.8413662321995776, 1.5680292873844999], "645.3": [11.547403560992565, -10.5543166996104]},...}
}
```

The ybus element directly maps to a Python dictionary of dimension 2 or a dictionary of a dictionary. The numeric values are the real and imaginary components of the Y-bus admittance for each sparse matrix entry. Note that GridAPPS-D serializes messages using the JSON data interchange format, but unfortunately complex values are not directly supported by JSON. Therefore, each complex value is instead serialized as a two-element tuple.  Further, only unique Y-bus entries are included in the response message that were determined by the Static Y-bus service to be part of the lower diagonal portion of the sparse matrix.  Therefore, most applications will need to both convert the tuples to complex numbers as well as populate the symmetric upper diagonal Y-bus entries to simplify working with the matrix in application code.  A function to do both of those steps along with the code for invoking that function given the response message at the end of the earlier code snippet is as follows:

```
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


# invoke function to build full Y-bus with complex values
Ybus = fullComplex(message['ybus'])
```

See the test_sybus.py script in the static-ybus directory of the gridappsd-toolbox GitHub repo for the complete example of requesting and processing a Y-bus response from the Static Y-bus service.


