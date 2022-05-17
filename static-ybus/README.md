# GridAPPS-D Toolbox Static Y-bus

Static Y-bus is a lightweight service that provides Y-bus complex system admittance matrices represented in a Python dictionary of dimension 2, also referred to as a dictionary of a dictionary. Y-bus matrices are constructed from the underlying network equipment model using a method originally developed for the GridAPPS-D Model Validator application. Applications processing as-built models (e.g., outside of a field or simulation context) should use the Static Y-bus service, while applications within an as-operated (e.g., field or simulation) context should use the related Dynamic Y-bus service. The Static Y-bus service supports request/response queries for returning Y-bus matrices.

## Static Y-bus Overview

Static Y-bus operates solely through the GridAPPS-D request/response messaging pattern. Applications request a Y-bus admittance matrix for a specified model, which is then created and subsequently returned by the same GridAPPS-D API get_reponse call from with the request was initiated.

Static Y-bus is started automatically when the GridAPPS-D platform is launched and therefore is always available to applications connected to the platform. Internally Y-bus admittance matrices are cached so that only the first request for a given model within a GridAPPS-D platform instance will result in the construction of the matrix from the underlying network equipment model. Subsequent requests from any application for that platform instance will immediately return the cached Y-bus matrix that was previously built.

Because of the complexity in deriving the Y-bus matrix directly from the network equipment model, even for simple models it takes 15 or more seconds for an application to receive the Y-bus response when it must be constructed for the initial request, longer for a large model. Caching of matrices therefore results in significantly better performance when there are repeated Static Y-bus requests.

An example Static Y-bus request/response is provided in the gridappsd-toolbox GitHub repo in static-ybus/test_sybus.py. The following documentation describes this usage of the Static Y-bus service, both the request and response.

## Service Output Request

Request Y-bus matrix
The following code snippet shows the topic and request format for returning the static Y-bus for a specified feeder mrid:

```
from gridappds import GridAPPSD

gapps = GridAPPSD()

feeder_mrid = "_5B186B93-7A5F-B64C-8640-47C17D6E4B0F" #_ieee13assets

topic = "goss.gridappsd.request.data.static-ybus"

request = {
  "requestType": "GET_SNAPSHOT_YBUS",
  "feeder_id": feeder_mrid
}

message = gapps.get_response(topic, request, timeout=90)
```

## Service Output Response

Response Y-bus matrix format

