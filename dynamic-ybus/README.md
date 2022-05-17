# GridAPPS-D Toolbox Dynamic Y-bus

Dynamic Y-bus is a lightweight service that provides Y-bus complex system admittance sparse matrices represented in a Python dictionary of dimension 2, also referred to as a dictionary of a dictionary. Y-bus matrices are constructed from the underlying network equipment model using a method originally developed for the GridAPPS-D Model Validator application. Applications within an as-operated (e.g., field or simulation) context should use the Dynamic Y-bus service, while applications processing as-built models (e.g., outside of a field or simulation context) should use the related Static Y-bus service. Dynamic Y-bus provides updates from changes in switch, voltage regular, and capacitor states over time. The Dynamic Y-bus service supports both "snapshot" request/response queries along with subscribe/publish messaging for Y-bus updates.

## Dynamic Y-bus Overview

Dynamic Y-bus operates through the GridAPPS-D request/response and subscribe/publish messaging patterns. The basic workflow is that the request/response is first used by an application to initialize a starting Y-bus and subsequent subscribe/publish messages are used to capture updates to the Y-bus over the course of simulation execution. A slight tweak to this workflow is recommended, described below, that avoids the possibility of a publish/subscribe message coming through with a more recent simulation timestamp than the request/response message prior to establishing a starting Y-bus. While this isn't likely, it is possible and would result in the starting Y-bus being out of date and not corrected. The tweak guarantees that Y-bus is initialized using the version with the most recent timestamp, whether that is from the request/response or subscribe/publish message.

Dynamic Y-bus is started with a simulation by the GridAPPS-D platform when it is configured through the Service Configuration tab for setting up a simulation through the platform viz web browser user interface. Select gridappsd-dynamic-ybus-service from the Available Services dropdown menu, hitting the Confirm button on the dropdown menu and then the Apply button on the Available Servies tab. Failing to hit the Apply button will result in Dynamic Y-bus not being started with the simulation. There are no user input options for Dynamic Y-bus beyond selecting it to be run. If a simulation is being invoked through a JSON config file rather than through the platform viz, a service_configs array as follows will result in the service being started:

```
   "service_configs":[
       {"id":"gridappsd-dynamic-ybus-service"}
    ]
```

When the simulation is started and Dynamic Y-bus service is selected to run, the service will immediately request (without any application interaction) an initial or starting Y-bus from the Static Y-bus service that Dynamic Y-bus itself maintains internally (updating during the course of the simulation) for request/response and subscribe/publish messaging with any applications using the service. Because Static Y-bus may take 15 to 45+ seconds to construct this Y-bus if there have been no previous requests (meaning Static Y-bus does not have a cached matrix to immediately return) for the specified feeder id, it can also take this long for the Dynamic Y-bus service to return a starting Y-bus from an initial application request. If there have been either previous Dynamic Y-bus or Static Y-bus requests for the given feeder id for the GridAPPS-D platform instance, the initial response with a starting Y-bus will happen right away.

An example Dynamic Y-bus request/response and subscribe/publish workflow is provided in the gridappsd-toolbox GitHub repo in dynamic-ybus/test_dybus.py. The following documentation describes the usage of the Dynamic Y-bus service adhering to the recommended workflow insuring an up-to-date Y-bus is maintained.

## Service Output Request and Subscribe

The following code snippet shows the topic and request format for returning the starting Dynamic Y-bus for a simulation as specified by the simulation id. To insure that the starting Y-bus is not out of date, the recommended workflow is to subscribe to Y-bus updates prior to making the request as shown:

```
from gridappds import GridAPPSD
from gridappsd.topics import service_output_topic

class DYbusTester:
  def __init__(self, gapps, simulation_id):
    self.Ybus = {}
    self.YbusPreInit = {}
    self.timestampPreInit = 0
    self.ybusInitFlag = False
    self.keepLoopingFlag = True

    gapps.subscribe(service_output_topic('gridappsd-dynamic-ybus', simulation_id), self)

    topic = 'goss.gridappsd.request.data.dynamic-ybus.' + simulation_id
    request = {
      "requestType": "GET_SNAPSHOT_YBUS"
    }
    message = gapps.get_response(topic, request, timeout=90)
```

## Service Output Response and Published Update Messages

The Dynamic Y-bus response message, with the ybus element being shortened from the full value, is as follows:

```
message = {
  "feeder_id": "_5B816B93-7A5F-B64C-8460-47C17D6E4B0F",
  "simulation_id": "12345678",
  "timestamp": 1645840953,
  "ybus": {"646.2": {"645.2": [-7.177543473834806, 6.6777250702760815], "646.2": [7.177543473834806, -6.6777250702760815], "645.3": [2.408564072415804, -0.9996874529053109]}, "645.2": {"645.2": [11.468753101398052, -10.603388331034477]}, "645.3": {"645.2": [-3.8413662321995776, 1.5680292873844999], "645.3": [11.547403560992565, -10.5543166996104]},...}
}
```

The published Y-bus update message, with the ybus element being shortened from the full value, is as follows:

```
message = {
  "feeder_id": "_5B816B93-7A5F-B64C-8460-47C17D6E4B0F",
  "simulation_id": "12345678",
  "timestamp": 1645840956,
  "ybus": {"646.2": {"645.2": [-7.177543473834806, 6.6777250702760815], "646.2": [7.177543473834806, -6.6777250702760815], "645.3": [2.408564072415804, -0.9996874529053109]}, "645.2": {"645.2": [11.468753101398052, -10.603388331034477]}, "645.3": {"645.2": [-3.8413662321995776, 1.5680292873844999], "645.3": [11.547403560992565, -10.5543166996104]},...}
  "ybusChanges": {"646.2": {"645.2": [-7.177543473834806, 6.6777250702760815], "646.2": [7.177543473834806, -6.6777250702760815], "645.3": [2.408564072415804, -0.9996874529053109]}}
}
```

Note the update message contains both ybus and ybusChanges elements.  Normally applications only need to process one or the other and the workflow described below uses ybusChanges only once the starting Y-bus has been initialized. Using ybusChanges speeds processing for Dynamic Y-bus update messages notably for large models and also allows applications to be optimized by only considering changed Y-bus elements.

## Service Output Response and Published Update Processing

After making the request via the get_response call and still within the \_\_init__ function for the class, the following code insures the starting Y-bus is the most recent available:

```
    if self.timestampPreInit > message['timestamp']:
      self.Ybus = self.fullComplexInit(self.YbusPreInit)
      self.YbusPreInit = {} # free up memory no longer needed
    else:
      self.Ybus = self.fullComplexInit(message['ybus'])

    self.ybusInitFlag = True

    # do any processing after Ybus is initalized here
```

The ybus element in the messages and ybusChanges in the update message directly maps to a Python dictionary of dimension 2 or a dictionary of a dictionary. The numeric values are the real and imaginary components of the Y-bus admittance for each sparse matrix entry. Note that GridAPPS-D serializes messages using the JSON data interchange format, but unfortunately complex values are not directly supported by JSON. Therefore, each complex value is instead serialized as a two-element tuple. Further, only unique Y-bus entries are included in the messages that were determined by the Dynamic Y-bus service to be part of the lower diagonal portion of the sparse matrix. Therefore, most applications will need to both convert the tuples to complex numbers as well as populate the symmetric upper diagonal Y-bus entries to simplify working with the matrix in application code. The fullComplexInit function converts the lower diagonal sparse Y-bus into a full symmetric Y-bus matrix along with creating complex values from the JSON serialized tuple elements:

```
  def fullComplexInit(self, lowerUncomplex):
    YbusInit = {}

    for noderow in lowerUncomplex:
      for nodecol,value in lowerUncomplex[noderow].items():
        if noderow not in YbusInit:
          YbusInit[noderow] = {}
        if nodecol not in YbusInit:
          YbusInit[nodecol] = {}
        YbusInit[noderow][nodecol] = YbusInit[nodecol][noderow] = complex(value[0], value[1])

    return YbusInit
```

The on_message function of the class receives the published Dynamic Y-bus updates. The three class variables with "Init" in the names that are declared in the \_\_init__ function are used in on_message to be able to determine if the Y-bus returned by the get_response call is indeed the most recent version. Dynamic Y-bus, as a help to applications, also publishes as "processStatus" message that denotes when the underlying simulation has finished and there will be no further Dynamic Y-bus messages for that simulation. The keepLooping function can be called as a while loop conditional loop outside the class to exit message processing. The on_message and keepLooping functions are:

```
  def on_message(self, header, message):
    if 'processStatus' in message:
      if message['processStatus']=='COMPLETE' or message['processStatus']=='CLOSED':
        self.keepLoopingFlag = False

    elif not self.ybusInitFlag:
      self.YbusPreInit = message['ybus']
      self.timestampPreInit = message['timestamp']

    else:
      self.fullComplexUpdate(message['ybusChanges'])

      # do any processing after Ybus is updated here

  def keepLooping(self):
    return self.keepLoopingFlag
```

The elif block in on_message handles any Dynamic Y-bus published messages before the starting Y-bus has been initialized while the else block handles all the published messages after Y-bus initialization. The fullComplexUpdate function is similar to fullComplexInit except it specifically processes just changes to the existing Y-bus where changes only update existing entries rather than creating new entries:

```
  def fullComplexUpdate(self, lowerUncomplex):
    for noderow in lowerUncomplex:
      for nodecol,value in lowerUncomplex[noderow].items():
        self.Ybus[noderow][nodecol] = self.Ybus[nodecol][noderow] = complex(value[0], value[1])
```

See the test_dybus.py script in the dynamic-ybus directory of the gridappsd-toolbox GitHub repo for the complete workflow example for the Dynamic Y-bus service.

