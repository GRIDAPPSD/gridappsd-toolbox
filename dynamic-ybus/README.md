# GridAPPS-D Toolbox Dynamic Y-bus

Dynamic Y-bus is a lightweight service that provides Y-bus complex system admittance sparse matrices represented in a Python dictionary of dimension 2, also referred to as a dictionary of a dictionary. Y-bus matrices are constructed from the underlying network equipment model using a method originally developed for the GridAPPS-D Model Validator application. Applications within an as-operated (e.g., field or simulation) context should use the Dynamic Y-bus service, while applications processing as-built models (e.g., outside of a field or simulation context) should use the related Static Y-bus service.  Dynamic Y-bus provides updates from changes in switch, voltage regular, and capacitor states over time.  The Dynamic Y-bus service supports "snapshot" request/response queries along with subscribe/publish messaging for Y-bus updates.

## Dynamic Y-bus Overview

Dynamic Y-bus operates through the GridAPPS-D request/response and subscribe/publish messaging patterns. The basic workflow is the request/response is first used by an application to initialize Y-bus and subsequent subscribe/publish messages are used to capture updates to the Y-bus over the course of simulation execution. A slight tweak to this workflow is recommended though, described below, that avoids the possibility while initializing Y-bus of a publish/subscribe message coming through with a more recent simulation timestamp than the request/response message.  While this is unlikely, it is possible and would potentially result in Y-bus being initialized with an out-of-date version. The tweak guarantees that Y-bus is initialized using the version with the most recent timestamp, whether that is from the request/response or subscribe/publish message.

Dynamic Y-bus is started with a simulation by the GridAPPS-D platform when it is configured through the Service Configuration tab for setting up a simulation through the platform viz web browser user interface. Select gridappsd-dynamic-ybus-service from the Available Services dropdown menu, hitting the Confirm button on the dropdown menu and then the Apply button on the Available Servies tab. Failing to hit the Apply button will result in Dynamic Y-bus not being started with the simulation. There are no user input options for Dynamic Y-bus beyond selecting it to be run. If a simulation is being invoked through a JSON config file rather than through the platform viz, specifying "gridappsd-dynamic-ybus-service" as the "id" value in the "service_configs" array will result in the service being started.

When the simulation is started and Dynamic Y-bus service is selected to run, the service will immediately request a starting Y-bus from the Static Y-bus service that Dynamic Y-bus itself maintains internally (updating during the course of the simulation) for request/response and subscribe/publish messaging with any applications using the service. Because Static Y-bus may take 15 to 45+ seconds to construct this Y-bus if there have been no previous requests (meaning Static Y-bus does not have a cached matrix to immediately return) for the specified feeder id, it can also take this long for the Dynamic Y-bus service to return a starting Y-bus from an initial application request. If there have been either previous Dynamic Y-bus or Static Y-bus requests for the given feeder id for the GridAPPS-D platform instance, the initial response with a starting Y-bus will happen right away.

An example Dynamic Y-bus request/response and subscribe/publish workflow is provided in the gridappsd-toolbox GitHub repo in dynamic-ybus/test_dybus.py. The following documentation describes the usage of the Dynamic Y-bus service adhering to the recommended workflow insuring an up-to-date Y-bus is always maintained within an application.

## Service Output Request

## Service Output Response

## Service Output Updates

