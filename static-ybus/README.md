# GridAPPS-D Toolbox Static Y-bus

Static Y-bus is a lightweight service that returns Y-bus admittance matrices created from corresponding network equipment models to requesting applications. The term "static" refers to the matrix only being tied to the model and not to a simulation using the model. If an application is associated with a running simulation, the Dynamic Y-bus service should normally be used instead, unless there is some specialized need to only receive a Y-bus that does not reflect any of the ongoing changes such as to switch states and regulator tap positions from the simlation.

## Static Y-bus Overview

Static Y-bus operates solely through the GridAPPS-D request/response messaging pattern. Applications request a Y-bus admittance matrix for a specified model, which is then created and subsequently returned by the same GridAPPS-D API get_reponse call from with the request was initiated.

Static Y-bus is started automatically when the GridAPPS-D platform is launched and therefore is always available to applications connected to the platform. Internally Y-bus admittance matrices are cached so that only the first request for a given model within a GridAPPS-D platform instance will result in creating the matrix from the corresponding network equipment model. Subsequent requests from any application for that platform instance will immediately return the cached Y-bus matrix that was previously created.

Because of the complexity in deriving the Y-bus matrix directly from the network equipment model, even for simple models it takes 15 or more seconds for an application to receive the Y-bus response when it must be created for the initial request, longer for a large model. Caching of matrices therefore results in significantly better performance when there are repeated Static Y-bus requests. 

An example Static Y-bus request/response is provided in the gridappsd-toolbox GitHub repo in static-ybus/test_sybus.py. The following documentation describes this usage of the Static Y-bus service, both the request and response.

## Service Output Request

Request Y-bus matrix

## Service Output Response

Response Y-bus matrix format

