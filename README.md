# GridAPPS-D App Toolbox

The concept of the Application Toolbox is a direct pipeline from GridAPPS-D stakeholder input to the creation of new software capability that streamlines building new applications and makes them more reliable and robust. 

This capability will be in the form of APIs, publish/subscribe services, and utility applications. There will be regular meetings to solicit feedback from stakeholders that will prioritize tool development, with those tools benefitting multiple applications taking precedence. 

During development, an application developer will serve as tool champion to help with determining requirements and evaluating the resulting capability, with both working group meetings and tool champions ensuring the direct pipeline to stakeholders.

App Toolbox capabilities currently in development:

* __Topology Processor__ -- Process network topology updates in real time, keep track of what DERs are connected to which substation

* __Dynamic Y-Bus Matrix__ -- Provides a real-time Y-bus when queried, updates Y-bus with current switch and tap positions 


App Toolbox capability currently under consideration:

* __Measurement mRID Manager__ -- Configure sensors by removing specific measurement mRIDs from simulation output (different from dropping measurements via sensor service)

* __Snapshot Power Flow Service__ -- Send substation voltage and load values, receive snapshot power flow for current state of the network

* __Snapshot State Estimation Service__ -- Generate a single state estimation run for the current state of the network

