#!/bin/bash

#SIMREQ={\"power_system_config\":{\"Line_name\":\"_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62\"},\"service_configs\":[{\"id\":\"state-estimator\",\"user_options\":{\"use-sensors-for-estimates\":false}}]} # ieee13nodeckt using simulation

SIMREQ={\"power_system_config\":{\"Line_name\":\"_5B816B93-7A5F-B64C-8460-47C17D6E4B0F\"},\"service_configs\":[{\"id\":\"state-estimator\",\"user_options\":{\"use-sensors-for-estimates\":false}}]} # ieee13nodecktassets using simulation

#SIMREQ={\"power_system_config\":{\"Line_name\":\"_C1C3E687-6FFD-C753-582B-632A27E28507\"},\"service_configs\":[{\"id\":\"state-estimator\",\"user_options\":{\"use-sensors-for-estimates\":false}}]} # ieee123 using simulation

#SIMREQ={\"power_system_config\":{\"Line_name\":\"_AAE94E4A-2465-6F5E-37B1-3E72183A4E44\"},\"service_configs\":[{\"id\":\"state-estimator\",\"user_options\":{\"use-sensors-for-estimates\":false}}]} # test9500new using simulation

if [[ -z "$SIMREQ" ]]; then
    # requires at least a reference to the type of simulation to use
    if [ "$#" -eq 0 ]; then
        echo "Usage: ./run-dybus_viz.sh #nodes"
        echo
        exit
    fi

    read -d "\n" SIMID SIMREQ <<< $(../sim_starter/sim_starter.py $1)
else
#   invocation when simulation is already started from platform viz
    SIMID=$1
fi

python3 dynamic_ybus.py $SIMID "$SIMREQ" 2>&1 | tee dybus.log

