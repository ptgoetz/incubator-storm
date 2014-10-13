#!/usr/bin/python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
from os.path import expanduser
import sys
import json


SLIDER_DIR = os.getenv('SLIDER_HOME', None)
STORM_HOME = os.getenv('STORM_HOME', None)

if  STORM_HOME == None or (not os.path.exists(STORM_HOME)):
    print "Unable to find STORM_HOME. Please configure STORM_HOME before running storm-slider"
    sys.exit(1)

if  SLIDER_DIR == None or (not os.path.exists(SLIDER_DIR)):
    print "Unable to find SLIDER_HOME. Please configure SLIDER_HOME before running storm-slider"
    sys.exit(1)

if not os.path.exists(STORM_HOME+"/bin/storm"):
    print "Faled find storm cmd. Please install storm package"
    sys.exit(1)

SLIDER_CLIENT_CONF = SLIDER_DIR + "/conf/slider-client.xml"
STORM_TEMP_JSON_FILE = "/tmp/storm.json"
STORM_CONF_DIR = expanduser("~")+"/.storm"
STORM_CONF_FILE = STORM_CONF_DIR + "/storm.yaml"
SLIDER_REGISTRY_CMD = SLIDER_DIR+"/bin/slider"
STORM_CMD = STORM_HOME+"/bin/storm"


def get_storm_config_json(appname):
    all_args = [
        "slider",
        "registry",
        "--getconf storm-site",
        "--name "+appname,
        "--format json",
        "--dest "+STORM_TEMP_JSON_FILE]
    os.spawnvp(os.P_WAIT,SLIDER_REGISTRY_CMD, all_args)
    if not os.path.exists(STORM_TEMP_JSON_FILE):
        print "Failed to read slider deployed storm config"
        sys.exit(1)

def storm_cmd_args(args):
    file = open(STORM_TEMP_JSON_FILE,"r")
    data = json.load(file)
    args.insert(0, "storm")
    args.extend(["-c nimbus.host="+data["nimbus.host"],
                 "-c nimbus.thrift.port="+data["nimbus.thrift.port"]])
    os.remove(STORM_TEMP_JSON_FILE)
    return args

def main():
    if len(sys.argv) < 2:
        print "Please provide yarn appName followed by storm command."
        os.execvp("storm" , ["help"])
        sys.exit(-1)

    get_storm_config_json(sys.argv[1])
    storm_args = storm_cmd_args(sys.argv[2:])
    os.execvp(STORM_CMD,storm_args)

if __name__ == "__main__":
    main()
