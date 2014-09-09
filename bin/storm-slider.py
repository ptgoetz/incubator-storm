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

if  SLIDER_DIR == None or (not os.path.exists(SLIDER_DIR)):
    print "Unable to find SLIDER_HOME. Please configure SLIDER_HOME before running storm-slider"
    sys.exit(1)

if not os.path.exists("/usr/bin/storm"):
    print "Faled find storm cmd. Please storm package"
    sys.exit(1)

SLIDER_CLIENT_CONF = SLIDER_DIR + "/conf/slider-client.xml"
STORM_TEMP_JSON_FILE = "/tmp/storm.json"
STORM_CONF_DIR = expanduser("~")+"/.storm"
STORM_CONF_FILE = STORM_CONF_DIR + "/storm.yaml"
SLIDER_REGISTRY_CMD = SLIDER_DIR+"/bin/slider"


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
        print "Unable to read slider deployed storm config"
        sys.exit(1)

def json_to_yaml():
    if not os.path.exists(STORM_CONF_DIR):
        os.makedirs(STORM_CONF_DIR)
    file = open(STORM_TEMP_JSON_FILE,"r")
    data = json.load(file)
    f = open(STORM_CONF_FILE,"w")
    f.write("nimbus.host: "+data["nimbus.host"]+"\n")
    f.write("nimbus.thrift.port: "+data["nimbus.thrift.port"]+"\n")

def main():
    if len(sys.argv) <= 1:
        os.execvp("storm" , ["help"])
        sys.exit(-1)

    get_storm_config_json(sys.argv[1])
    json_to_yaml()
    storm_args = sys.argv[2:]
    storm_args.insert(0, "storm")
    if len(storm_args) < 1:
        os.execvp("storm" , ["help"])
        sys.exit(-1)

    os.execvp("storm",storm_args)

if __name__ == "__main__":
    main()
