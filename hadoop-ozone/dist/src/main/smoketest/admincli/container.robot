# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

*** Settings ***
Documentation       Test ozone admin container command
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes
Suite Setup         Create test data

*** Variables ***
${CONTAINER}
${SCM}       scm

*** Keywords ***
Create test data
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab
                        Execute          ozone freon ockg -n1 -t1 -p container

Container is closed
    [arguments]     ${container}
    ${output} =         Execute          ozone admin container info "${container}"
                        Should contain   ${output}   CLOSED

*** Test Cases ***
Create container
    ${output} =         Execute          ozone admin container create
                        Should contain   ${output}   is created
    ${container} =      Execute          echo "${output}" | grep 'is created' | cut -f2 -d' '
                        Set Suite Variable    ${CONTAINER}    ${container}

List containers
    ${output} =         Execute          ozone admin container list
                        Should contain   ${output}   OPEN
                        Should Start With   ${output}   [
                        Should End With   ${output}   ]

List containers with explicit host
    ${output} =         Execute          ozone admin container list --scm ${SCM}
                        Should contain   ${output}   OPEN
                        Should Start With   ${output}   [
                        Should End With   ${output}   ]

List containers with container state
    ${output} =         Execute          ozone admin container list --state=CLOSED
                        Should Not contain   ${output}   OPEN
                        Should Start With   ${output}   [
                        Should End With   ${output}   ]

List containers with replication factor ONE
    ${output} =         Execute          ozone admin container list -t RATIS -r ONE
                        Should Not contain   ${output}   THREE
                        Should Start With   ${output}   [
                        Should End With   ${output}   ]

List containers with replication factor THREE
    ${output} =         Execute          ozone admin container list -t RATIS -r THREE
                        Should Not contain   ${output}   ONE
                        Should Start With   ${output}   [
                        Should End With   ${output}   ]

Container info
    ${output} =         Execute          ozone admin container info "${CONTAINER}"
                        Should contain   ${output}   Container id: ${CONTAINER}
                        Should contain   ${output}   Pipeline id
                        Should contain   ${output}   Datanodes

Verbose container info
    ${output} =         Execute          ozone admin --verbose container info "${CONTAINER}"
                        Should contain   ${output}   Pipeline Info

List containers as JSON
    ${output} =         Execute          ozone admin container info "${CONTAINER}" --json | jq -r '.'
                        Should contain   ${output}    containerInfo
                        Should contain   ${output}    pipeline
                        Should contain   ${output}    replicas
                        Should contain   ${output}    writePipelineID

Report containers as JSON
     ${output} =         Execute          ozone admin container report --json | jq -r '.'
                         Should contain   ${output}   reportTimeStamp
                         Should contain   ${output}   stats
                         Should contain   ${output}   samples

List all containers
    ${output} =         Execute          ozone admin container list --all
                        Should contain   ${output}   OPEN
                        Should Start With   ${output}   [
                        Should End With   ${output}   ]

List all containers according to count (batchSize)
    ${output} =         Execute          ozone admin container list --all --count 10
                        Should contain   ${output}   OPEN
                        Should Start With   ${output}   [
                        Should End With   ${output}   ]

List all containers from a particular container ID
    ${output} =         Execute          ozone admin container list --all --start 2
                        Should contain   ${output}   OPEN
                        Should Start With   ${output}   [
                        Should End With   ${output}   ]

Check JSON array parsing
    ${output} =         Execute          ozone admin container list
                        Should Start With   ${output}   [
                        Should Contain   ${output}   containerID
                        Should End With   ${output}   ]
    ${containerIDs} =   Execute          echo '${output}' | jq -r '.[].containerID'
                        Should Not Be Empty   ${containerIDs}

Check state filtering with JSON array format
    ${output} =         Execute          ozone admin container list --state=OPEN
                        Should Start With   ${output}   [
                        Should End With   ${output}   ]
    ${states} =         Execute          echo '${output}' | jq -r '.[].state'
                        Should Contain   ${states}   OPEN
                        Should Not Contain   ${states}   CLOSED

Check count limit with JSON array format
    ${output} =         Execute          ozone admin container create
                        Should contain   ${output}   is created
    ${output} =         Execute          ozone admin container create
                        Should contain   ${output}   is created
    ${output} =         Execute          ozone admin container create
                        Should contain   ${output}   is created
    ${output} =         Execute          ozone admin container create
                        Should contain   ${output}   is created
    ${output} =         Execute          ozone admin container create
                        Should contain   ${output}   is created
    ${output} =         Execute And Ignore Error          ozone admin container list --count 5 2> /dev/null # This logs to error that the list is incomplete
    ${count} =          Execute          echo '${output}' | jq -r 'length'
                        Should Be True   ${count} == 5

Close container
    ${container} =      Execute          ozone admin container list --state OPEN | jq -r '.[] | select(.replicationConfig.replicationFactor == "ONE") | .containerID' | head -1
                        Execute          ozone admin container close "${container}"
    ${output} =         Execute          ozone admin container info "${container}"
                        Should contain   ${output}   CLOS
    Wait until keyword succeeds    1min    10sec    Container is closed    ${container}

Incomplete command
    ${output} =         Execute And Ignore Error     ozone admin container
                        Should contain   ${output}   Missing required subcommand
                        Should contain   ${output}   list
                        Should contain   ${output}   info
                        Should contain   ${output}   create
                        Should contain   ${output}   close
                        Should contain   ${output}   report

#List containers on unknown host
#    ${output} =         Execute And Ignore Error     ozone admin --verbose container list --scm unknown-host
#                        Should contain   ${output}   Invalid host name

Cannot close container without admin privilege
    Requires admin privilege    ozone admin container close "${CONTAINER}"

Cannot create container without admin privilege
    Requires admin privilege    ozone admin container create

Reset user
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab
