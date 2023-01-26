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
Documentation       Finalize Upgrade of the Ozone cluster
Resource            ../commonlib.robot
Test Timeout        10 minutes
Test Setup          Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab

*** Test Cases ***
Check Finalized
    [Tags]    finalized
    ${result} =        Execute      ozone admin om finalizationstatus
                       Log    ${result}
                       Should Contain Any    ${result}    ALREADY_FINALIZED    FINALIZATION_DONE


    ${result} =        Execute      ozone admin scm finalizationstatus
                       Log    ${result}
                       Should Contain Any    ${result}    ALREADY_FINALIZED    FINALIZATION_DONE

Check Pre Finalized
    [Tags]    pre-finalized
    ${result} =        Execute      ozone admin om finalizationstatus
                       Log    ${result}
                       Should Contain Any    ${result}    FINALIZATION_REQUIRED


    ${result} =        Execute      ozone admin scm finalizationstatus
                       Log    ${result}
                       Should Contain Any    ${result}    FINALIZATION_REQUIRED

