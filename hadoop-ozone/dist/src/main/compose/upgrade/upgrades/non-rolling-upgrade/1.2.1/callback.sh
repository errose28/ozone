#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

source "$TEST_DIR"/testlib.sh


with_old_version() {
  # 1.2.0 was the first version with the upgrade framework where we can check
  # finalization status.
  execute_robot_test scm --include finalized upgrade/check-finalization
  .robot
}

with_new_version_pre_finalized() {
  execute_robot_test scm --include pre-finalized-ec-tests ec/upgrade-ec-check.robot
}

with_old_version_downgraded() {
  execute_robot_test scm --include finalized upgrade/check-finalization
  .robot
}

with_new_version_finalized() {
  execute_robot_test scm --include post-finalized-ec-tests ec/upgrade-ec-check.robot
}
