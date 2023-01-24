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

with_new_version_pre_finalized() {
  _check_hdds_mlvs 2
  _check_om_mlvs 0
  check_ec_is_disabled
}

with_old_version_downgraded() {
  _check_hdds_mlvs 2
  _check_om_mlvs 0
}

with_new_version_finalized() {
  _check_hdds_mlvs 4
  _check_om_mlvs 3
  check_ec_is_enabled
}
