#!/usr/bin/env bash
# ------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# ------------------------------------------------------------

BENCHMARK_PATH=../benchmarks/tpch
SYSTEM_NAME=Standard

ansible-playbook -i ",localhost" run_local_query.yml --extra-vars="query_file=$BENCHMARK_PATH/setup/create.sqlpp report=false"
ansible-playbook -i ",localhost" load/load.yml --extra-vars="query_file=$BENCHMARK_PATH/setup/create.sqlpp report=false"

for f in `ls /tmp/asterixdb/dmls/*.sqlpp`; do
    ansible-playbook -i ",localhost" run_local_query.yml --extra-vars="query_file=$f report=false"
done

for number in 1 2 3
do
    for query in $BENCHMARK_PATH/queries/*.sqlpp; do
       echo $query
       ansible-playbook -i ",localhost" --extra-vars="query_file=$query report=true metric=$SYSTEM_NAME" run_local_query.yml
#       break;
    done
done