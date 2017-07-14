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

#!/bin/bash

pushd `dirname $0` > /dev/null
SCRIPT_PATH=`pwd -P`
popd > /dev/null
export ANSIBLE_HOST_KEY_CHECKING=false

INVENTORY=$1

# Checks the existence of the inventory file.
if [ ! -f "$INVENTORY" ];
then
   echo "The inventory file \"$INVENTORY\" does not exist."
   exit 1
fi

# Configure HDFS
ansible-playbook -i $INVENTORY install_hdfs.yml
ansible-playbook -i $INVENTORY start_hdfs.yml
# Configure Sparks
ansible-playbook -i $INVENTORY install_sparks.yml
ansible-playbook -i $INVENTORY start_sparks.yml
# Generate data
ansible-playbook -i $INVENTORY gen_tpch.yml
ansible-playbook -i $INVENTORY load_tpch.yml
# Execute queries
ansible-playbook -i $INVENTORY prepare_queries.yml
ansible-playbook -i $INVENTORY execute_queries.yml