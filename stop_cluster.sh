#!/usr/bin/env bash

script_dir="$(cd "$(dirname "$0")" && pwd)"
source "${script_dir}/nodes.sh"

echo "Stopping Storage Nodes..."
for node in ${nodes[@]}; do
    echo "${node}"
    ssh "${node}" "pkill -u "$(whoami)" storage_node"
done

echo "Stopping Controller..."
ssh "${controller}" 'pkill -u "$(whoami)" controller'
echo "${controller}"

echo "Done!"
