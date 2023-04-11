#chmod +x startup.sh
#!/usr/bin/env bash

port_prefix=24  # Assigned port prefix
count=12        # Number of nodes to run

script_dir="$(cd "$(dirname "$0")" && pwd)"
log_dir="${script_dir}/logs"
source "${script_dir}/nodes.sh"

# Create log directory
echo "Creating log directory: ${log_dir}"
mkdir -pv "${log_dir}"

# Start controller
echo "Starting controller on ${controller}: listening on port ${cport}"
ssh "${controller}" "${HOME}/go/bin/controller ${cport}" &> "${log_dir}/controller.log" &

sleep 1

# Start storage nodes
for (( i = 0; i < count; i++ )); do
    port=$(( port_prefix * 1005 + (i + 1) ))
    node=$(( i % ${#nodes[@]} ))

    # ssh to the orion machine and run 'storage_node controller:cport' in the background
    echo "Starting node on ${nodes[${node}]}: listening on port ${port}"
    ssh ${nodes[${node}]} "${HOME}/go/bin/storage_node ${controller}:${cport} ${port}" &> "${log_dir}/${nodes[${node}]}.log" &
done

echo "Startup complete"
