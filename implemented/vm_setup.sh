#!/bin/bash
# Create VMs using LXD

echo "Creating VMs for HW5..."

# Initialize LXD if not already done
sudo lxd init --auto

# Create tiny instance (name node)
echo "Creating tiny.instance (namenode)..."
lxc launch ubuntu:24.04 namenode
lxc config set namenode limits.cpu 4
lxc config set namenode limits.memory 4GB
lxc config device add namenode root disk path=/ pool=default size=10GB

# Create large instance
echo "Creating large.instance..."
lxc launch ubuntu:24.04 large1
lxc config set large1 limits.cpu 32
lxc config set large1 limits.memory 32GB
lxc config device add large1 root disk path=/ pool=default size=240GB

# Create 8 small instances
for i in {1..8}; do
    echo "Creating small.instance $i..."
    lxc launch ubuntu:24.04 small$i
    lxc config set small$i limits.cpu 4
    lxc config set small$i limits.memory 4GB
    lxc config device add small$i root disk path=/ pool=default size=30GB
done

# Wait for VMs to start
sleep 10

# Install Java and dependencies on all VMs
echo "Installing dependencies on all VMs..."
for vm in namenode large1 small{1..8}; do
    echo "Setting up $vm..."
    lxc exec $vm -- apt-get update
    lxc exec $vm -- apt-get install -y openjdk-11-jdk ssh pdsh wget
done

# Get VM IPs and create hosts file
echo "Getting VM IP addresses..."
echo "# HW5 Cluster" > ~/hw5/hosts.txt
for vm in namenode large1 small{1..8}; do
    IP=$(lxc list $vm -c 4 | grep eth0 | awk '{print $1}')
    echo "$IP $vm" >> ~/hw5/hosts.txt
    echo "$vm: $IP"
done

echo "VM creation complete!"
echo "VM IPs saved to ~/hw5/hosts.txt"