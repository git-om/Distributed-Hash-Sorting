#!/bin/bash
# CS553 HW5 - Complete Setup Script for Chameleon Cloud
# Run this script on your main Chameleon bare metal node

set -e

echo "=== CS553 HW5 Setup Script ==="
echo "This script will set up everything needed for the assignment"

# Update system
echo "Step 1: Updating system packages..."
sudo apt-get update
sudo apt-get upgrade -y

# Install essential tools
echo "Step 2: Installing essential tools..."
sudo apt-get install -y \
    openjdk-11-jdk \
    maven \
    python3 \
    python3-pip \
    git \
    wget \
    ssh \
    pdsh \
    vim \
    htop \
    sysstat \
    iotop

# Install virtualization tools (LXD)
echo "Step 3: Installing LXD for virtualization..."
sudo snap install lxd
sudo lxd init --auto

# Set JAVA_HOME
echo "Step 4: Setting up Java environment..."
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> ~/.bashrc

# Download and install Hadoop
echo "Step 5: Downloading and installing Hadoop 3.3.6..."
cd ~
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
mv hadoop-3.3.6 hadoop

# Set Hadoop environment variables
export HADOOP_HOME=~/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
echo "export HADOOP_HOME=~/hadoop" >> ~/.bashrc
echo "export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop" >> ~/.bashrc
echo "export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin" >> ~/.bashrc

# Download and install Spark
echo "Step 6: Downloading and installing Spark 3.5.0..."
cd ~
wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz
mv spark-3.5.0-bin-hadoop3 spark

# Set Spark environment variables
export SPARK_HOME=~/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
echo "export SPARK_HOME=~/spark" >> ~/.bashrc
echo "export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin" >> ~/.bashrc

# Install Python dependencies
echo "Step 7: Installing Python dependencies..."
pip3 install blake3 pyspark

# Create project directory
echo "Step 8: Creating project directory..."
mkdir -p ~/hw5
cd ~/hw5

# Setup SSH for passwordless login
echo "Step 9: Setting up SSH..."
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

echo "=== Setup Complete ==="
echo "Please run 'source ~/.bashrc' to load environment variables"
echo "Next steps:"
echo "1. Create and configure your VMs using LXD"
echo "2. Configure Hadoop and Spark for your cluster"
echo "3. Run the provided Java/Python code"