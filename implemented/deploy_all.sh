#!/bin/bash
# Complete deployment script for CS553 HW5
# This script automates the entire setup process

set -e

echo "========================================"
echo "CS553 HW5 - Complete Deployment Script"
echo "========================================"

# Configuration
HADOOP_VERSION="3.3.6"
SPARK_VERSION="3.5.0"
JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Step 1: Install prerequisites
install_prerequisites() {
    log_info "Step 1: Installing prerequisites..."
    
    sudo apt-get update
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
        iotop \
        bc
    
    # Install Python packages
    pip3 install blake3 pyspark psutil pandas matplotlib
    
    log_info "Prerequisites installed successfully"
}

# Step 2: Install LXD
install_lxd() {
    log_info "Step 2: Installing and configuring LXD..."
    
    if ! command_exists lxc; then
        sudo snap install lxd
        sudo lxd init --auto
        sudo usermod -a -G lxd $USER
        log_warn "You may need to log out and back in for LXD permissions"
    else
        log_info "LXD already installed"
    fi
}

# Step 3: Download and setup Hadoop
setup_hadoop() {
    log_info "Step 3: Setting up Hadoop ${HADOOP_VERSION}..."
    
    cd ~
    if [ ! -d "hadoop" ]; then
        wget -q https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
        tar -xzf hadoop-${HADOOP_VERSION}.tar.gz
        mv hadoop-${HADOOP_VERSION} hadoop
        rm hadoop-${HADOOP_VERSION}.tar.gz
    fi
    
    # Set environment variables
    if ! grep -q "HADOOP_HOME" ~/.bashrc; then
        cat >> ~/.bashrc <<EOF

# Hadoop Environment
export HADOOP_HOME=~/hadoop
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin
EOF
    fi
    
    export HADOOP_HOME=~/hadoop
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
    export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
    
    log_info "Hadoop setup complete"
}

# Step 4: Download and setup Spark
setup_spark() {
    log_info "Step 4: Setting up Spark ${SPARK_VERSION}..."
    
    cd ~
    if [ ! -d "spark" ]; then
        wget -q https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz
        tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz
        mv spark-${SPARK_VERSION}-bin-hadoop3 spark
        rm spark-${SPARK_VERSION}-bin-hadoop3.tgz
    fi
    
    # Set environment variables
    if ! grep -q "SPARK_HOME" ~/.bashrc; then
        cat >> ~/.bashrc <<EOF

# Spark Environment
export SPARK_HOME=~/spark
export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin
EOF
    fi
    
    export SPARK_HOME=~/spark
    export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
    
    log_info "Spark setup complete"
}

# Step 5: Setup SSH keys
setup_ssh() {
    log_info "Step 5: Setting up SSH..."
    
    if [ ! -f ~/.ssh/id_rsa ]; then
        ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
        cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
        chmod 0600 ~/.ssh/authorized_keys
    fi
    
    log_info "SSH setup complete"
}

# Step 6: Create VMs
create_vms() {
    log_info "Step 6: Creating VMs..."
    
    # Check if VMs already exist
    if lxc list | grep -q "namenode"; then
        log_warn "VMs already exist. Skipping creation."
        return
    fi
    
    # Create namenode (tiny instance)
    log_info "Creating namenode..."
    lxc launch ubuntu:24.04 namenode
    lxc config set namenode limits.cpu 4
    lxc config set namenode limits.memory 4GB
    
    # Create large instance
    log_info "Creating large1..."
    lxc launch ubuntu:24.04 large1
    lxc config set large1 limits.cpu 32
    lxc config set large1 limits.memory 32GB
    
    # Create 8 small instances
    for i in {1..8}; do
        log_info "Creating small$i..."
        lxc launch ubuntu:24.04 small$i
        lxc config set small$i limits.cpu 4
        lxc config set small$i limits.memory 4GB
    done
    
    log_info "Waiting for VMs to start..."
    sleep 30
    
    # Install dependencies on all VMs
    for vm in namenode large1 small{1..8}; do
        log_info "Installing dependencies on $vm..."
        lxc exec $vm -- apt-get update
        lxc exec $vm -- apt-get install -y openjdk-11-jdk ssh pdsh wget rsync
    done
    
    log_info "VMs created successfully"
}

# Step 7: Configure cluster
configure_cluster() {
    log_info "Step 7: Configuring cluster..."
    
    # Copy Hadoop and Spark to all nodes
    for vm in namenode large1 small{1..8}; do
        log_info "Configuring $vm..."
        
        # Copy Hadoop
        lxc file push -r ~/hadoop $vm/home/ubuntu/
        
        # Copy Spark
        lxc file push -r ~/spark $vm/home/ubuntu/
        
        # Setup environment
        lxc exec $vm -- bash -c "echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> /home/ubuntu/.bashrc"
        lxc exec $vm -- bash -c "echo 'export HADOOP_HOME=/home/ubuntu/hadoop' >> /home/ubuntu/.bashrc"
        lxc exec $vm -- bash -c "echo 'export SPARK_HOME=/home/ubuntu/spark' >> /home/ubuntu/.bashrc"
    done
    
    # Setup Hadoop configuration
    log_info "Copying Hadoop configuration files..."
    for vm in namenode large1 small{1..8}; do
        lxc file push core-site.xml $vm/home/ubuntu/hadoop/etc/hadoop/
        lxc file push hdfs-site.xml $vm/home/ubuntu/hadoop/etc/hadoop/
        lxc file push yarn-site.xml $vm/home/ubuntu/hadoop/etc/hadoop/
        lxc file push mapred-site.xml $vm/home/ubuntu/hadoop/etc/hadoop/
    done
    
    # Setup workers file
    cat > /tmp/workers <<EOF
large1
small1
small2
small3
small4
small5
small6
small7
small8
EOF
    lxc file push /tmp/workers namenode/home/ubuntu/hadoop/etc/hadoop/workers
    
    # Setup Spark configuration
    log_info "Copying Spark configuration files..."
    for vm in namenode large1 small{1..8}; do
        lxc file push spark-env.sh $vm/home/ubuntu/spark/conf/
    done
    
    log_info "Cluster configured successfully"
}

# Step 8: Start cluster
start_cluster() {
    log_info "Step 8: Starting Hadoop and Spark..."
    
    # Format namenode (only if not already formatted)
    log_info "Formatting HDFS namenode..."
    lxc exec namenode -- sudo -u ubuntu /home/ubuntu/hadoop/bin/hdfs namenode -format -force
    
    # Start HDFS
    log_info "Starting HDFS..."
    lxc exec namenode -- sudo -u ubuntu /home/ubuntu/hadoop/sbin/start-dfs.sh
    
    # Start YARN
    log_info "Starting YARN..."
    lxc exec namenode -- sudo -u ubuntu /home/ubuntu/hadoop/sbin/start-yarn.sh
    
    # Wait for services to start
    sleep 20
    
    # Start Spark
    log_info "Starting Spark..."
    lxc exec namenode -- sudo -u ubuntu /home/ubuntu/spark/sbin/start-all.sh
    
    sleep 10
    
    # Verify cluster
    log_info "Verifying Hadoop cluster..."
    lxc exec namenode -- /home/ubuntu/hadoop/bin/hdfs dfsadmin -report
    
    log_info "Cluster started successfully"
}

# Step 9: Build and deploy applications
build_deploy() {
    log_info "Step 9: Building and deploying applications..."
    
    # Build with Maven
    log_info "Building Java applications..."
    cd ~/hw5
    mvn clean package
    
    # Deploy JAR to namenode
    log_info "Deploying JAR to cluster..."
    lxc file push target/hw5-vault-1.0.jar namenode/home/ubuntu/
    
    # Deploy Python scripts
    log_info "Deploying Python scripts..."
    lxc file push hashgen.py large1/home/ubuntu/
    lxc file push hashgen.py small1/home/ubuntu/
    lxc file push monitor_resources.py large1/home/ubuntu/
    lxc file push monitor_resources.py namenode/home/ubuntu/
    
    log_info "Applications deployed successfully"
}

# Step 10: Print summary
print_summary() {
    echo ""
    echo "========================================"
    log_info "Deployment Complete!"
    echo "========================================"
    echo ""
    echo "Next steps:"
    echo "1. Source environment: source ~/.bashrc"
    echo "2. Check cluster status:"
    echo "   lxc exec namenode -- /home/ubuntu/hadoop/bin/hdfs dfsadmin -report"
    echo "3. Run experiments:"
    echo "   ./run_experiments.sh"
    echo ""
    echo "VM IPs:"
    for vm in namenode large1 small{1..8}; do
        IP=$(lxc list $vm -c 4 | grep eth0 | awk '{print $1}')
        echo "  $vm: $IP"
    done
    echo ""
    echo "Web UIs:"
    NAMENODE_IP=$(lxc list namenode -c 4 | grep eth0 | awk '{print $1}')
    echo "  HDFS NameNode: http://$NAMENODE_IP:9870"
    echo "  YARN ResourceManager: http://$NAMENODE_IP:8088"
    echo "  Spark Master: http://$NAMENODE_IP:8080"
    echo ""
}

# Main execution
main() {
    log_info "Starting deployment..."
    
    install_prerequisites
    install_lxd
    setup_hadoop
    setup_spark
    setup_ssh
    create_vms
    configure_cluster
    start_cluster
    build_deploy
    print_summary
    
    log_info "All done!"
}

# Run main function
main