#!/bin/bash -ex
#set auto snapshot to false
for node in `./ec2-ips scylla-db`;
do
    ./ec2-ssh $node sudo sed -i "s/auto_snapshot.*//g" /etc/scylla/scylla.yaml
    ./ec2-ssh $node sudo sed -i "/^$/d" /etc/scylla/scylla.yaml
    ./ec2-ssh $node "bash -c \"echo 'auto_snapshot: false' | sudo tee -a /etc/scylla/scylla.yaml\""
    ./ec2-ssh $node cat /etc/scylla/scylla.yaml
    ./ec2-ssh $node sudo systemctl restart scylla-server
done
