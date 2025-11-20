#!/bin/bash

# Script to free up disk space on CI systems
# Source: https://github.com/apache/flink/blob/02d30ace69dc18555a5085eccf70ee884e73a16e/tools/azure-pipelines/free_disk_space.sh

echo "Freeing up disk space on CI system"

echo "Listing 100 largest packages"
dpkg-query -Wf '${Installed-Size}\t${Package}\n' | sort -n | tail -n 100
df -h

echo "Removing large packages"
sudo apt-get remove -y '^ghc-8.*'
sudo apt-get remove -y '^dotnet-.*'
sudo apt-get remove -y '^llvm-.*'
sudo apt-get remove -y 'php.*'
sudo apt-get remove -y azure-cli google-cloud-sdk hhvm google-chrome-stable firefox powershell mono-devel
sudo apt-get autoremove -y
sudo apt-get clean
df -h

echo "Removing large directories"
rm -rf /usr/share/dotnet/
df -h