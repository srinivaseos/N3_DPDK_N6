#!/bin/bash

echo "Removing log files"

cd logs

#rm -rf *.log
#rm -rf app device l7 perf pfcp pkt
rm -rf /home/dpdk/srinivas/dpdk_practice/logs/*.log
#rm -rf /home/dpdk/xiusupf_ecserver/dplane/logs/app/*.log


echo "logs Removed"

cd ..
