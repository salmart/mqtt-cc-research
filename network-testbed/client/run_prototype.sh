#!/bin/bash
if [ $# -eq 0 ]; then
exit 1
fi
file_path="$1"
if [ ! -f "$file_path" ]; then
exit 1
else 
python3 proto_client.py
fi
python3 proto_client.py "$file_path" testbed 900 0.05 3
