#!/bin/bash
file_path=$1
zstd -t "$file_path" --memory=2048MB
