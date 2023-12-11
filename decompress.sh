#!/bin/bash
file_path=$1
zstd -d --rm "$file_path" --memory=2048MB
