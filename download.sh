#!/bin/bash
index=$1
torrent_file=$2
aria2c --file-allocation=none --seed-time=0 --select-file="$index" "$torrent_file"
