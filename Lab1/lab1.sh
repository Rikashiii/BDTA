#!/bin/bash
# Author: Sumit Nagpure
# Created: 23-10-2025

# Script: lab1.sh
# Description: Automates common HDFS commands for a standard Hadoop lab.
# Tasks: Implements existence checks and uses variables to avoid hardcoding.

# Usage: ./lab1.sh

# Prerequisites
# NOTE: Assuming these files exist in the current directory for the purpose of the script.
# 1. local.txt 
# 2. small_blocks.txt

# --- Configuration Variables ---
null="/dev/null"
hdfs_dir="test"
hdfs_data_file="$hdfs_dir/data.txt"
hdfs_small_file="$hdfs_dir/small_blocks.txt"
output_file="merged.txt"

local_data_file="data.txt"
local_data2_file="data2.txt"
local_small_file="small_blocks.txt"

# --- Script Start ---
echo "====================================================="
echo "  STARTING HDFS AUTOMATION SCRIPT"
echo "====================================================="

# 1) Test the availability of a directory or file on HDFS.
echo " Checking for existing HDFS directory: $hdfs_dir"

# Check if the main directory ($hdfs_dir) exists. If it does, delete it and its contents.
if hdfs dfs -test -e $hdfs_dir; then
   hdfs dfs -rm -R $hdfs_dir > $null
fi

# Create the main working directory.
echo "Creating main HDFS directory: $hdfs_dir"
hdfs dfs -mkdir $hdfs_dir > $null

# Show the initial directory contents.
echo "HDFS Root Directory contents (should show only 'test'):"
hdfs dfs -ls $hdfs_dir

echo "-----------------------------------------------------"

# Create nested directories using single and -p (parent) commands.
echo "Creating nested directories inside $hdfs_dir"
hdfs dfs -mkdir $hdfs_dir
hdfs dfs -mkdir "$hdfs_dir/test1" 
hdfs dfs -mkdir -p "$hdfs_dir/test2/test3" 
echo "   -> Created test/test1 and test/test2/test3"

# Show directory contents after creation.
echo "Contents of $hdfs_dir after mkdirs:"
hdfs dfs -ls -R $hdfs_dir
echo "-----------------------------------------------------"

# Delete a nested directory (test/test2) recursively.
echo "Deleting directory $hdfs_dir/test2 recursively."
# Use the -R flag to delete the non-empty directory /test2 and everything inside.
hdfs dfs -rm -R $hdfs_dir/test2 > $null
echo "   -> Deleted $hdfs_dir/test2"

# Show recursive contents of the main directory.
echo "Recursive contents of $hdfs_dir after deletion:"
hdfs dfs -ls -R $hdfs_dir


echo "-----------------------------------------------------"

# Upload data.txt to the HDFS test directory.
echo " Uploading $local_data_file to $hdfs_data_file"
hdfs dfs -put $local_data_file $hdfs_data_file > $null

# List the contents of the main directory after the put operation.
echo "Contents of $hdfs_dir after uploading data.txt:"
hdfs dfs -ls -R $hdfs_dir

echo "-----------------------------------------------------"

# Copy the uploaded file to a new file within a subdirectory.
echo "Copying $hdfs_data_file to $hdfs_dir/test1/data.txt"
# $hdfs_data_file resolves to test/local_data_file
hdfs dfs -cp $hdfs_data_file "$hdfs_dir/test1/data.txt" > $null
echo "   -> File copied to $hdfs_dir/test1/data.txt"

# Delete the copied file.
echo "Deleting the copied file /test/test1/data.txt"
hdfs dfs -rm "$hdfs_dir/test1/data.txt" > $null

echo "-----------------------------------------------------"

#Display the entire contents of the original uploaded file.
echo "Displaying full contents of $hdfs_data_file /(hdfs dfs -cat/):"
# hdfs dfs -cat $hdfs_data_file

echo "-----------------------------------------------------"

# Display the last 1KB of the original uploaded file.
echo "Displaying last few lines of $hdfs_data_file (hdfs dfs -tail):"
hdfs dfs -tail $hdfs_data_file

echo "-----------------------------------------------------"
if [ -e "$local_data2_file" ]; then
   rm -f $local_data2_file
fi
# Download the file from HDFS to the local filesystem.
echo " Downloading $hdfs_data_file to local directory."
# The file will be named 'data.txt' in the local directory.
hdfs dfs -get $hdfs_data_file $local_data2_file > $null

# List local files to confirm the download.
echo " Local files list (should show $local_data2_file):"
ls -lh "$local_data2_file"

echo "-----------------------------------------------------"

if hdfs dfs -test -e $hdfs_small_file; then
   hdfs dfs -rm $hdfs_small_file > $null
fi
# Upload the second file (small_blocks.txt).
echo "️ Uploading $local_small_file to $hdfs_small_file"
# Using $hdfs_dir as the destination directory name.
hdfs dfs -put $local_small_file $hdfs_small_file > $null


if [ -e "$output_file" ]; then
   rm -f $output_file
fi
# Merge all files in the HDFS directory to a single local file.
echo " Merging all files in $hdfs_dir to local file: $output_file"

# The files test/data.txt and test/small_blocks.txt are merged into merged.txt
hdfs dfs -getmerge $hdfs_dir $output_file > $null
echo "   -> Files merged successfully into $output_file"

echo "====================================================="
echo " HDFS AUTOMATION SCRIPT COMPLETE"
echo "====================================================="


# questions asked-
# 1. how would the script react if the cluster is down?
# 2. error handling for the hdfs reaction.
# 3. Linux Functions for repetitive commands

