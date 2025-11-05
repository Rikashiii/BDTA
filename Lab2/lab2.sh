#!/bin/bash
# Author: Your Name (e.g., Cloudera User)
# Created: 26-10-2025

# Script: lab2.sh
# Description: Implements a MapReduce application to count word frequency 
#              in 'Complete_Shakespeare.txt' while excluding stop words.

# Usage: ./lab2.sh input_shakespeare Shakespere_work

# Tasks: 
#   1. Compile Java code.
#   2. Create shakesperework.jar with Manifest.
#   3. Upload input and stop_words files to HDFS.
#   4. Run the YARN job using 'yarn jar'.

# NOTE: This script assumes 'Complete_Shakespeare.txt' and 'stop_words.txt' 
#       are present in the project path: /home/cloudera/workspace/WordCountShakespere/

# =======================================================================
# 1. CONFIGURATION
# =======================================================================

# File Names
DRIVER_CLASS="WordCountCompleteShakespere"
JAR_NAME="shakesperework.jar"
INPUT_FILE="Complete_Shakespeare.txt"
STOP_WORDS_FILE="stop_words.txt"
# Folder Names
SRC_DIR="src/wordcount"
PROJECT_PATH="/home/cloudera/workspace/WordCountShakespere"
SRC_DIR="src/wordcountshakespere"
CLASS_DIR="bin"
NULL="/dev/null"

# HDFS Paths
HDFS_HOME="/user/$(whoami)"
HDFS_INPUT_DIR="${HDFS_HOME}/input_shakespeare"
HDFS_STOP_WORDS_PATH="${HDFS_HOME}/${STOP_WORDS_FILE}"
# The business required output folder name:
HDFS_OUTPUT_DIR="Shakespere_work" 
MANIFEST_FILE="MANIFEST.txt"

# =======================================================================
# 2. BUILD THE APPLICATION
# =======================================================================

echo "--- 1. Cleaning and Compiling Java Sources ---"
# Remove old class directory and create a new one
rm -rf ${PROJECT_PATH}/${CLASS_DIR}
mkdir -p ${PROJECT_PATH}/${CLASS_DIR}

# Get the Hadoop classpath (essential for compilation)
HADOOP_CLASSPATH=$(hadoop classpath)

# Compile all Java files in the source directory
# Ensure you are compiling your main driver and inner classes correctly.
javac -classpath ${HADOOP_CLASSPATH} -d ${PROJECT_PATH}/bin ${PROJECT_PATH}/${SRC_DIR}/*.java

if [ $? -ne 0 ]; then
    echo "Compilation FAILED. Exiting."
    exit 1
else
	echo "Compilation SUCCESSFULL."
fi
	

echo "--- 2. Creating JAR File (${JAR_NAME}) ---"
# ADD: Create the MANIFEST file, specifying the full class path (package.ClassName)
echo "Main-Class: wordcountshakespere.WordCountCompleteShakespere" > ${PROJECT_PATH}/${MANIFEST_FILE}

# '-C classes' includes the compiled classes from the classes directory
# -c means create, -v means verbose, -f specifies the file name
jar -cvfm ${PROJECT_PATH}/${JAR_NAME} ${PROJECT_PATH}/${MANIFEST_FILE} -C ${PROJECT_PATH}/bin . > $NULL 2>&1

if [ $? -ne 0 ]; then
    echo "Jar File Creation FAILED. Exiting."
    exit 1
else
	echo "Jar File Creation SUCCESSFULL."
fi

# Clean up the manifest file (optional)
rm "${PROJECT_PATH}/${MANIFEST_FILE}" > /dev/null

# =======================================================================
# 3. HDFS SETUP AND CLEANUP
# =======================================================================

echo "--- 3. HDFS Setup ---"

# Upload Input File (Complete_Shakespeare.txt)
hdfs dfs -mkdir -p ${HDFS_INPUT_DIR}
echo "Uploading input file: ${INPUT_FILE} to ${HDFS_INPUT_DIR}"
hdfs dfs -put -f ${INPUT_FILE} ${HDFS_INPUT_DIR}/ > $NULL  2>&1

if [ $? -ne 0 ]; then
    echo "Uploading $INPUT_FILE FAILED. Exiting."
    exit 1
fi

# Upload Stop Words File (stop_words.txt)
echo "Uploading stop words file: ${STOP_WORDS_FILE} to ${HDFS_HOME}"
hdfs dfs -put -f ${STOP_WORDS_FILE} ${HDFS_STOP_WORDS_PATH}

if [ $? -ne 0 ]; then
    echo "Uploading FAILED. Exiting."
    exit 1
fi

# Clean up the output directory (required)
echo "Cleaning up HDFS output directory: ${HDFS_OUTPUT_DIR}"
hdfs dfs -rm -r -skipTrash ${HDFS_OUTPUT_DIR} > $NULL 2>&1


# =======================================================================
# 4. RUN THE YARN APPLICATION
# =======================================================================

echo "--- 4. Running MapReduce Job ---"

# The command required by the business demands:
# yarn jar shakesperework.jar <Input path> <Output path>
# The <Input path> must be the specific file path on HDFS
# The <Output path> must be the required output folder path

INPUT_PATH="${HDFS_INPUT_DIR}/${INPUT_FILE}"
OUTPUT_PATH="${HDFS_OUTPUT_DIR}"

echo "Running with: yarn jar ${PROJECT_PATH}/${JAR_NAME} ${INPUT_PATH} ${OUTPUT_PATH}"
yarn jar ${PROJECT_PATH}/${JAR_NAME} ${INPUT_PATH} ${OUTPUT_PATH} > $NULL 2>&1

# =======================================================================
# 5. VERIFICATION
# =======================================================================

if [ $? -eq 0 ]; then
    echo ""
    echo "Job finished successfully!"
    echo "Output is stored in the folder: ${HDFS_OUTPUT_DIR}"
    echo "Verifying 3 output files..."
    # Check the number of part files (should be 3, plus _SUCCESS)
    hdfs dfs -ls ${HDFS_OUTPUT_DIR} | grep 'part-r-' | wc -l
else
    echo ""
    echo "Job FAILED!"
fi