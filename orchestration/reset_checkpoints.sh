#!/bin/bash
set -e

echo "⚠️  This will DELETE all checkpoints and data."

read -p "Are you sure you want to clear all checkpoints? (Y/N): " answer

if [[ "$answer" == "Y" ]]; then

    # checkpoint paths
    kafka_ingestion_chkpt="/tmp/checkpoints-fraud"

    writing_to_bronze_layer_parquet_chkpt="/tmp/checkpoints-raw_bronze_layer_parquet"
    writing_to_bronze_layer_console_chkpt="/tmp/checkpoints-raw_bronze_layer"

    bronze_to_silver_parquet_chkpt="/tmp/checkpoints-bronze_to_silver_parquet"
    bronze_to_silver_console_chkpt="/tmp/checkpoints-bronze_to_silver"

    gold_layer_highamount_parquet_chkpt="/tmp/checkpoints-highamount_parquet"
    gold_layer_velocity_parquet_chkpt="/tmp/checkpoints-velocity_parquet"

    gold_layer_highamount_console_chkpt="/tmp/checkpoints-highamount"
    gold_layer_velocity_console_chkpt="/tmp/checkpoints-velocity"

    echo "🧹 Clearing checkpoints..."

    rm -rf "$kafka_ingestion_chkpt"
    rm -rf "$writing_to_bronze_layer_parquet_chkpt"
    rm -rf "$writing_to_bronze_layer_console_chkpt"
    rm -rf "$bronze_to_silver_parquet_chkpt"
    rm -rf "$bronze_to_silver_console_chkpt"
    rm -rf "$gold_layer_highamount_parquet_chkpt"
    rm -rf "$gold_layer_velocity_parquet_chkpt"
    rm -rf "$gold_layer_highamount_console_chkpt"
    rm -rf "$gold_layer_velocity_console_chkpt"

    echo "✅ Checkpoints cleared."

else
    echo "Aborted operation"
    exit 1
fi


read -p "Do you want to clear data directories? (Y/N): " confirm

if [[ "$confirm" == "Y" ]]; then

    echo "🧹 Clearing data layers..."

    rm -rf ../data/raw_bronze_layer/*
    rm -rf ../data/transformed_silver_layer/*
    rm -rf ../data/gold_layer/*

    echo "✅ All layers cleared, ready for fresh run"

else
    echo "Directory clear operation aborted"
    exit 1
fi


