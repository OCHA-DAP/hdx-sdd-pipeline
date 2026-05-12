#!/bin/bash

# Batch process all models with skip-existing option
echo "Starting batch processing for all models..."
echo "=========================================="

models=(
    "gpt-4.1-nano"
    "gpt-4.1-mini" 
    "gpt-4.1"
    "gpt-5-nano"
    "gpt-5-mini"
    "DeepSeek-V3.1"
    "DeepSeek-V4-Flash"
)

for model in "${models[@]}"; do
    echo ""
    echo "Processing model: $model"
    echo "------------------------"
    uv run python batch_process_model.py --model "$model" --skip-existing
    
    if [ $? -eq 0 ]; then
        echo "✅ Completed: $model"
    else
        echo "❌ Failed: $model"
    fi
done

echo ""
echo "=========================================="
echo "All models processing complete!"
echo "=========================================="
