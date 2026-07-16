#!/bin/bash

# Batch process all models with skip-existing option
echo "Starting batch processing for all models..."
echo "=========================================="

models=(
    "gpt-5.4-mini"
    # "gpt-5.4"
    # "DeepSeek-V3.1"
    # "DeepSeek-V4-Pro"
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
