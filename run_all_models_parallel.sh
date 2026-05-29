#!/bin/bash

# Batch process all models in parallel with skip-existing option
echo "Starting parallel batch processing for all models..."
echo "===================================================="

models=(
    # "gpt-4.1-nano"
    # "gpt-4.1-mini" 
    # "gpt-5-nano"
    # "gpt-5-mini"
    # "DeepSeek-V3.1"
    "DeepSeek-V4-Flash"
)

# Function to process a single model
process_model() {
    local model=$1
    echo "Starting model: $model"
    uv run python batch_process_model.py --model "$model" --skip-existing
    
    if [ $? -eq 0 ]; then
        echo "✅ Completed: $model"
    else
        echo "❌ Failed: $model"
    fi
}

# Export the function to use with parallel
export -f process_model

# Run all models in parallel (limit to 3 concurrent jobs to avoid overwhelming system)
printf '%s\n' "${models[@]}" | xargs -P 6 -I {} bash -c 'process_model "$@"' _ {}

echo ""
echo "===================================================="
echo "All models processing complete!"
echo "===================================================="
