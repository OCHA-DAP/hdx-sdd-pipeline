# Batch Processing Guide

## Running a New Model on All Groundtruth Datasets

### Quick Start

To process all datasets with the `gpt-4.1` model:

```bash
python batch_process_model.py --model gpt-4.1
```

### Options

#### Skip Existing Results
If you want to skip datasets that already have results (useful for resuming interrupted runs):

```bash
python batch_process_model.py --model gpt-4.1 --skip-existing
```

#### Test with Limited Datasets
To test with just a few datasets first:

```bash
python batch_process_model.py --model gpt-4.1 --limit 5
```

### What It Does

1. **Finds all datasets** in `research/results/test_results/groundtruth2/`
2. **Locates source files** in `research/data/`
3. **Processes each dataset** with the specified model
4. **Saves results** to `research/results/test_results/{model_name}/`
5. **Shows progress** with detailed logging

### Output

Results are saved to:
```
research/results/test_results/gpt-4.1/
├── Event Data AFG.csv.json
├── HAPI IDPS Yemen.csv.json
├── ...
```

### Example Output

```
======================================================================
Batch Processing with Model: gpt-4.1
======================================================================

Found 28 datasets in groundtruth2

Processing 28 datasets...

[1/28] Event Data AFG.csv
📊 Processing: Event Data AFG.csv
✅ Completed Event Data AFG.csv: 1 sheets, 0 sensitive

[2/28] HAPI IDPS Yemen.csv
📊 Processing: HAPI IDPS Yemen.csv
✅ Completed HAPI IDPS Yemen.csv: 1 sheets, 1 sensitive

...

======================================================================
BATCH PROCESSING COMPLETE
======================================================================
Total datasets: 28
✅ Successful: 28
⏭️  Skipped: 0
❌ Failed: 0

Results saved to: research/results/test_results/gpt-4.1
======================================================================
```

### Troubleshooting

**Source file not found:**
- Make sure the original data files are in `research/data/`
- The script looks for files with the exact same name as in groundtruth2

**API errors:**
- Check your `.env` file has correct Azure OpenAI credentials
- Ensure the model name is correct and deployed in Azure

**Out of memory:**
- The script processes datasets one at a time to avoid memory issues
- If a single dataset is too large, adjust `max_rows` in the script

### After Processing

Once complete, you can:

1. **View statistics** in the dashboard at `http://localhost:3000`
2. **Compare models** in the "Model Comparison" tab
3. **Generate PDF reports** with the new model's results

### Advanced Usage

#### Process with Different ISP Rules

Edit the script and change line 223:
```python
isp_rules = load_isp_rules('afghanistan')  # Instead of 'default'
```

#### Change Sample Size

Edit line 83:
```python
sample_size=10,  # Instead of 5
```

#### Process Specific Datasets Only

Create a custom list:
```python
datasets = ['Event Data AFG.csv', 'HAPI IDPS Yemen.csv']
```

## Model Names

Available models (check your Azure deployment):
- `gpt-4.1`
- `gpt-4.1-nano`
- `gpt-4.1-mini`
- `gpt-5-nano`
- `gpt-5-mini`
- `DeepSeek-V3.1`
