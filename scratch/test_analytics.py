
def _normalize_flag(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ('true', 'yes', 'y', '1', 'high_sensitive', 'moderate_sensitive'):
            return True
        if v in ('false', 'no', 'n', '0', '', 'non_sensitive', 'none'):
            return False
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return False

gt_sheet1 = {
    "sheet_name": "2023 Data",
    "personal_data_sensitive": False,
    "non_personal_data_sensitive": True,
}
gt_sheet2 = {
    "sheet_name": "MetaData&Notes",
    "personal_data_sensitive": False,
    "non_personal_data_sensitive": False,
}
groundtruth_data = [gt_sheet1, gt_sheet2]

gt_personal = False
gt_non_personal = False
gt_sheets = {}

for sheet in groundtruth_data:
    personal_flag = _normalize_flag(sheet.get('personal_data_sensitive', False))
    non_personal_flag = _normalize_flag(sheet.get('non_personal_data_sensitive', False))
    
    if not non_personal_flag and isinstance(sheet.get('non_personal_data'), dict):
        nested_sens = sheet['non_personal_data'].get('sensitivity')
        if nested_sens and _normalize_flag(nested_sens):
            non_personal_flag = True

    if personal_flag:
        gt_personal = True
    if non_personal_flag:
        gt_non_personal = True
    gt_sheets[sheet.get('sheet_name').strip().lower()] = {
        'personal_data_sensitive': personal_flag,
        'non_personal_data_sensitive': non_personal_flag,
    }

gt_overall = gt_personal or gt_non_personal
print(f"{gt_personal=}, {gt_non_personal=}, {gt_overall=}")

model_data = {
  "sdd_report": [
    {
      "sheet_name": "2023 Data",
      "personal_data_sensitive": False,
      "non_personal_data_sensitive": True,
    },
    {
      "sheet_name": "MetaData&Notes",
      "personal_data_sensitive": False,
      "non_personal_data_sensitive": False,
    }
  ]
}

model_personal = False
model_non_personal = False
sheet_reports = model_data["sdd_report"]
for sheet in sheet_reports:
    if sheet.get('personal_data_sensitive', False):
        model_personal = True
    if sheet.get('non_personal_data_sensitive', False):
        model_non_personal = True

model_overall = model_personal or model_non_personal
print(f"{model_personal=}, {model_non_personal=}, {model_overall=}")

if model_overall and gt_overall:
    print("True Positive")
elif model_overall and not gt_overall:
    print("False Positive")
elif not model_overall and gt_overall:
    print("False Negative")
else:
    print("True Negative")
