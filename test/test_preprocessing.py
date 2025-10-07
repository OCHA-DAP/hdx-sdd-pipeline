from processing.analyze_xlsx import analyze_excel_file

def test_analyze_excel_file():
    file_path = "processing/tests/test.xlsx"
    output = analyze_excel_file(file_path)
    assert output is not None
    assert output["multiple_tables"] is True
    assert output["multiple_sheets"] is True
    print(output["multiple_tables"])
    assert output["multiple_tables"] == {'SalesData': False, 'Metadata': False, 'MixedData': True}
