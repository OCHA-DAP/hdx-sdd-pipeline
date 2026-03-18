"use client";
import { useState, useEffect } from "react";
import { Dataset } from "../types/dataset";
import { getApiUrl } from "../services/api";

interface Props {
  datasets: Dataset[];
}

interface Report {
  resource_id?: string;
  file_name?: string;
  file_url?: string;
  sheet_name: string;
  processing_timestamp?: string;
  processing_success: boolean;
  n_records?: number;
  n_columns?: number;
  personal_data_sensitive?: boolean;
  non_personal_data_sensitive?: boolean;
  columns?: Array<{
    column_name: string;
    sample_values: string[];
    personal_data?: {
      entity_type?: string;
      sensitive?: boolean;
    };
    non_personal_data?: {
      sensitivity?: string;
    };
  }>;
  error_source?: string;
  error_message?: string;
}

interface ReportResponse {
  has_report: boolean;
  report_path?: string;
  report?: Report[];
  sensitivity?: string;
  message?: string;
}

const MODEL_OPTIONS = [
    "gpt-5-nano",
    "gpt-5-mini",
    "gpt-4.1-nano",
    "gpt-4.1-mini",
    "gpt-4.1",
    "DeepSeek-V3.1",
];

export default function ResultsTab({ datasets }: Props) {
  const [selectedDataset, setSelectedDataset] = useState<string | null>(null);
  const [selectedModel, setSelectedModel] = useState(MODEL_OPTIONS[0]);
  const [report, setReport] = useState<ReportResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [selectedSheetIndex, setSelectedSheetIndex] = useState(0);
  const [reportStatus, setReportStatus] = useState<Record<string, boolean>>({});
  const [checkingStatus, setCheckingStatus] = useState(false);

  const checkReportExists = async (datasetName: string, modelName: string): Promise<boolean> => {
    try {
      const params = new URLSearchParams({
        dataset_filename: datasetName,
        model_name: modelName,
      });

      const response = await fetch(`${getApiUrl('has-report')}?${params}`, {
        method: "POST",
      });

      if (response.ok) {
        const data = await response.json();
        return data.has_report === true;
      }
      return false;
    } catch (error) {
      console.error("Error checking report existence:", error);
      return false;
    }
  };

  const checkAllReports = async () => {
    if (datasets.length === 0) return;
    
    setCheckingStatus(true);
    const statusMap: Record<string, boolean> = {};
    
    // Check all datasets in parallel
    const checks = datasets.map(async (dataset) => {
      const hasReport = await checkReportExists(dataset.name, selectedModel);
      statusMap[dataset.name] = hasReport;
    });
    
    await Promise.all(checks);
    setReportStatus(statusMap);
    setCheckingStatus(false);
  };

  const fetchReport = async (datasetName: string, modelName: string) => {
    setLoading(true);
    try {
      const params = new URLSearchParams({
        dataset_filename: datasetName,
        model_name: modelName,
      });

      const response = await fetch(getApiUrl(`api/generate-report?${params}`), {
        method: "POST",
      });

      if (response.ok) {
        const data: ReportResponse = await response.json();
        setReport(data);
        // Update status after fetching
        setReportStatus((prev) => ({
          ...prev,
          [datasetName]: data.has_report || !!data.report,
        }));
      } else {
        const error = await response.json();
        console.error("Failed to fetch report:", error);
        setReport(null);
      }
    } catch (error) {
      console.error("Error fetching report:", error);
      setReport(null);
    } finally {
      setLoading(false);
    }
  };

  const generateReport = async () => {
    if (!selectedDataset || !selectedModel) return;
    
    setLoading(true);
    try {
      const params = new URLSearchParams({
        dataset_filename: selectedDataset,
        model_name: selectedModel,
      });

      const response = await fetch(getApiUrl(`api/generate-report?${params}`), {
        method: "POST",
      });

      if (response.ok) {
        const data: ReportResponse = await response.json();
        setReport(data);
        // Update status after generating
        setReportStatus((prev) => ({
          ...prev,
          [selectedDataset]: true,
        }));
      } else {
        const error = await response.json();
        console.error("Failed to generate report:", error);
        alert(`Failed to generate report: ${error.detail || 'Unknown error'}`);
      }
    } catch (error) {
      console.error("Error generating report:", error);
      alert("Failed to generate report");
    } finally {
      setLoading(false);
    }
  };

  // Check report status when model or datasets change
  useEffect(() => {
    if (datasets.length > 0) {
      checkAllReports();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedModel, datasets.length]);

  useEffect(() => {
    if (selectedDataset && selectedModel) {
      const hasReport = reportStatus[selectedDataset];
      if (hasReport) {
        // Only fetch if report exists
        fetchReport(selectedDataset, selectedModel);
        setSelectedSheetIndex(0); // Reset to first sheet when fetching new report
      } else {
        // Clear report if no report exists
        setReport(null);
        setSelectedSheetIndex(0);
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedDataset, selectedModel]);

  return (
    <div className="flex gap-6 h-[calc(100vh-200px)]">
      {/* Left side - Dataset list */}
      <div className="w-1/3 border-r pr-6 overflow-y-auto">
        <div className="mb-4">
          <h2 className="text-xl text-white font-semibold mb-2">Datasets</h2>
          <div className="mb-3">
            <label className="text-sm text-white/80 block mb-1">Model:</label>
            <select
              value={selectedModel}
              onChange={(e) => setSelectedModel(e.target.value)}
              className="w-full px-2 py-1 rounded text-sm text-black"
            >
              {MODEL_OPTIONS.map((model) => (
                <option key={model} value={model}>
                  {model}
                </option>
              ))}
            </select>
          </div>
        </div>

      {datasets.length === 0 ? (
          <div className="text-sm text-white/80 border rounded-lg p-4">
          Upload datasets to see results.
        </div>
      ) : (
          <div className="space-y-2">
            {checkingStatus && (
              <div className="text-xs text-white/60 text-center py-2">
                Checking report status...
              </div>
            )}
            {datasets.map((d) => {
              const hasReport = reportStatus[d.name] === true;
              const isSelected = selectedDataset === d.name;
              
              return (
                <button
                  key={d.id}
                  onClick={() => setSelectedDataset(d.name)}
                  className={`w-full text-left border-2 rounded-lg p-3 transition-all duration-300 relative overflow-hidden ${
                    isSelected
                      ? "bg-white/10 text-white border-[#009edb] shadow-[0_0_15px_rgba(0,158,219,0.5)]"
                      : hasReport
                      ? "bg-white/10 text-white/80 border-green-500/50 hover:bg-white/20 shadow-[0_0_15px_rgba(34,197,94,0.3)]"
                      : "bg-white/10 text-white/80 border-red-500/50 hover:bg-white/20 shadow-[0_0_15px_rgba(239,68,68,0.3)]"
                  }`}
                >
                  {/* Status indicator dot */}
                  <div className="absolute top-2 right-2">
                    <div
                      className={`w-2 h-2 rounded-full ${
                        hasReport
                          ? "bg-green-400 shadow-[0_0_8px_rgba(34,197,94,0.8)]"
                          : "bg-red-400 shadow-[0_0_8px_rgba(239,68,68,0.8)]"
                      }`}
                    />
                  </div>
                  
                  <div className="flex items-center justify-between pr-4">
                    <div className="flex-1 min-w-0">
                      <h3 className={`font-medium truncate ${isSelected ? 'text-white' : ''}`}>{d.name}</h3>
                      <p className="text-xs mt-1 opacity-70">
                        {hasReport ? "✓ Report available" : "✗ No report"}
                      </p>
                    </div>
                    <span className="text-xs px-2 py-1 rounded bg-gray-100 text-black ml-2 flex-shrink-0">
                      {d.name.split(".").pop()?.toUpperCase()}
                    </span>
                  </div>
                </button>
              );
            })}
          </div>
        )}
      </div>

      {/* Right side - Report display */}
      <div className="flex-1 overflow-y-auto">
        <h2 className="text-xl text-white font-semibold mb-4">Report</h2>

        {!selectedDataset ? (
          <div className="text-sm text-white/80 border rounded-lg p-6">
            Select a dataset to view its report.
          </div>
        ) : (() => {
          const hasReport = reportStatus[selectedDataset];
          
          if (!hasReport) {
            return (
              <div className="flex flex-col items-center justify-center border rounded-lg p-8 bg-white/5 min-h-[400px]">
                <div className="text-center mb-6">
                  <div className="w-16 h-16 rounded-full bg-red-500/20 flex items-center justify-center mb-4 mx-auto">
                    <svg
                      className="w-8 h-8 text-red-400"
                      fill="none"
                      stroke="currentColor"
                      viewBox="0 0 24 24"
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth={2}
                        d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                      />
                    </svg>
                  </div>
                  <h3 className="text-xl font-semibold text-white mb-2">
                    No Report Found
                  </h3>
                  <p className="text-white/70 mb-1">
                    No report exists for this dataset and model combination.
                  </p>
                  <p className="text-sm text-white/60">
                    Dataset: <span className="font-mono">{selectedDataset}</span>
                  </p>
                  <p className="text-sm text-white/60 mb-6">
                    Model: <span className="font-mono">{selectedModel}</span>
                  </p>
                  <button
                    onClick={generateReport}
                    disabled={loading}
                    className="px-6 py-3 bg-[#009edb] text-white rounded-lg hover:bg-[#0088c2] disabled:opacity-50 disabled:cursor-not-allowed font-medium transition shadow-lg hover:shadow-xl"
                  >
                    {loading ? (
                      <span className="flex items-center gap-2">
                        <svg
                          className="animate-spin h-5 w-5"
                          xmlns="http://www.w3.org/2000/svg"
                          fill="none"
                          viewBox="0 0 24 24"
                        >
                          <circle
                            className="opacity-25"
                            cx="12"
                            cy="12"
                            r="10"
                            stroke="currentColor"
                            strokeWidth="4"
                          />
                          <path
                            className="opacity-75"
                            fill="currentColor"
                            d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                          />
                        </svg>
                        Generating...
                      </span>
                    ) : (
                      "Generate Report"
                    )}
                  </button>
                </div>
              </div>
            );
          }
          
          if (loading) {
            return (
              <div className="flex items-center justify-center border rounded-lg p-8 bg-white/5 min-h-[400px]">
                <div className="text-center">
                  <svg
                    className="animate-spin h-12 w-12 text-[#009edb] mx-auto mb-4"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="none"
                    viewBox="0 0 24 24"
                  >
                    <circle
                      className="opacity-25"
                      cx="12"
                      cy="12"
                      r="10"
                      stroke="currentColor"
                      strokeWidth="4"
                    />
                    <path
                      className="opacity-75"
                      fill="currentColor"
                      d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                    />
                  </svg>
                  <p className="text-white/80">Loading report...</p>
                </div>
              </div>
            );
          }
          
          if (!report || !report.report) {
            return (
              <div className="text-sm text-white/80 border rounded-lg p-6">
                No report data available.
              </div>
            );
          }
          
          return (
            <div className="space-y-4">
              {/* Overall sensitivity */}
              {report.sensitivity && (
                <div className="border rounded-lg p-4 bg-white/10">
                  <h3 className="font-semibold text-white mb-2">Overall Sensitivity</h3>
                  <p className="text-lg font-medium text-[#009edb]">{report.sensitivity}</p>
                </div>
              )}

              {/* Sheet tabs */}
              {Array.isArray(report.report) && report.report.length > 1 && (
                <div className="border-b border-white/20">
                  <div className="flex gap-2 overflow-x-auto">
                    {report.report.map((sheetReport, idx) => (
                      <button
                        key={idx}
                        onClick={() => setSelectedSheetIndex(idx)}
                        className={`px-4 py-2 font-medium text-sm whitespace-nowrap transition ${
                          selectedSheetIndex === idx
                            ? 'border-b-2 border-[#009edb] font-semibold text-white'
                            : 'text-white/60 hover:text-white/80'
                        }`}
                      >
                        {sheetReport.sheet_name}
                      </button>
                    ))}
                  </div>
                </div>
              )}

              {/* Selected sheet report */}
              {Array.isArray(report.report) && report.report[selectedSheetIndex] && (() => {
                const sheetReport = report.report![selectedSheetIndex];
                return (
                  <div className="border rounded-lg p-4 bg-white/5">
                    <div className="mb-4">
                      {report.report!.length === 1 && (
                        <h3 className="text-lg font-semibold text-white mb-2">
                          Sheet: {sheetReport.sheet_name}
                        </h3>
                      )}
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                        {sheetReport.n_records !== undefined && (
                          <div>
                            <span className="text-white/60">Records:</span>
                            <span className="text-white ml-2">{sheetReport.n_records}</span>
                          </div>
                        )}
                        {sheetReport.n_columns !== undefined && (
                          <div>
                            <span className="text-white/60">Columns:</span>
                            <span className="text-white ml-2">{sheetReport.n_columns}</span>
                          </div>
                        )}
                        <div>
                          <span className="text-white/60">Personal Data Sensitive:</span>
                          <span className={`ml-2 ${sheetReport.personal_data_sensitive ? 'text-red-400' : 'text-green-400'}`}>
                            {sheetReport.personal_data_sensitive ? 'Yes' : 'No'}
                          </span>
                        </div>
                        <div>
                          <span className="text-white/60">Non-Personal Data Sensitive:</span>
                          <span className={`ml-2 ${sheetReport.non_personal_data_sensitive ? 'text-red-400' : 'text-green-400'}`}>
                            {sheetReport.non_personal_data_sensitive ? 'Yes' : 'No'}
                          </span>
                        </div>
                      </div>
                      {sheetReport.processing_success === false && (
                        <div className="mt-2 p-2 bg-red-500/20 border border-red-500/50 rounded">
                          <p className="text-sm text-red-300">
                            <strong>Error:</strong> {sheetReport.error_source || 'Unknown error'}
                          </p>
                          {sheetReport.error_message && (
                            <p className="text-xs text-red-200 mt-1">{sheetReport.error_message}</p>
                          )}
                        </div>
                      )}
                    </div>

                    {/* Columns */}
                    {sheetReport.columns && sheetReport.columns.length > 0 && (
                      <div className="mt-4">
                        <h4 className="font-semibold text-white mb-2">Columns</h4>
                        <div className="space-y-2">
                          {sheetReport.columns.map((col, colIdx) => (
                            <div key={colIdx} className="border rounded p-3 bg-white/5">
                              <div className="flex items-center justify-between mb-2">
                                <span className="font-medium text-white">{col.column_name}</span>
                                <div className="flex gap-2">
                                  {col.personal_data?.entity_type && (
                                    <span className="text-xs px-2 py-1 rounded bg-blue-500/30 text-blue-200">
                                      Personal Data: {col.personal_data.entity_type}
                                    </span>
                                  )}
                                  {col.personal_data?.sensitive && (
                                    <span className="text-xs px-2 py-1 rounded bg-red-500/30 text-red-200">
                                      Sensitive
                                    </span>
                                  )}
                                  {col.non_personal_data?.sensitivity && (
                                    <span className="text-xs px-2 py-1 rounded bg-yellow-500/30 text-yellow-200">
                                      {col.non_personal_data.sensitivity}
                                    </span>
                                  )}
                                </div>
                              </div>
                              {col.sample_values && col.sample_values.length > 0 && (
                                <div className="mt-2">
                                  <p className="text-xs text-white/60 mb-1">Sample values:</p>
                                  <div className="flex flex-wrap gap-1">
                                    {col.sample_values.slice(0, 5).map((val, valIdx) => (
                                      <span
                                        key={valIdx}
                                        className="text-xs px-2 py-1 rounded bg-gray-700 text-white/80"
                                      >
                                        {String(val).substring(0, 30)}
                                        {String(val).length > 30 ? '...' : ''}
                                      </span>
                                    ))}
                                  </div>
                                </div>
                              )}
            </div>
          ))}
                        </div>
        </div>
      )}
                  </div>
                );
              })()}
            </div>
          );
        })()}
      </div>
    </div>
  );
}
