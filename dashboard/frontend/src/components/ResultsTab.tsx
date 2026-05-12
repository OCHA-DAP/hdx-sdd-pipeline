"use client";
import { useState, useEffect } from "react";
import { FileText, Download, ChevronRight, Search, Filter, Info, CheckCircle, XCircle, Eye, ChevronDown } from "lucide-react";
import { getApiUrl } from "../services/api";

interface ModelResult {
  model: string;
  dataset: string;
  processed_at: string;
  file_path: string;
  sensitivity?: string;
  pii_count?: number;
  row_count?: number;
  status: "completed" | "failed" | "processing";
}

interface ReportDetail {
  dataset_name: string;
  model: string;
  processed_at: string;
  sheets: {
    [sheet_name: string]: {
      columns: string[];
      predictions: {
        [column_name: string]: {
          prediction: string;
          confidence?: number;
          reasoning?: string;
          sample_values?: string[];
          explanation?: string;
          isp_used?: string;
        };
      };
      metadata: {
        total_rows: number;
        pii_detected: number;
        sensitivity_level: string;
        personal_data_sensitive: boolean;
        non_personal_data_sensitive: boolean;
        explanation?: string;
        non_personal_explanation?: string;
        non_personal_sensitivity?: string;
        isp_used?: string;
      };
      groundtruth?: {
        personal_data_sensitive: boolean;
        non_personal_data_sensitive: boolean;
      };
    };
  };
  groundtruth?: any;
}

interface ErrorAnalysis {
  dataset: string;
  model: string;
  errorType: 'false_positive' | 'false_negative';
  groundTruth: {
    personal_data_sensitive: boolean;
    non_personal_data_sensitive: boolean;
  };
  prediction: {
    personal_data_sensitive: boolean;
    non_personal_data_sensitive: boolean;
  };
  sheetName?: string;
}

interface Props {
  selectedModel?: string;
}

export default function ResultsTab({ selectedModel: propSelectedModel }: Props = { selectedModel: "" }) {
  const [models, setModels] = useState<string[]>([]);
  const [internalSelectedModel, setInternalSelectedModel] = useState<string>("");
  const [modelResults, setModelResults] = useState<ModelResult[]>([]);
  const [expandedReports, setExpandedReports] = useState<Set<string>>(new Set());
  const [reportDetails, setReportDetails] = useState<Record<string, ReportDetail>>({});
  const [errorAnalysis, setErrorAnalysis] = useState<ErrorAnalysis[]>([]);
  const [loading, setLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState("");
  const [modelsExpanded, setModelsExpanded] = useState(false);

  // Use prop if provided, otherwise use internal state
  const currentSelectedModel = propSelectedModel || internalSelectedModel;

  useEffect(() => {
    fetchModels();
  }, []);

  useEffect(() => {
    if (currentSelectedModel) {
      fetchModelResults(currentSelectedModel);
    }
  }, [currentSelectedModel]);

  // Analyze errors when report details are loaded
  useEffect(() => {
    if (currentSelectedModel && modelResults.length > 0) {
      // Automatically fetch and analyze all reports for the current model
      modelResults.forEach(result => {
        fetchReportDetail(currentSelectedModel, result.dataset);
      });
    }
  }, [currentSelectedModel, modelResults]);

  const fetchModels = async () => {
    try {
      const response = await fetch(getApiUrl("api/models"));
      if (response.ok) {
        const data = await response.json();
        setModels(data.models || []);
      }
    } catch (error) {
      console.error("Failed to fetch models:", error);
    }
  };

  const fetchModelResults = async (model: string) => {
    setLoading(true);
    try {
      const response = await fetch(getApiUrl(`api/results/${model}`));
      if (response.ok) {
        const data = await response.json();
        setModelResults(data.results || []);
      }
    } catch (error) {
      console.error("Failed to fetch model results:", error);
    } finally {
      setLoading(false);
    }
  };

  const fetchReportDetail = async (model: string, dataset: string) => {
    const cacheKey = `${model}-${dataset}`;
    if (reportDetails[cacheKey]) {
      return; // Already loaded
    }

    try {
      const response = await fetch(getApiUrl(`api/report/${model}/${dataset}`));
      if (response.ok) {
        const data = await response.json();
        setReportDetails(prev => ({ ...prev, [cacheKey]: data }));
        
        // Analyze for errors when report detail is loaded
        analyzeErrors(data, model, dataset);
      }
    } catch (error) {
      console.error("Failed to fetch report detail:", error);
    }
  };

  const analyzeErrors = (reportDetail: ReportDetail, model: string, dataset: string) => {
    const errors: ErrorAnalysis[] = [];
    
    if (!reportDetail.groundtruth && !Object.values(reportDetail.sheets).some(s => s.groundtruth)) return;
    
    const gt = reportDetail.groundtruth;
    
    // Analyze each sheet
    Object.entries(reportDetail.sheets).forEach(([sheetName, sheetData]) => {
      const prediction = {
        personal_data_sensitive: sheetData.metadata.personal_data_sensitive,
        non_personal_data_sensitive: sheetData.metadata.non_personal_data_sensitive
      };
      
      // Use sheet-level ground truth if available, otherwise fallback to file-level
      const sheetGt = sheetData.groundtruth || gt;
      
      if (!sheetGt) return;
      
      // Check for false positives (model predicted sensitive but ground truth says not)
      if (prediction.personal_data_sensitive && !sheetGt.personal_data_sensitive) {
        errors.push({
          dataset,
          model,
          errorType: 'false_positive',
          groundTruth: {
            personal_data_sensitive: sheetGt.personal_data_sensitive,
            non_personal_data_sensitive: sheetGt.non_personal_data_sensitive
          },
          prediction,
          sheetName
        });
      }
      
      if (prediction.non_personal_data_sensitive && !sheetGt.non_personal_data_sensitive) {
        errors.push({
          dataset,
          model,
          errorType: 'false_positive',
          groundTruth: {
            personal_data_sensitive: sheetGt.personal_data_sensitive,
            non_personal_data_sensitive: sheetGt.non_personal_data_sensitive
          },
          prediction,
          sheetName
        });
      }
      
      // Check for false negatives (ground truth says sensitive but model predicted not)
      if (!prediction.personal_data_sensitive && sheetGt.personal_data_sensitive) {
        errors.push({
          dataset,
          model,
          errorType: 'false_negative',
          groundTruth: {
            personal_data_sensitive: sheetGt.personal_data_sensitive,
            non_personal_data_sensitive: sheetGt.non_personal_data_sensitive
          },
          prediction,
          sheetName
        });
      }
      
      if (!prediction.non_personal_data_sensitive && sheetGt.non_personal_data_sensitive) {
        errors.push({
          dataset,
          model,
          errorType: 'false_negative',
          groundTruth: {
            personal_data_sensitive: sheetGt.personal_data_sensitive,
            non_personal_data_sensitive: sheetGt.non_personal_data_sensitive
          },
          prediction,
          sheetName
        });
      }
    });
    
    setErrorAnalysis(prev => {
      const filtered = prev.filter(e => !(e.model === model && e.dataset === dataset));
      return [...filtered, ...errors];
    });
  };

  const toggleReportExpansion = async (model: string, dataset: string) => {
    const cacheKey = `${model}-${dataset}`;
    const newExpanded = new Set(expandedReports);
    
    if (newExpanded.has(cacheKey)) {
      newExpanded.delete(cacheKey);
    } else {
      newExpanded.add(cacheKey);
      await fetchReportDetail(model, dataset);
    }
    
    setExpandedReports(newExpanded);
  };

  const filteredResults = modelResults.filter(result =>
    result.dataset.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const getStatusColor = (status: string) => {
    switch (status) {
      case "completed":
        return "bg-green-100 text-green-800";
      case "processing":
        return "bg-yellow-100 text-yellow-800";
      case "failed":
        return "bg-red-100 text-red-800";
      default:
        return "bg-gray-100 text-gray-800";
    }
  };

  const getSensitivityColor = (sensitivity?: string) => {
    if (!sensitivity) return "bg-gray-100 text-gray-800";
    switch (sensitivity.toLowerCase()) {
      case "high":
        return "bg-red-100 text-red-800";
      case "medium":
        return "bg-yellow-100 text-yellow-800";
      case "low":
        return "bg-green-100 text-green-800";
      default:
        return "bg-gray-100 text-gray-800";
    }
  };

  const getPredictionColor = (prediction: string) => {
    return prediction === 'personal_data_sensitive' 
      ? 'bg-red-100 text-red-800' 
      : 'bg-green-100 text-green-800';
  };

  return (
    <div className="p-8">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900 mb-2">Results</h1>
          <p className="text-gray-600">View predictions and reports from all models</p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-4 gap-8">
          {/* Results Content */}
          <div className="lg:col-span-4">
            {currentSelectedModel ? (
              <>
                {/* Search and Filter */}
                <div className="bg-white rounded-xl border border-gray-200 p-4 mb-6">
                  <div className="flex items-center gap-4">
                    <div className="flex-1 relative">
                      <Search className="w-5 h-5 text-gray-400 absolute left-3 top-1/2 transform -translate-y-1/2" />
                      <input
                        type="text"
                        placeholder="Search datasets..."
                        value={searchTerm}
                        onChange={(e) => setSearchTerm(e.target.value)}
                        className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                      />
                    </div>
                    <button className="flex items-center gap-2 px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50">
                      <Filter className="w-4 h-4" />
                      Filter
                    </button>
                  </div>
                </div>

                {/* Error Analysis Table */}
                {errorAnalysis.length > 0 && (
                  <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
                    <h3 className="text-lg font-semibold text-gray-900 mb-4">False Positives and False Negatives</h3>
                    <div className="border border-gray-200 rounded-lg overflow-hidden">
                      <table className="w-full">
                        <thead className="bg-gray-50">
                          <tr>
                            <th className="px-4 py-2 text-left text-xs font-medium text-gray-900">Dataset</th>
                            <th className="px-4 py-2 text-left text-xs font-medium text-gray-900">Sheet</th>
                            <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">Error Type</th>
                            <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">Ground Truth</th>
                            <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">Prediction</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-200">
                          {errorAnalysis
                            .filter(error => error.model === currentSelectedModel)
                            .map((error, index) => (
                              <tr key={`${error.dataset}-${error.sheetName}-${index}`} className="hover:bg-gray-50">
                                <td className="px-4 py-2 text-xs font-medium text-gray-900">{error.dataset}</td>
                                <td className="px-4 py-2 text-xs text-gray-600">{error.sheetName || 'N/A'}</td>
                                <td className="px-4 py-2 text-center">
                                  <span className={`inline-block px-2 py-1 text-xs font-medium rounded ${
                                    error.errorType === 'false_positive' 
                                      ? 'bg-red-100 text-red-800' 
                                      : 'bg-orange-100 text-orange-800'
                                  }`}>
                                    {error.errorType === 'false_positive' ? 'False Positive' : 'False Negative'}
                                  </span>
                                </td>
                                <td className="px-4 py-2 text-xs text-gray-600">
                                  <div className="space-y-1">
                                    <div className="flex items-center gap-1">
                                      <span className="font-medium">Personal:</span>
                                      <span className={error.groundTruth.personal_data_sensitive ? 'text-red-600' : 'text-green-600'}>
                                        {error.groundTruth.personal_data_sensitive ? 'Sensitive' : 'Not Sensitive'}
                                      </span>
                                    </div>
                                    <div className="flex items-center gap-1">
                                      <span className="font-medium">Non-Personal:</span>
                                      <span className={error.groundTruth.non_personal_data_sensitive ? 'text-red-600' : 'text-green-600'}>
                                        {error.groundTruth.non_personal_data_sensitive ? 'Sensitive' : 'Not Sensitive'}
                                      </span>
                                    </div>
                                  </div>
                                </td>
                                <td className="px-4 py-2 text-xs text-gray-600">
                                  <div className="space-y-1">
                                    <div className="flex items-center gap-1">
                                      <span className="font-medium">Personal:</span>
                                      <span className={error.prediction.personal_data_sensitive ? 'text-red-600' : 'text-green-600'}>
                                        {error.prediction.personal_data_sensitive ? 'Sensitive' : 'Not Sensitive'}
                                      </span>
                                    </div>
                                    <div className="flex items-center gap-1">
                                      <span className="font-medium">Non-Personal:</span>
                                      <span className={error.prediction.non_personal_data_sensitive ? 'text-red-600' : 'text-green-600'}>
                                        {error.prediction.non_personal_data_sensitive ? 'Sensitive' : 'Not Sensitive'}
                                      </span>
                                    </div>
                                  </div>
                                </td>
                              </tr>
                            ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}

                {/* Results List */}
                {loading ? (
                  <div className="text-center py-12">
                    <div className="w-8 h-8 border-2 border-blue-600 border-t-transparent rounded-full animate-spin mx-auto mb-4"></div>
                    <p className="text-gray-600">Loading results...</p>
                  </div>
                ) : filteredResults.length === 0 ? (
                  <div className="text-center py-12">
                    <FileText className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                    <p className="text-gray-600">No results found for {currentSelectedModel}</p>
                  </div>
                ) : (
                  <div className="space-y-4">
                    {filteredResults.map((result) => {
                      const cacheKey = `${result.model}-${result.dataset}`;
                      const isExpanded = expandedReports.has(cacheKey);
                      const reportDetail = reportDetails[cacheKey];

                      return (
                        <div
                          key={cacheKey}
                          className="bg-white rounded-xl border border-gray-200 overflow-hidden"
                        >
                          {/* Header */}
                          <div className="p-6">
                            <div className="flex items-start justify-between mb-4">
                              <div className="flex-1">
                                <h3 className="text-lg font-semibold text-gray-900 mb-1">
                                  {result.dataset}
                                </h3>
                                <div className="flex items-center gap-3 text-sm text-gray-600">
                                  <span>{result.model}</span>
                                  <span>•</span>
                                  <span>{new Date(result.processed_at).toLocaleDateString()}</span>
                                </div>
                              </div>
                              {result.sensitivity && (
                                <span className={`px-2 py-1 text-xs font-medium rounded-full ${getSensitivityColor(result.sensitivity)}`}>
                                  {result.sensitivity}
                                </span>
                              )}
                            </div>

                            {/* Stats */}
                            <div className="grid grid-cols-3 gap-4 mb-4">
                              <div className="text-center p-3 bg-gray-50 rounded-lg">
                                <div className="text-lg font-semibold text-gray-900">{result.row_count?.toLocaleString() || "N/A"}</div>
                                <div className="text-xs text-gray-600">Rows</div>
                              </div>
                              <div className="text-center p-3 bg-gray-50 rounded-lg">
                                <div className="text-lg font-semibold text-gray-900">{result.pii_count?.toLocaleString() || "N/A"}</div>
                                <div className="text-xs text-gray-600">PII Found</div>
                              </div>
                              <div className="text-center p-3 bg-gray-50 rounded-lg">
                                <div className="text-lg font-semibold text-gray-900">{result.sensitivity || "N/A"}</div>
                                <div className="text-xs text-gray-600">Sensitivity</div>
                              </div>
                            </div>

                            {/* Actions */}
                            <div className="flex gap-2">
                              <button
                                onClick={() => toggleReportExpansion(result.model, result.dataset)}
                                className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors text-sm"
                              >
                                <Eye className="w-4 h-4" />
                                {isExpanded ? 'Hide Details' : 'Show Details'}
                              </button>
                              <button className="flex items-center gap-2 px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition-colors text-sm">
                                <Download className="w-4 h-4" />
                                Download
                              </button>
                            </div>
                          </div>

                          {/* Expanded Details */}
                          {isExpanded && reportDetail && (
                            <div className="border-t border-gray-200 p-6 bg-gray-50">
                              {Object.entries(reportDetail.sheets).map(([sheetName, sheetData]) => (
                                <div key={sheetName} className="mb-6 last:mb-0">
                                  <h4 className="text-lg font-semibold text-gray-900 mb-4">{sheetName}</h4>
                                  
                                  {/* Sheet Metadata */}
                                  <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-4">
                                    <div className="p-3 bg-white rounded-lg border">
                                      <div className="text-lg font-semibold text-gray-900">{sheetData.metadata.total_rows.toLocaleString()}</div>
                                      <div className="text-xs text-gray-600">Total Rows</div>
                                    </div>
                                    <div className="p-3 bg-white rounded-lg border">
                                      <div className="text-lg font-semibold text-gray-900">{sheetData.metadata.pii_detected.toLocaleString()}</div>
                                      <div className="text-xs text-gray-600">PII Detected</div>
                                    </div>
                                    <div className="p-3 bg-white rounded-lg border">
                                      <div className="text-lg font-semibold text-gray-900">{sheetData.metadata.sensitivity_level}</div>
                                      <div className="text-xs text-gray-600">Sensitivity</div>
                                    </div>
                                    <div className="p-3 bg-white rounded-lg border">
                                      <div className="text-lg font-semibold text-gray-900">{sheetData.metadata.isp_used || "Unknown"}</div>
                                      <div className="text-xs text-gray-600">ISP Used</div>
                                    </div>
                                  </div>


                                  {/* Ground Truth Comparison */}
                                  {(sheetData.groundtruth || reportDetail.groundtruth) && (
                                    <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-3 mb-4">
                                      <div className="text-sm text-yellow-800">
                                        <div className="grid grid-cols-2 gap-4">
                                          <div>
                                            <span className="font-medium text-yellow-800">Ground Truth ({sheetData.groundtruth ? 'Sheet' : 'File'}):</span>
                                            <div className="mt-1 space-y-1">
                                              <div className="flex items-center gap-2">
                                                {(sheetData.groundtruth || reportDetail.groundtruth).personal_data_sensitive ? <CheckCircle className="w-3 h-3 text-green-600" /> : <XCircle className="w-3 h-3 text-red-600" />}
                                                <span className="text-xs">Personal: {(sheetData.groundtruth || reportDetail.groundtruth).personal_data_sensitive ? 'Yes' : 'No'}</span>
                                              </div>
                                              <div className="flex items-center gap-2">
                                                {(sheetData.groundtruth || reportDetail.groundtruth).non_personal_data_sensitive ? <CheckCircle className="w-3 h-3 text-green-600" /> : <XCircle className="w-3 h-3 text-red-600" />}
                                                <span className="text-xs">Non-Personal: {(sheetData.groundtruth || reportDetail.groundtruth).non_personal_data_sensitive ? 'Yes' : 'No'}</span>
                                              </div>
                                            </div>
                                          </div>
                                          <div>
                                            <span className="font-medium text-yellow-800">Model ({sheetData.metadata.isp_used || 'Unknown'}):</span>
                                            <div className="mt-1 space-y-1">
                                              <div className="flex items-center gap-2">
                                                {sheetData.metadata.personal_data_sensitive ? <CheckCircle className="w-3 h-3 text-green-600" /> : <XCircle className="w-3 h-3 text-red-600" />}
                                                <span className="text-xs">Personal: {sheetData.metadata.personal_data_sensitive ? 'Yes' : 'No'}</span>
                                              </div>
                                              <div className="flex items-center gap-2">
                                                {sheetData.metadata.non_personal_data_sensitive ? <CheckCircle className="w-3 h-3 text-green-600" /> : <XCircle className="w-3 h-3 text-red-600" />}
                                                <span className="text-xs">Non-Personal: {sheetData.metadata.non_personal_data_sensitive ? 'Yes' : 'No'}</span>
                                              </div>
                                            </div>
                                          </div>
                                        </div>
                                      </div>
                                    </div>
                                  )}

                                  {/* Explanation */}
                                  {sheetData.metadata.explanation && (
                                    <div className="bg-blue-50 border border-blue-200 rounded-lg p-3 mb-4">
                                      <div className="flex items-center gap-2 mb-1">
                                        <Info className="w-4 h-4 text-blue-600" />
                                        <span className="font-semibold text-blue-900 text-sm">General Explanation</span>
                                      </div>
                                      <p className="text-sm text-blue-800">{sheetData.metadata.explanation}</p>
                                    </div>
                                  )}

                                  {/* Non-Personal Sensitive Data Explanations */}
                                  {sheetData.metadata.non_personal_data_sensitive && (
                                    <div className="bg-orange-50 border border-orange-200 rounded-lg p-3 mb-4">
                                      <div className="flex items-center gap-2 mb-1">
                                        <Info className="w-4 h-4 text-orange-600" />
                                        <span className="font-semibold text-orange-900 text-sm">Non-Personal Sensitive Data Explanation</span>
                                      </div>
                                      <div className="space-y-2">
                                        <div className="text-sm">
                                          <span className="font-medium text-orange-800">Status: </span>
                                          <span className="text-orange-700">Non-personal sensitive data detected</span>
                                        </div>
                                        <div className="text-sm">
                                          <span className="font-medium text-orange-800">Sensitivity Level: </span>
                                          <span className="text-orange-700">{sheetData.metadata.non_personal_sensitivity || 'Unknown'}</span>
                                        </div>
                                        {sheetData.metadata.non_personal_explanation ? (
                                          <div className="text-sm">
                                            <span className="font-medium text-orange-800">Explanation: </span>
                                            <span className="text-orange-700">{sheetData.metadata.non_personal_explanation}</span>
                                          </div>
                                        ) : (
                                          <div className="text-sm text-orange-600">
                                            <span className="font-medium">Explanation: </span>
                                            <span>No explanation available in API response</span>
                                          </div>
                                        )}
                                      </div>
                                    </div>
                                  )}

                                  {/* Column Values Table */}
                                  <div className="bg-white rounded-lg border overflow-hidden">
                                    <table className="w-full">
                                      <thead className="bg-gray-50">
                                        <tr>
                                          <th className="px-3 py-2 text-left text-xs font-medium text-gray-900">Column</th>
                                          <th className="px-3 py-2 text-left text-xs font-medium text-gray-900">Sample Values</th>
                                        </tr>
                                      </thead>
                                      <tbody className="divide-y divide-gray-200">
                                        {Object.entries(sheetData.predictions).map(([columnName, prediction]) => (
                                          <tr key={columnName}>
                                            <td className="px-3 py-2 text-xs font-medium text-gray-900">{columnName}</td>
                                            <td className="px-3 py-2 text-xs text-gray-600">
                                              <div className="flex flex-wrap gap-1">
                                                {prediction.sample_values && prediction.sample_values.length > 0 ? (
                                                  <>
                                                    {prediction.sample_values.slice(0, 5).map((value, idx) => (
                                                      <span key={idx} className="bg-gray-100 px-2 py-1 rounded text-xs inline-block truncate max-w-[120px]" title={value}>
                                                        {value}
                                                      </span>
                                                    ))}
                                                    {prediction.sample_values.length > 5 && (
                                                      <span className="text-xs text-gray-500 italic">
                                                        +{prediction.sample_values.length - 5} more
                                                      </span>
                                                    )}
                                                  </>
                                                ) : (
                                                  <span className="text-gray-400">No sample values</span>
                                                )}
                                              </div>
                                            </td>
                                          </tr>
                                        ))}
                                      </tbody>
                                    </table>
                                  </div>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                )}
              </>
            ) : (
              <div className="text-center py-16">
                <FileText className="w-16 h-16 text-gray-300 mx-auto mb-4" />
                <h3 className="text-lg font-medium text-gray-900 mb-2">Select a Model</h3>
                <p className="text-gray-600">Choose a model from the sidebar to view its results</p>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
