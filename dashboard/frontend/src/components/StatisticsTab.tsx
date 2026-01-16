"use client";
import { useState, useEffect } from "react";

interface ConfusionMatrix {
  true_negative: number;
  false_positive: number;
  false_negative: number;
  true_positive: number;
}

interface Metrics {
  accuracy: number;
  precision: number;
  recall: number;
  f1: number;
  confusion_matrix: ConfusionMatrix;
  total_files?: number;
  total_sheets?: number;
  misclassifications: Array<{
    file: string;
    sheet_name?: string;
    true_label: string;
    predicted_label: string;
    error_type: string;
  }>;
  error?: string;
}

interface ModelStatistics {
  file_level: Metrics;
  sheet_level_pii: Metrics;
  sheet_level_non_pii: Metrics;
}

interface StatisticsResponse {
  models: Record<string, ModelStatistics>;
  available_models: string[];
}

export default function StatisticsTab() {
  const [statistics, setStatistics] = useState<StatisticsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [selectedModel, setSelectedModel] = useState<string | null>(null);
  const [activeView, setActiveView] = useState<'overview' | 'file' | 'sheet'>('overview');

  useEffect(() => {
    fetchStatistics();
  }, []);

  const fetchStatistics = async () => {
    setLoading(true);
    try {
      const response = await fetch("http://localhost:8000/api/statistics");
      if (response.ok) {
        const data: StatisticsResponse = await response.json();
        setStatistics(data);
        if (data.available_models.length > 0 && !selectedModel) {
          setSelectedModel(data.available_models[0]);
        }
      } else {
        console.error("Failed to fetch statistics");
      }
    } catch (error) {
      console.error("Error fetching statistics:", error);
    } finally {
      setLoading(false);
    }
  };

  const formatPercent = (value: number) => `${(value * 100).toFixed(1)}%`;
  const formatNumber = (value: number) => value.toFixed(2);

  const MetricCard = ({ title, value, subtitle }: { title: string; value: string | number; subtitle?: string }) => (
    <div className="border rounded-lg p-4 bg-white/5 border-white/20">
      <h3 className="text-sm text-white/60 mb-1">{title}</h3>
      <p className="text-2xl font-bold text-white">{value}</p>
      {subtitle && <p className="text-xs text-white/50 mt-1">{subtitle}</p>}
    </div>
  );

  const ConfusionMatrixDisplay = ({ cm, title }: { cm: ConfusionMatrix; title: string }) => (
    <div className="border rounded-lg p-4 bg-white/5">
      <h4 className="font-semibold text-white mb-3">{title}</h4>
      <div className="grid grid-cols-2 gap-2">
        <div className="bg-green-500/20 border border-green-500/50 rounded p-3">
          <div className="text-xs text-white/60 mb-1">True Negative</div>
          <div className="text-xl font-bold text-green-400">{cm.true_negative}</div>
        </div>
        <div className="bg-red-500/20 border border-red-500/50 rounded p-3">
          <div className="text-xs text-white/60 mb-1">False Positive</div>
          <div className="text-xl font-bold text-red-400">{cm.false_positive}</div>
        </div>
        <div className="bg-orange-500/20 border border-orange-500/50 rounded p-3">
          <div className="text-xs text-white/60 mb-1">False Negative</div>
          <div className="text-xl font-bold text-orange-400">{cm.false_negative}</div>
        </div>
        <div className="bg-blue-500/20 border border-blue-500/50 rounded p-3">
          <div className="text-xs text-white/60 mb-1">True Positive</div>
          <div className="text-xl font-bold text-blue-400">{cm.true_positive}</div>
        </div>
      </div>
    </div>
  );

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <div className="text-center">
          <svg
            className="animate-spin h-12 w-12 text-[#009edb] mx-auto mb-4"
            xmlns="http://www.w3.org/2000/svg"
            fill="none"
            viewBox="0 0 24 24"
          >
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path
              className="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
            />
          </svg>
          <p className="text-white/80">Loading statistics...</p>
        </div>
      </div>
    );
  }

  if (!statistics || !selectedModel || !statistics.models[selectedModel]) {
    return (
      <div className="text-sm text-white/80 border rounded-lg p-6">
        No statistics available. Make sure groundtruth2 and model predictions exist.
      </div>
    );
  }

  const modelStats = statistics.models[selectedModel];
  const hasError = modelStats.file_level.error || modelStats.sheet_level_pii.error || modelStats.sheet_level_non_pii.error;

  if (hasError) {
    return (
      <div className="text-sm text-red-400 border border-red-500/50 rounded-lg p-6 bg-red-500/10">
        Error loading statistics: {modelStats.file_level.error || modelStats.sheet_level_pii.error || modelStats.sheet_level_non_pii.error}
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header with model selector */}
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-white">Model Performance Statistics</h2>
        <select
          value={selectedModel}
          onChange={(e) => setSelectedModel(e.target.value)}
          className="px-4 py-2 rounded text-sm text-black bg-white"
        >
          {statistics.available_models.map((model) => (
            <option key={model} value={model}>
              {model}
            </option>
          ))}
        </select>
      </div>

      {/* View selector */}
      <div className="flex gap-2 border-b border-white/20">
        <button
          onClick={() => setActiveView('overview')}
          className={`px-4 py-2 font-medium transition ${
            activeView === 'overview'
              ? 'border-b-2 border-[#009edb] text-white'
              : 'text-white/60 hover:text-white/80'
          }`}
        >
          Overview
        </button>
        <button
          onClick={() => setActiveView('file')}
          className={`px-4 py-2 font-medium transition ${
            activeView === 'file'
              ? 'border-b-2 border-[#009edb] text-white'
              : 'text-white/60 hover:text-white/80'
          }`}
        >
          File-Level Metrics
        </button>
        <button
          onClick={() => setActiveView('sheet')}
          className={`px-4 py-2 font-medium transition ${
            activeView === 'sheet'
              ? 'border-b-2 border-[#009edb] text-white'
              : 'text-white/60 hover:text-white/80'
          }`}
        >
          Sheet-Level Metrics
        </button>
      </div>

      {/* Overview Tab */}
      {activeView === 'overview' && (
        <div className="space-y-6">
          {/* File-level summary */}
          <div>
            <h3 className="text-xl font-semibold text-white mb-4">File-Level Performance</h3>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
              <MetricCard
                title="Accuracy"
                value={formatPercent(modelStats.file_level.accuracy)}
                subtitle={`${modelStats.file_level.total_files || 0} files evaluated`}
              />
              <MetricCard
                title="Precision"
                value={formatPercent(modelStats.file_level.precision)}
                subtitle="Correct sensitive predictions"
              />
              <MetricCard
                title="Recall"
                value={formatPercent(modelStats.file_level.recall)}
                subtitle="Sensitive files detected"
              />
              <MetricCard
                title="F1 Score"
                value={formatNumber(modelStats.file_level.f1)}
                subtitle="Harmonic mean"
              />
            </div>
            <ConfusionMatrixDisplay cm={modelStats.file_level.confusion_matrix} title="File-Level Confusion Matrix" />
          </div>

          {/* Sheet-level summary */}
          <div className="grid md:grid-cols-2 gap-6">
            <div>
              <h3 className="text-xl font-semibold text-white mb-4">PII Sheet-Level Performance</h3>
              <div className="grid grid-cols-2 gap-4 mb-4">
                <MetricCard
                  title="Accuracy"
                  value={formatPercent(modelStats.sheet_level_pii.accuracy)}
                />
                <MetricCard
                  title="F1 Score"
                  value={formatNumber(modelStats.sheet_level_pii.f1)}
                />
              </div>
              <ConfusionMatrixDisplay cm={modelStats.sheet_level_pii.confusion_matrix} title="PII Confusion Matrix" />
            </div>

            <div>
              <h3 className="text-xl font-semibold text-white mb-4">Non-PII Sheet-Level Performance</h3>
              <div className="grid grid-cols-2 gap-4 mb-4">
                <MetricCard
                  title="Accuracy"
                  value={formatPercent(modelStats.sheet_level_non_pii.accuracy)}
                />
                <MetricCard
                  title="F1 Score"
                  value={formatNumber(modelStats.sheet_level_non_pii.f1)}
                />
              </div>
              <ConfusionMatrixDisplay cm={modelStats.sheet_level_non_pii.confusion_matrix} title="Non-PII Confusion Matrix" />
            </div>
          </div>

          {/* Model comparison chart */}
          <div>
            <h3 className="text-xl font-semibold text-white mb-4">Model Comparison</h3>
            <div className="border rounded-lg p-4 bg-white/5 overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-white/20">
                    <th className="text-left py-2 text-white/80">Model</th>
                    <th className="text-right py-2 text-white/80">File Accuracy</th>
                    <th className="text-right py-2 text-white/80">PII F1</th>
                    <th className="text-right py-2 text-white/80">Non-PII F1</th>
                  </tr>
                </thead>
                <tbody>
                  {statistics.available_models.map((model) => {
                    const stats = statistics.models[model];
                    if (stats.file_level.error) return null;
                    return (
                      <tr
                        key={model}
                        className={`border-b border-white/10 ${
                          model === selectedModel ? 'bg-[#009edb]/20' : ''
                        }`}
                      >
                        <td className="py-2 text-white font-medium">{model}</td>
                        <td className="text-right py-2 text-white">
                          {formatPercent(stats.file_level.accuracy)}
                        </td>
                        <td className="text-right py-2 text-white">
                          {formatNumber(stats.sheet_level_pii.f1)}
                        </td>
                        <td className="text-right py-2 text-white">
                          {formatNumber(stats.sheet_level_non_pii.f1)}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* File-level detailed view */}
      {activeView === 'file' && (
        <div className="space-y-6">
          <div>
            <h3 className="text-xl font-semibold text-white mb-4">File-Level Detailed Metrics</h3>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
              <MetricCard title="Accuracy" value={formatPercent(modelStats.file_level.accuracy)} />
              <MetricCard title="Precision" value={formatPercent(modelStats.file_level.precision)} />
              <MetricCard title="Recall" value={formatPercent(modelStats.file_level.recall)} />
              <MetricCard title="F1 Score" value={formatNumber(modelStats.file_level.f1)} />
            </div>
            <ConfusionMatrixDisplay cm={modelStats.file_level.confusion_matrix} title="Confusion Matrix" />
          </div>

          {/* Misclassifications */}
          {modelStats.file_level.misclassifications.length > 0 && (
            <div>
              <h3 className="text-xl font-semibold text-white mb-4">
                Misclassifications ({modelStats.file_level.misclassifications.length})
              </h3>
              <div className="border rounded-lg overflow-hidden bg-white/5">
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="bg-white/10">
                      <tr>
                        <th className="text-left py-3 px-4 text-white/80">File</th>
                        <th className="text-left py-3 px-4 text-white/80">True Label</th>
                        <th className="text-left py-3 px-4 text-white/80">Predicted Label</th>
                        <th className="text-left py-3 px-4 text-white/80">Error Type</th>
                      </tr>
                    </thead>
                    <tbody>
                      {modelStats.file_level.misclassifications.map((error, idx) => (
                        <tr
                          key={idx}
                          className={`border-t border-white/10 ${
                            error.error_type === 'False Negative'
                              ? 'bg-red-500/10'
                              : 'bg-orange-500/10'
                          }`}
                        >
                          <td className="py-2 px-4 text-white font-mono text-xs">{error.file}</td>
                          <td className="py-2 px-4 text-white">{error.true_label}</td>
                          <td className="py-2 px-4 text-white">{error.predicted_label}</td>
                          <td className="py-2 px-4">
                            <span
                              className={`px-2 py-1 rounded text-xs font-medium ${
                                error.error_type === 'False Negative'
                                  ? 'bg-red-500/30 text-red-200'
                                  : 'bg-orange-500/30 text-orange-200'
                              }`}
                            >
                              {error.error_type}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Sheet-level detailed view */}
      {activeView === 'sheet' && (
        <div className="space-y-6">
          <div className="grid md:grid-cols-2 gap-6">
            {/* PII Sheet-level */}
            <div>
              <h3 className="text-xl font-semibold text-white mb-4">PII Sheet-Level Metrics</h3>
              <div className="grid grid-cols-2 gap-4 mb-4">
                <MetricCard title="Accuracy" value={formatPercent(modelStats.sheet_level_pii.accuracy)} />
                <MetricCard title="Precision" value={formatPercent(modelStats.sheet_level_pii.precision)} />
                <MetricCard title="Recall" value={formatPercent(modelStats.sheet_level_pii.recall)} />
                <MetricCard title="F1 Score" value={formatNumber(modelStats.sheet_level_pii.f1)} />
              </div>
              <ConfusionMatrixDisplay cm={modelStats.sheet_level_pii.confusion_matrix} title="Confusion Matrix" />
              
              {modelStats.sheet_level_pii.misclassifications.length > 0 && (
                <div className="mt-4">
                  <h4 className="font-semibold text-white mb-2">
                    Misclassifications ({modelStats.sheet_level_pii.misclassifications.length})
                  </h4>
                  <div className="border rounded-lg overflow-hidden bg-white/5 max-h-64 overflow-y-auto">
                    <table className="w-full text-xs">
                      <thead className="bg-white/10 sticky top-0">
                        <tr>
                          <th className="text-left py-2 px-2 text-white/80">File</th>
                          <th className="text-left py-2 px-2 text-white/80">Sheet</th>
                          <th className="text-left py-2 px-2 text-white/80">Error</th>
                        </tr>
                      </thead>
                      <tbody>
                        {modelStats.sheet_level_pii.misclassifications.map((error, idx) => (
                          <tr key={idx} className="border-t border-white/10">
                            <td className="py-1 px-2 text-white/90 font-mono">{error.file}</td>
                            <td className="py-1 px-2 text-white/90">{error.sheet_name}</td>
                            <td className="py-1 px-2">
                              <span className={`px-1 py-0.5 rounded text-xs ${
                                error.error_type === 'False Negative' ? 'bg-red-500/30 text-red-200' : 'bg-orange-500/30 text-orange-200'
                              }`}>
                                {error.error_type}
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>

            {/* Non-PII Sheet-level */}
            <div>
              <h3 className="text-xl font-semibold text-white mb-4">Non-PII Sheet-Level Metrics</h3>
              <div className="grid grid-cols-2 gap-4 mb-4">
                <MetricCard title="Accuracy" value={formatPercent(modelStats.sheet_level_non_pii.accuracy)} />
                <MetricCard title="Precision" value={formatPercent(modelStats.sheet_level_non_pii.precision)} />
                <MetricCard title="Recall" value={formatPercent(modelStats.sheet_level_non_pii.recall)} />
                <MetricCard title="F1 Score" value={formatNumber(modelStats.sheet_level_non_pii.f1)} />
              </div>
              <ConfusionMatrixDisplay cm={modelStats.sheet_level_non_pii.confusion_matrix} title="Confusion Matrix" />
              
              {modelStats.sheet_level_non_pii.misclassifications.length > 0 && (
                <div className="mt-4">
                  <h4 className="font-semibold text-white mb-2">
                    Misclassifications ({modelStats.sheet_level_non_pii.misclassifications.length})
                  </h4>
                  <div className="border rounded-lg overflow-hidden bg-white/5 max-h-64 overflow-y-auto">
                    <table className="w-full text-xs">
                      <thead className="bg-white/10 sticky top-0">
                        <tr>
                          <th className="text-left py-2 px-2 text-white/80">File</th>
                          <th className="text-left py-2 px-2 text-white/80">Sheet</th>
                          <th className="text-left py-2 px-2 text-white/80">Error</th>
                        </tr>
                      </thead>
                      <tbody>
                        {modelStats.sheet_level_non_pii.misclassifications.map((error, idx) => (
                          <tr key={idx} className="border-t border-white/10">
                            <td className="py-1 px-2 text-white/90 font-mono">{error.file}</td>
                            <td className="py-1 px-2 text-white/90">{error.sheet_name}</td>
                            <td className="py-1 px-2">
                              <span className={`px-1 py-0.5 rounded text-xs ${
                                error.error_type === 'False Negative' ? 'bg-red-500/30 text-red-200' : 'bg-orange-500/30 text-orange-200'
                              }`}>
                                {error.error_type}
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
