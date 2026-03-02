"use client";
import { useState, useEffect } from "react";
import { generateStatisticsPDF } from "../utils/pdfGenerator";
import { getApiUrl } from "../services/api";

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
  file_level_personal_data: Metrics;
  file_level_non_personal_data: Metrics;
  sheet_level_personal_data: Metrics;
  sheet_level_non_personal_data: Metrics;
}

interface StatisticsResponse {
  models: Record<string, ModelStatistics>;
  available_models: string[];
}

export default function StatisticsTab() {
  const [statistics, setStatistics] = useState<StatisticsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [selectedModel, setSelectedModel] = useState<string | null>(null);
  const [activeView, setActiveView] = useState<'overview' | 'file' | 'sheet' | 'comparison' | 'costs'>('overview');
  const [costAnalysis, setCostAnalysis] = useState<any>(null);
  const [loadingCosts, setLoadingCosts] = useState(false);

  const fetchCostAnalysis = async () => {
    setLoadingCosts(true);
    try {
      const response = await fetch(getApiUrl("api/cost-analysis"));
      if (response.ok) {
        const data = await response.json();
        setCostAnalysis(data);
      } else {
        console.error("Failed to fetch cost analysis");
      }
    } catch (error) {
      console.error("Error fetching cost analysis:", error);
    } finally {
      setLoadingCosts(false);
    }
  };

  useEffect(() => {
    fetchStatistics();
    fetchCostAnalysis();
  }, []);

  const fetchStatistics = async () => {
    setLoading(true);
    try {
      const response = await fetch(getApiUrl("api/statistics"));
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

  const getBestMetric = (models: string[], getMetrics: (model: string) => Metrics | undefined) => {
    const validStats = models
      .map(m => getMetrics(m))
      .filter((s): s is Metrics => !!s && !s.error);
    
    if (validStats.length === 0) return { metric: 'f1' as keyof Metrics, value: 0 };

    const maxF1 = Math.max(...validStats.map(s => s.f1 || 0));
    if (maxF1 > 0) return { metric: 'f1' as keyof Metrics, value: maxF1 };

    const maxRecall = Math.max(...validStats.map(s => s.recall || 0));
    if (maxRecall > 0) return { metric: 'recall' as keyof Metrics, value: maxRecall };

    const maxPrecision = Math.max(...validStats.map(s => s.precision || 0));
    if (maxPrecision > 0) return { metric: 'precision' as keyof Metrics, value: maxPrecision };

    const maxAccuracy = Math.max(...validStats.map(s => s.accuracy || 0));
    return { metric: 'accuracy' as keyof Metrics, value: maxAccuracy };
  };

  const exportToHTML = () => {
    if (!statistics || !costAnalysis) {
      alert('Please wait for data to load before exporting');
      return;
    }

    const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Model Comparison & Cost Analysis Report</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
      background: linear-gradient(135deg, #1e3a8a 0%, #1e293b 100%);
      color: #e2e8f0;
      padding: 40px 20px;
      line-height: 1.6;
    }
    .container { max-width: 1400px; margin: 0 auto; }
    .header {
      background: linear-gradient(135deg, #009edb 0%, #0077b6 100%);
      padding: 40px;
      border-radius: 12px;
      margin-bottom: 40px;
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
    }
    .header h1 {
      font-size: 2.5rem;
      margin-bottom: 10px;
      color: white;
    }
    .header p {
      font-size: 1.1rem;
      opacity: 0.9;
      color: #e0f2fe;
    }
    .section {
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid rgba(255, 255, 255, 0.1);
      border-radius: 12px;
      padding: 30px;
      margin-bottom: 30px;
      backdrop-filter: blur(10px);
    }
    .section h2 {
      font-size: 1.8rem;
      margin-bottom: 20px;
      color: #009edb;
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .section h3 {
      font-size: 1.3rem;
      margin: 25px 0 15px 0;
      color: #60a5fa;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      margin: 20px 0;
      background: rgba(255, 255, 255, 0.03);
      border-radius: 8px;
      overflow: hidden;
    }
    thead {
      background: rgba(0, 158, 219, 0.2);
    }
    th {
      padding: 15px;
      text-align: left;
      font-weight: 600;
      color: white;
      border-bottom: 2px solid rgba(255, 255, 255, 0.1);
    }
    td {
      padding: 12px 15px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.05);
    }
    tr:hover {
      background: rgba(255, 255, 255, 0.05);
    }
    .best-badge {
      background: #10b981;
      color: white;
      padding: 4px 12px;
      border-radius: 12px;
      font-size: 0.75rem;
      font-weight: 600;
      margin-left: 8px;
    }
    .metric-value {
      font-weight: 600;
      font-family: 'Courier New', monospace;
    }
    .cost-highlight {
      color: #10b981;
      font-weight: bold;
    }
    .summary-cards {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
      gap: 20px;
      margin: 20px 0;
    }
    .card {
      background: linear-gradient(135deg, rgba(0, 158, 219, 0.1) 0%, rgba(96, 165, 250, 0.1) 100%);
      border: 1px solid rgba(0, 158, 219, 0.3);
      border-radius: 8px;
      padding: 20px;
    }
    .card-title {
      font-size: 0.9rem;
      color: #93c5fd;
      margin-bottom: 8px;
    }
    .card-value {
      font-size: 2rem;
      font-weight: bold;
      color: white;
    }
    .text-right { text-align: right; }
    .text-center { text-align: center; }
    .footer {
      text-align: center;
      margin-top: 40px;
      padding: 20px;
      color: #94a3b8;
      font-size: 0.9rem;
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>📊 Model Comparison & Cost Analysis Report</h1>
      <p>Generated on ${new Date().toLocaleString()}</p>
    </div>

    <!-- MODEL COMPARISON SECTION -->
    <div class="section">
      <h2>Model Comparison</h2>
      
      <h3>📁 Overall File-Level Performance</h3>
      <table>
        <thead>
          <tr>
            <th>Model</th>
            <th class="text-center">Accuracy</th>
            <th class="text-center">Precision</th>
            <th class="text-center">Recall</th>
            <th class="text-center">F1 Score</th>
            <th class="text-center">Files Tested</th>
          </tr>
        </thead>
        <tbody>
          ${Object.entries(statistics.models)
            .map(([modelName, stats]: [string, any]) => {
              const bestMetric = getBestMetric(statistics.available_models, (m) => statistics.models[m].file_level);
              const isBest = stats.file_level[bestMetric.metric] === bestMetric.value;
              return `
                <tr>
                  <td><strong>${modelName}</strong>${isBest ? '<span class="best-badge">Best</span>' : ''}</td>
                  <td class="text-center metric-value">${formatPercent(stats.file_level.accuracy)}</td>
                  <td class="text-center metric-value">${formatPercent(stats.file_level.precision)}</td>
                  <td class="text-center metric-value">${formatPercent(stats.file_level.recall)}</td>
                  <td class="text-center metric-value">${stats.file_level.f1.toFixed(3)}</td>
                  <td class="text-center">${stats.file_level.total_files || 0}</td>
                </tr>
              `;
            }).join('')}
        </tbody>
      </table>

      <h3>👤 File-Level Personal Sensitive Data Detection</h3>
      <table>
        <thead>
          <tr>
            <th>Model</th>
            <th class="text-center">Accuracy</th>
            <th class="text-center">Precision</th>
            <th class="text-center">Recall</th>
            <th class="text-center">F1 Score</th>
            <th class="text-center">Files Tested</th>
          </tr>
        </thead>
        <tbody>
          ${Object.entries(statistics.models)
            .map(([modelName, stats]: [string, any]) => {
              const bestMetric = getBestMetric(statistics.available_models, (m) => statistics.models[m].file_level_personal_data);
              const isBest = (stats.file_level_personal_data?.[bestMetric.metric] || 0) === bestMetric.value;
              return `
                <tr>
                  <td><strong>${modelName}</strong>${isBest ? '<span class="best-badge">Best</span>' : ''}</td>
                  <td class="text-center metric-value">${formatPercent(stats.file_level_personal_data?.accuracy || 0)}</td>
                  <td class="text-center metric-value">${formatPercent(stats.file_level_personal_data?.precision || 0)}</td>
                  <td class="text-center metric-value">${formatPercent(stats.file_level_personal_data?.recall || 0)}</td>
                  <td class="text-center metric-value">${(stats.file_level_personal_data?.f1 || 0).toFixed(3)}</td>
                  <td class="text-center">${stats.file_level_personal_data?.total_files || 0}</td>
                </tr>
              `;
            }).join('')}
        </tbody>
      </table>

      <h3>🔒 File-Level Non Personal Sensitive Data Detection</h3>
      <table>
        <thead>
          <tr>
            <th>Model</th>
            <th class="text-center">Accuracy</th>
            <th class="text-center">Precision</th>
            <th class="text-center">Recall</th>
            <th class="text-center">F1 Score</th>
            <th class="text-center">Files Tested</th>
          </tr>
        </thead>
        <tbody>
          ${Object.entries(statistics.models)
            .map(([modelName, stats]: [string, any]) => {
              const bestMetric = getBestMetric(statistics.available_models, (m) => statistics.models[m].file_level_non_personal_data);
              const isBest = (stats.file_level_non_personal_data?.[bestMetric.metric] || 0) === bestMetric.value;
              return `
                <tr>
                  <td><strong>${modelName}</strong>${isBest ? '<span class="best-badge">Best</span>' : ''}</td>
                  <td class="text-center metric-value">${formatPercent(stats.file_level_non_personal_data?.accuracy || 0)}</td>
                  <td class="text-center metric-value">${formatPercent(stats.file_level_non_personal_data?.precision || 0)}</td>
                  <td class="text-center metric-value">${formatPercent(stats.file_level_non_personal_data?.recall || 0)}</td>
                  <td class="text-center metric-value">${(stats.file_level_non_personal_data?.f1 || 0).toFixed(3)}</td>
                  <td class="text-center">${stats.file_level_non_personal_data?.total_files || 0}</td>
                </tr>
              `;
            }).join('')}
        </tbody>
      </table>

      <h3>👤 Sheet-Level Personal Sensitive Data Detection</h3>
      <table>
        <thead>
          <tr>
            <th>Model</th>
            <th class="text-center">Accuracy</th>
            <th class="text-center">Precision</th>
            <th class="text-center">Recall</th>
            <th class="text-center">F1 Score</th>
            <th class="text-center">Sheets Tested</th>
          </tr>
        </thead>
        <tbody>
          ${Object.entries(statistics.models)
            .map(([modelName, stats]: [string, any]) => {
              const bestMetric = getBestMetric(statistics.available_models, (m) => statistics.models[m].sheet_level_personal_data);
              const isBest = stats.sheet_level_personal_data[bestMetric.metric] === bestMetric.value;
              return `
                <tr>
                  <td><strong>${modelName}</strong>${isBest ? '<span class="best-badge">Best</span>' : ''}</td>
                  <td class="text-center metric-value">${formatPercent(stats.sheet_level_personal_data.accuracy)}</td>
                  <td class="text-center metric-value">${formatPercent(stats.sheet_level_personal_data.precision)}</td>
                  <td class="text-center metric-value">${formatPercent(stats.sheet_level_personal_data.recall)}</td>
                  <td class="text-center metric-value">${stats.sheet_level_personal_data.f1.toFixed(3)}</td>
                  <td class="text-center">${stats.sheet_level_personal_data.total_sheets || 0}</td>
                </tr>
              `;
            }).join('')}
        </tbody>
      </table>

      <h3>🔒 Sheet-Level Non Personal Sensitive Data Detection</h3>
      <table>
        <thead>
          <tr>
            <th>Model</th>
            <th class="text-center">Accuracy</th>
            <th class="text-center">Precision</th>
            <th class="text-center">Recall</th>
            <th class="text-center">F1 Score</th>
            <th class="text-center">Sheets Tested</th>
          </tr>
        </thead>
        <tbody>
          ${Object.entries(statistics.models)
            .map(([modelName, stats]: [string, any]) => {
              const bestMetric = getBestMetric(statistics.available_models, (m) => statistics.models[m].sheet_level_non_personal_data);
              const isBest = stats.sheet_level_non_personal_data[bestMetric.metric] === bestMetric.value;
              return `
                <tr>
                  <td><strong>${modelName}</strong>${isBest ? '<span class="best-badge">Best</span>' : ''}</td>
                  <td class="text-center metric-value">${formatPercent(stats.sheet_level_non_personal_data.accuracy)}</td>
                  <td class="text-center metric-value">${formatPercent(stats.sheet_level_non_personal_data.precision)}</td>
                  <td class="text-center metric-value">${formatPercent(stats.sheet_level_non_personal_data.recall)}</td>
                  <td class="text-center metric-value">${stats.sheet_level_non_personal_data.f1.toFixed(3)}</td>
                  <td class="text-center">${stats.sheet_level_non_personal_data.total_sheets || 0}</td>
                </tr>
              `;
            }).join('')}
        </tbody>
      </table>
    </div>

    <!-- COST ANALYSIS SECTION -->
    <div class="section">
      <h2>Cost Analysis</h2>
      
      <div class="summary-cards">
        <div class="card">
          <div class="card-title">Total Models</div>
          <div class="card-value">${Object.keys(costAnalysis.models || {}).length}</div>
        </div>
        <div class="card">
          <div class="card-title">Total Tokens</div>
          <div class="card-value">${Object.values(costAnalysis.models || {}).reduce((sum: number, model: any) => sum + model.total_tokens, 0).toLocaleString()}</div>
        </div>
        <div class="card">
          <div class="card-title">Total Cost</div>
          <div class="card-value cost-highlight">$${Object.values(costAnalysis.models || {}).reduce((sum: number, model: any) => sum + model.total_cost_usd, 0).toFixed(4)}</div>
        </div>
      </div>

      <h3>Detailed Cost Breakdown</h3>
      <table>
        <thead>
          <tr>
            <th>Model</th>
            <th class="text-center">Reports</th>
            <th class="text-right">Prompt Tokens</th>
            <th class="text-right">Completion Tokens</th>
            <th class="text-right">Total Tokens</th>
            <th class="text-right">Price/1M</th>
            <th class="text-right">Total Cost</th>
            <th class="text-right">Cost/Report</th>
          </tr>
        </thead>
        <tbody>
          ${Object.entries(costAnalysis.models || {})
            .sort(([, a]: [string, any], [, b]: [string, any]) => b.total_cost_usd - a.total_cost_usd)
            .map(([modelName, data]: [string, any]) => `
              <tr>
                <td><strong>${modelName}</strong></td>
                <td class="text-center">${data.reports_processed}${data.reports_with_errors > 0 ? ' ⚠️' : ''}</td>
                <td class="text-right metric-value">${data.prompt_tokens.toLocaleString()}</td>
                <td class="text-right metric-value">${data.completion_tokens.toLocaleString()}</td>
                <td class="text-right metric-value"><strong>${data.total_tokens.toLocaleString()}</strong></td>
                <td class="text-right metric-value">$${data.price_per_million.toFixed(2)}</td>
                <td class="text-right cost-highlight metric-value">$${data.total_cost_usd.toFixed(4)}</td>
                <td class="text-right metric-value">$${data.cost_per_report.toFixed(4)}</td>
              </tr>
            `).join('')}
        </tbody>
      </table>

      <h3>Pricing Reference</h3>
      <table>
        <thead>
          <tr>
            <th>Model</th>
            <th class="text-right">Price per 1M Tokens</th>
          </tr>
        </thead>
        <tbody>
          ${Object.entries(costAnalysis.pricing || {}).map(([model, price]: [string, any]) => `
            <tr>
              <td>${model}</td>
              <td class="text-right metric-value">$${price.toFixed(2)}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
      <p style="margin-top: 15px; font-size: 0.9rem; color: #94a3b8; font-style: italic;">
        * Prices are per 1 million tokens (both prompt and completion tokens use the same rate)
      </p>
    </div>

    <div class="footer">
      <p>HDX Sensitive Data Detection Pipeline - Model Performance Report</p>
      <p>Generated automatically from test results</p>
    </div>
  </div>
</body>
</html>`;

    // Create and download the HTML file
    const blob = new Blob([html], { type: 'text/html' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `model-comparison-cost-analysis-${new Date().toISOString().split('T')[0]}.html`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };


  const MetricCard = ({ title, value, subtitle }: { title: string; value: string | number; subtitle?: string }) => (
    <div className="border rounded-lg p-4 bg-white/5 border-white/20">
      <h3 className="text-sm text-white/60 mb-1">{title}</h3>
      <p className="text-2xl font-bold text-white">{value}</p>
      {subtitle && <p className="text-xs text-white/50 mt-1">{subtitle}</p>}
    </div>
  );

  const ConfusionMatrixDisplay = ({ cm, title }: { cm: ConfusionMatrix; title: string }) => (
    <div className="border border-white/20 rounded-lg p-4 bg-white/5">
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
  const hasError = modelStats.file_level.error || modelStats.sheet_level_personal_data.error || modelStats.sheet_level_non_personal_data.error;

  if (hasError) {
    return (
      <div className="text-sm text-red-400 border border-red-500/50 rounded-lg p-6 bg-red-500/10">
        Error loading statistics: {modelStats.file_level.error || modelStats.sheet_level_personal_data.error || modelStats.sheet_level_non_personal_data.error}
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header with model selector */}
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-white">Model Performance Statistics</h2>
        <div className="flex items-center gap-3">
          <button
            onClick={() => generateStatisticsPDF(statistics, selectedModel)}
            className="px-4 py-2 rounded text-sm font-medium bg-[#009edb] text-white hover:bg-[#007ab8] transition-colors flex items-center gap-2"
          >
            <svg 
              xmlns="http://www.w3.org/2000/svg" 
              className="h-5 w-5" 
              fill="none" 
              viewBox="0 0 24 24" 
              stroke="currentColor"
            >
              <path 
                strokeLinecap="round" 
                strokeLinejoin="round" 
                strokeWidth={2} 
                d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" 
              />
            </svg>
            Generate PDF Report
          </button>
          <button
            onClick={exportToHTML}
            className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700 transition text-sm"
          >
            <svg 
              xmlns="http://www.w3.org/2000/svg" 
              className="h-5 w-5" 
              fill="none" 
              viewBox="0 0 24 24" 
              stroke="currentColor"
            >
              <path 
                strokeLinecap="round" 
                strokeLinejoin="round" 
                strokeWidth={2} 
                d="M8 7H5a2 2 0 00-2 2v9a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-3m-1 4l-3 3m0 0l-3-3m3 3V4" 
              />
            </svg>
            Save as HTML
          </button>
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
        <button
          onClick={() => setActiveView('comparison')}
          className={`px-4 py-2 font-medium transition ${
            activeView === 'comparison'
              ? 'border-b-2 border-[#009edb] text-white'
              : 'text-white/60 hover:text-white/80'
          }`}
        >
          Model Comparison
        </button>
        <button
          onClick={() => setActiveView('costs')}
          className={`px-4 py-2 font-medium transition ${
            activeView === 'costs'
              ? 'border-b-2 border-[#009edb] text-white'
              : 'text-white/60 hover:text-white/80'
          }`}
        >
          Cost Analysis
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
              <h3 className="text-xl font-semibold text-white mb-4">Personal Sensitive Data Sheet-Level Performance</h3>
              <div className="grid grid-cols-2 gap-4 mb-4">
                <MetricCard
                  title="Accuracy"
                  value={formatPercent(modelStats.sheet_level_personal_data.accuracy)}
                />
                <MetricCard
                  title="F1 Score"
                  value={formatNumber(modelStats.sheet_level_personal_data.f1)}
                />
              </div>
              <ConfusionMatrixDisplay cm={modelStats.sheet_level_personal_data.confusion_matrix} title="Personal Sensitive Data Confusion Matrix" />
            </div>

            <div>
              <h3 className="text-xl font-semibold text-white mb-4">Non-Personal Sensitive Data Sheet-Level Performance</h3>
              <div className="grid grid-cols-2 gap-4 mb-4">
                <MetricCard
                  title="Accuracy"
                  value={formatPercent(modelStats.sheet_level_non_personal_data.accuracy)}
                />
                <MetricCard
                  title="F1 Score"
                  value={formatNumber(modelStats.sheet_level_non_personal_data.f1)}
                />
              </div>
              <ConfusionMatrixDisplay cm={modelStats.sheet_level_non_personal_data.confusion_matrix} title="Non-Personal Sensitive Data Confusion Matrix" />
            </div>
          </div>

          {/* Model comparison chart */}
          <div>
            <h3 className="text-xl font-semibold text-white mb-4">Model Comparison</h3>
            <div className="border border-white/20 rounded-lg p-4 bg-white/5 overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-white/20">
                    <th className="text-left py-2 text-white/80">Model</th>
                    <th className="text-right py-2 text-white/80">File Accuracy</th>
                    <th className="text-right py-2 text-white/80">PSD F1</th>
                    <th className="text-right py-2 text-white/80">Non-PSD F1</th>
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
                          {formatNumber(stats.sheet_level_personal_data.f1)}
                        </td>
                        <td className="text-right py-2 text-white">
                          {formatNumber(stats.sheet_level_non_personal_data.f1)}
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
              <div className="border border-white/20 rounded-lg overflow-hidden bg-white/5">
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

          {/* File-Level Personal Sensitive Data Metrics */}
          <div>
            <h3 className="text-xl font-semibold text-white mb-4">File-Level Personal Sensitive Data Detection</h3>
            <p className="text-sm text-white/70 mb-4">
              Detects whether files contain Personal Sensitive Data. 
              A file is marked as having Personal Sensitive Data if any sheet contains Personal Sensitive Data.
            </p>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
              <MetricCard title="Accuracy" value={formatPercent(modelStats.file_level_personal_data?.accuracy || 0)} />
              <MetricCard title="Precision" value={formatPercent(modelStats.file_level_personal_data?.precision || 0)} />
              <MetricCard title="Recall" value={formatPercent(modelStats.file_level_personal_data?.recall || 0)} />
              <MetricCard title="F1 Score" value={formatNumber(modelStats.file_level_personal_data?.f1 || 0)} />
            </div>
            {modelStats.file_level_personal_data?.confusion_matrix && (
              <ConfusionMatrixDisplay 
                cm={modelStats.file_level_personal_data.confusion_matrix} 
                title="Personal Sensitive Data Detection Confusion Matrix" 
              />
            )}

            {/* Personal Sensitive Data Misclassifications */}
            {modelStats.file_level_personal_data?.misclassifications && modelStats.file_level_personal_data.misclassifications.length > 0 && (
              <div className="mt-4">
                <h4 className="text-lg font-semibold text-white mb-3">
                  Personal Sensitive Data Misclassifications ({modelStats.file_level_personal_data.misclassifications.length})
                </h4>
                <div className="border border-white/20 rounded-lg overflow-hidden bg-white/5">
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
                        {modelStats.file_level_personal_data.misclassifications.map((error, idx) => (
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

          {/* File-Level Non Personal Sensitive Data Metrics */}
          <div>
            <h3 className="text-xl font-semibold text-white mb-4">File-Level Non Personal Sensitive Data Detection</h3>
            <p className="text-sm text-white/70 mb-4">
              Detects whether files contain non personal sensitive data (confidential business info, financial data, etc.). 
              A file is marked as having non personal sensitive data if any sheet contains such data.
            </p>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
              <MetricCard title="Accuracy" value={formatPercent(modelStats.file_level_non_personal_data?.accuracy || 0)} />
              <MetricCard title="Precision" value={formatPercent(modelStats.file_level_non_personal_data?.precision || 0)} />
              <MetricCard title="Recall" value={formatPercent(modelStats.file_level_non_personal_data?.recall || 0)} />
              <MetricCard title="F1 Score" value={formatNumber(modelStats.file_level_non_personal_data?.f1 || 0)} />
            </div>
            {modelStats.file_level_non_personal_data?.confusion_matrix && (
              <ConfusionMatrixDisplay 
                cm={modelStats.file_level_non_personal_data.confusion_matrix} 
                title="Non Personal Sensitive Data Detection Confusion Matrix" 
              />
            )}

            {/* Non-Personal Sensitive Data Misclassifications */}
            {modelStats.file_level_non_personal_data?.misclassifications && modelStats.file_level_non_personal_data.misclassifications.length > 0 && (
              <div className="mt-4">
                <h4 className="text-lg font-semibold text-white mb-3">
                  Non-Personal Sensitive Data Misclassifications ({modelStats.file_level_non_personal_data.misclassifications.length})
                </h4>
                <div className="border border-white/20 rounded-lg overflow-hidden bg-white/5">
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
                        {modelStats.file_level_non_personal_data.misclassifications.map((error, idx) => (
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
        </div>
      )}

      {/* Sheet-level detailed view */}
      {activeView === 'sheet' && (
        <div className="space-y-6">
          <div className="grid md:grid-cols-2 gap-6">
            {/* Personal Sensitive Data Sheet-level */}
            <div>
              <h3 className="text-xl font-semibold text-white mb-4">Personal Sensitive Data Sheet-Level Metrics</h3>
              <div className="grid grid-cols-2 gap-4 mb-4">
                <MetricCard title="Accuracy" value={formatPercent(modelStats.sheet_level_personal_data.accuracy)} />
                <MetricCard title="Precision" value={formatPercent(modelStats.sheet_level_personal_data.precision)} />
                <MetricCard title="Recall" value={formatPercent(modelStats.sheet_level_personal_data.recall)} />
                <MetricCard title="F1 Score" value={formatNumber(modelStats.sheet_level_personal_data.f1)} />
              </div>
              <ConfusionMatrixDisplay cm={modelStats.sheet_level_personal_data.confusion_matrix} title="Confusion Matrix" />
              
              {modelStats.sheet_level_personal_data.misclassifications.length > 0 && (
                <div className="mt-4">
                  <h4 className="font-semibold text-white mb-2">
                    Misclassifications ({modelStats.sheet_level_personal_data.misclassifications.length})
                  </h4>
                  <div className="border border-white/20 rounded-lg overflow-hidden bg-white/5 max-h-64 overflow-y-auto">
                    <table className="w-full text-xs">
                      <thead className="bg-white/10 sticky top-0">
                        <tr>
                          <th className="text-left py-2 px-2 text-white/80">File</th>
                          <th className="text-left py-2 px-2 text-white/80">Sheet</th>
                          <th className="text-left py-2 px-2 text-white/80">Error</th>
                        </tr>
                      </thead>
                      <tbody>
                        {modelStats.sheet_level_personal_data.misclassifications.map((error, idx) => (
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

            {/* Non Personal Sensitive Data Sheet-level */}
            <div>
              <h3 className="text-xl font-semibold text-white mb-4">Non-Personal Sensitive Data Sheet-Level Metrics</h3>
              <div className="grid grid-cols-2 gap-4 mb-4">
                <MetricCard title="Accuracy" value={formatPercent(modelStats.sheet_level_non_personal_data.accuracy)} />
                <MetricCard title="Precision" value={formatPercent(modelStats.sheet_level_non_personal_data.precision)} />
                <MetricCard title="Recall" value={formatPercent(modelStats.sheet_level_non_personal_data.recall)} />
                <MetricCard title="F1 Score" value={formatNumber(modelStats.sheet_level_non_personal_data.f1)} />
              </div>
              <ConfusionMatrixDisplay cm={modelStats.sheet_level_non_personal_data.confusion_matrix} title="Confusion Matrix" />
              
              {modelStats.sheet_level_non_personal_data.misclassifications.length > 0 && (
                <div className="mt-4">
                  <h4 className="font-semibold text-white mb-2">
                    Misclassifications ({modelStats.sheet_level_non_personal_data.misclassifications.length})
                  </h4>
                  <div className="border border-white/20 rounded-lg overflow-hidden bg-white/5 max-h-64 overflow-y-auto">
                    <table className="w-full text-xs">
                      <thead className="bg-white/10 sticky top-0">
                        <tr>
                          <th className="text-left py-2 px-2 text-white/80">File</th>
                          <th className="text-left py-2 px-2 text-white/80">Sheet</th>
                          <th className="text-left py-2 px-2 text-white/80">Error</th>
                        </tr>
                      </thead>
                      <tbody>
                        {modelStats.sheet_level_non_personal_data.misclassifications.map((error, idx) => (
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

      {/* Model Comparison Tab */}
      {activeView === 'comparison' && (
        <div className="space-y-8">
          {/* Introduction */}
          <div className="bg-gradient-to-r from-[#009edb]/10 to-blue-500/10 border border-[#009edb]/30 rounded-lg p-6">
            <h3 className="text-xl font-bold text-white mb-4 flex items-center gap-2">
              <svg className="w-6 h-6 text-[#009edb]" fill="currentColor" viewBox="0 0 20 20">
                <path d="M9 2a1 1 0 000 2h2a1 1 0 100-2H9z" />
                <path fillRule="evenodd" d="M4 5a2 2 0 012-2 3 3 0 003 3h2a3 3 0 003-3 2 2 0 012 2v11a2 2 0 01-2 2H6a2 2 0 01-2-2V5zm3 4a1 1 0 000 2h.01a1 1 0 100-2H7zm3 0a1 1 0 000 2h3a1 1 0 100-2h-3zm-3 4a1 1 0 100 2h.01a1 1 0 100-2H7zm3 0a1 1 0 100 2h3a1 1 0 100-2h-3z" clipRule="evenodd" />
              </svg>
              Understanding Model Comparison
            </h3>
            
            <div className="space-y-4 text-sm text-white/90 leading-relaxed">
              <p className="text-base">
                This page helps you compare different AI models to see which one is best at detecting sensitive data. 
                Think of it like comparing different security guards - some might be better at spotting certain types of threats than others.
              </p>
              
              <div className="bg-white/5 rounded-lg p-4 border border-white/10">
                <h4 className="font-semibold text-white mb-2">🎯 What We're Testing</h4>
                <p>
                  We test each AI model on the same set of real humanitarian datasets to see how well they can identify:
                </p>
                <ul className="mt-2 space-y-1 ml-4">
                  <li>• <strong>Personal Sensitive Data</strong> - names, emails, phone numbers, addresses</li>
                  <li>• <strong>Non Personal Sensitive Data</strong> - sensitive infromation linked to ISPs or other non personal data</li>
                  <li>• <strong>Overall Sensitivity</strong> - any data that shouldn't be shared publicly</li>
                </ul>
              </div>


              <div className="bg-amber-500/10 border border-amber-500/30 rounded-lg p-4">
                <h4 className="font-semibold text-amber-200 mb-2 flex items-center gap-2">
                  <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
                    <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
                  </svg>
                  What Matters Most?
                </h4>
                <p className="text-amber-100/90 text-xs">
                  For humanitarian data, <strong>high recall</strong> is critical - we'd rather have a few false alarms than miss sensitive information 
                  that could put people at risk. A model with 90% recall and 80% precision is often better than one with 90% precision and 80% recall.
                </p>
              </div>

              <div className="flex items-start gap-3 bg-green-500/10 border border-green-500/30 rounded-lg p-4">
                <span className="text-2xl">💡</span>
                <div>
                  <h4 className="font-semibold text-green-200 mb-1">Look for the "Best" Badge</h4>
                  <p className="text-green-100/90 text-xs">
                    The model with the highest F1 score in each category gets a green "Best" badge. 
                    This is your top performer for that specific type of detection.
                  </p>
                </div>
              </div>
            </div>
          </div>

          {/* File-Level Performance */}
          <div>
            <div className="mb-4">
              <h3 className="text-xl font-bold text-white mb-3">📁 Overall File-Level Performance</h3>
              <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-4 mb-3">
                <h4 className="font-semibold text-blue-200 mb-2">What This Measures</h4>
                <p className="text-sm text-blue-100/90 leading-relaxed">
                  This shows how well each model can determine if an <strong>entire file</strong> contains any sensitive information. 
                  A file is marked as "sensitive" if it contains <em>either</em> personal sensitive data <em>or</em> non personal sensitive data.
                </p>
                <p className="text-sm text-blue-100/90 mt-2 leading-relaxed">
                  <strong>Real-world example:</strong> If a spreadsheet has 10 sheets and just 1 sheet contains phone numbers, 
                  the entire file should be flagged as sensitive.
                </p>
              </div>
              <div className="text-xs text-white/60 italic">
                💡 Tip: This is usually the first check - if a file is marked as not sensitive here, it's safe to share publicly.
              </div>
            </div>
            
            <div className="border border-white/20 rounded-lg overflow-hidden bg-white/5">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-[#009edb]/20 border-b border-white/20">
                    <tr>
                      <th className="text-left py-3 px-4 text-white font-semibold">Model</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Accuracy</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Precision</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Recall</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">F1 Score</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Files Tested</th>
                    </tr>
                  </thead>
                  <tbody>
                    {statistics.available_models.map((model, idx) => {
                      const stats = statistics.models[model];
                      if (stats.file_level.error) return null;
                      
                      const bestMetric = getBestMetric(statistics.available_models, (m) => statistics.models[m].file_level);
                      const isBest = stats.file_level[bestMetric.metric] === bestMetric.value;
                      
                      return (
                        <tr
                          key={model}
                          className={`border-b border-white/10 hover:bg-white/5 transition ${
                            isBest ? 'bg-green-500/10' : ''
                          }`}
                        >
                          <td className="py-3 px-4 text-white font-medium">
                            {model}
                            {isBest && <span className="ml-2 text-xs bg-green-500/30 text-green-200 px-2 py-0.5 rounded">Best ({bestMetric.metric})</span>}
                          </td>
                          <td className="text-center py-3 px-3 text-white">
                            {formatPercent(stats.file_level.accuracy)}
                          </td>
                          <td className="text-center py-3 px-3 text-white">
                            {formatPercent(stats.file_level.precision)}
                          </td>
                          <td className="text-center py-3 px-3 text-white">
                            {formatPercent(stats.file_level.recall)}
                          </td>
                          <td className="text-center py-3 px-3 text-white font-semibold">
                            {stats.file_level.f1.toFixed(3)}
                          </td>
                          <td className="text-center py-3 px-3 text-white/70">
                            {stats.file_level.total_files || 0}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          {/* File-Level Personal Sensitive Data Performance */}
          <div>
            <div className="mb-4">
              <h3 className="text-xl font-bold text-white mb-3">👤 File-Level Personal Sensitive Data Detection</h3>
              <div className="bg-purple-500/10 border border-purple-500/30 rounded-lg p-4 mb-3">
                <h4 className="font-semibold text-purple-200 mb-2">What This Measures</h4>
                <p className="text-sm text-purple-100/90 leading-relaxed">
                  This focuses specifically on <strong>Personally Sensitive Data</strong> - data that can identify specific individuals.
                </p>
                <div className="mt-2 text-sm text-purple-100/90">
                  <strong>PSD includes:</strong>
                  <ul className="mt-1 ml-4 space-y-0.5 text-xs">
                    <li>• Names, email addresses, phone numbers</li>
                    <li>• ID numbers, passport numbers, social security numbers</li>
                    <li>• Physical addresses, GPS coordinates of homes</li>
                    <li>• Biometric data, photos of identifiable people</li>
                  </ul>
                </div>
                <p className="text-sm text-purple-100/90 mt-2 leading-relaxed">
                  <strong>Why it matters:</strong> Exposing Personal Sensitive Data can lead to identity theft, harassment, or put vulnerable populations at risk.
                </p>
              </div>
              <div className="text-xs text-white/60 italic">
                ⚠️ High recall is critical here - missing Personal Sensitive Data could endanger individuals.
              </div>
            </div>
            
            <div className="border border-white/20 rounded-lg overflow-hidden bg-white/5">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-[#009edb]/20 border-b border-white/20">
                    <tr>
                      <th className="text-left py-3 px-4 text-white font-semibold">Model</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Accuracy</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Precision</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Recall</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">F1 Score</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Files Tested</th>
                    </tr>
                  </thead>
                  <tbody>
                    {statistics.available_models.map((model, idx) => {
                      const stats = statistics.models[model];
                      if (stats.file_level_personal_data?.error) return null;
                      
                      const bestMetric = getBestMetric(statistics.available_models, (m) => statistics.models[m].file_level_personal_data);
                      const isBest = (stats.file_level_personal_data?.[bestMetric.metric] || 0) === bestMetric.value;
                      
                      return (
                        <tr
                          key={model}
                          className={`border-b border-white/10 hover:bg-white/5 transition ${
                            isBest ? 'bg-green-500/10' : ''
                          }`}
                        >
                          <td className="py-3 px-4 text-white font-medium">
                            {model}
                            {isBest && <span className="ml-2 text-xs bg-green-500/30 text-green-200 px-2 py-0.5 rounded">Best ({bestMetric.metric})</span>}
                          </td>
                          <td className="text-center py-3 px-3 text-white">
                            {formatPercent(stats.file_level_personal_data?.accuracy || 0)}
                          </td>
                          <td className="text-center py-3 px-3 text-white">
                            {formatPercent(stats.file_level_personal_data?.precision || 0)}
                          </td>
                          <td className="text-center py-3 px-3 text-white">
                            {formatPercent(stats.file_level_personal_data?.recall || 0)}
                          </td>
                          <td className="text-center py-3 px-3 text-white font-semibold">
                            {(stats.file_level_personal_data?.f1 || 0).toFixed(3)}
                          </td>
                          <td className="text-center py-3 px-3 text-white/70">
                            {stats.file_level_personal_data?.total_files || 0}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          {/* File-Level Non Personal Sensitive Data Performance */}
          <div>
            <div className="mb-4">
              <h3 className="text-xl font-bold text-white mb-3">🔒 File-Level Non Personal Sensitive Data Detection</h3>
              <div className="bg-orange-500/10 border border-orange-500/30 rounded-lg p-4 mb-3">
                <h4 className="font-semibold text-orange-200 mb-2">What This Measures</h4>
                <p className="text-sm text-orange-100/90 leading-relaxed">
                  This detects <strong>confidential data that isn't personal information</strong> but still shouldn't be shared publicly.
                </p>
                <div className="mt-2 text-sm text-orange-100/90">
                  <strong>Non Personal Sensitive Data includes:</strong>
                  <ul className="mt-1 ml-4 space-y-0.5 text-xs">
                    <li>• Security information (access codes, facility locations)</li>
                    <li>• Operational details (staff numbers, warehouse locations)</li>
                  </ul>
                </div>
                <p className="text-sm text-orange-100/90 mt-2 leading-relaxed">
                  <strong>Why it matters:</strong> This data could cause harm if exposed.
                </p>
              </div>
            </div>
            
            <div className="border border-white/20 rounded-lg overflow-hidden bg-white/5">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-[#009edb]/20 border-b border-white/20">
                    <tr>
                      <th className="text-left py-3 px-4 text-white font-semibold">Model</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Accuracy</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Precision</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Recall</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">F1 Score</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Files Tested</th>
                    </tr>
                  </thead>
                  <tbody>
                    {statistics.available_models.map((model, idx) => {
                      const stats = statistics.models[model];
                      if (stats.file_level_non_personal_data?.error) return null;
                      
                      const bestMetric = getBestMetric(statistics.available_models, (m) => statistics.models[m].file_level_non_personal_data);
                      const isBest = (stats.file_level_non_personal_data?.[bestMetric.metric] || 0) === bestMetric.value;
                      
                      return (
                        <tr
                          key={model}
                          className={`border-b border-white/10 hover:bg-white/5 transition ${
                            isBest ? 'bg-green-500/10' : ''
                          }`}
                        >
                          <td className="py-3 px-4 text-white font-medium">
                            {model}
                            {isBest && <span className="ml-2 text-xs bg-green-500/30 text-green-200 px-2 py-0.5 rounded">Best ({bestMetric.metric})</span>}
                          </td>
                          <td className="text-center py-3 px-3 text-white">
                            {formatPercent(stats.file_level_non_personal_data?.accuracy || 0)}
                          </td>
                          <td className="text-center py-3 px-3 text-white">
                            {formatPercent(stats.file_level_non_personal_data?.precision || 0)}
                          </td>
                          <td className="text-center py-3 px-3 text-white">
                            {formatPercent(stats.file_level_non_personal_data?.recall || 0)}
                          </td>
                          <td className="text-center py-3 px-3 text-white font-semibold">
                            {(stats.file_level_non_personal_data?.f1 || 0).toFixed(3)}
                          </td>
                          <td className="text-center py-3 px-3 text-white/70">
                            {stats.file_level_non_personal_data?.total_files || 0}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          {/* Sheet-Level Personal Sensitive Data Performance */}
          <div>
            <div className="mb-4">
              <h3 className="text-xl font-bold text-white mb-3">👤 Sheet-Level Personal Sensitive Data Detection</h3>
              <div className="bg-indigo-500/10 border border-indigo-500/30 rounded-lg p-4 mb-3">
                <h4 className="font-semibold text-indigo-200 mb-2">What This Measures</h4>
                <p className="text-sm text-indigo-100/90 leading-relaxed">
                  This measures Personal Sensitive Data detection at a <strong>more granular level</strong> - looking at individual sheets within files, 
                  rather than the entire file.
                </p>
                <p className="text-sm text-indigo-100/90 mt-2 leading-relaxed">
                  <strong>Why this matters:</strong> A file might have 10 sheets, but only 2 contain Personal Sensitive Data. Sheet-level detection 
                  helps you understand exactly which parts of your data are sensitive, allowing for more targeted protection.
                </p>
                <p className="text-sm text-indigo-100/90 mt-2 leading-relaxed">
                  <strong>Example:</strong> An Excel file with "Summary" (no Personal Sensitive Data), "Contact List" (has Personal Sensitive Data), and "Statistics" (no Personal Sensitive Data) sheets.
                </p>
              </div>
              <div className="text-xs text-white/60 italic">
                🔍 More precise than file-level - helps identify exactly where sensitive data lives.
              </div>
            </div>
            
            <div className="border border-white/20 rounded-lg overflow-hidden bg-white/5">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-[#009edb]/20 border-b border-white/20">
                    <tr>
                      <th className="text-left py-3 px-4 text-white font-semibold">Model</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Accuracy</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Precision</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Recall</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">F1 Score</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Sheets Tested</th>
                    </tr>
                  </thead>
                  <tbody>
                    {statistics.available_models.map((model, idx) => {
                      const stats = statistics.models[model];
                      if (stats.sheet_level_personal_data.error) return null;
                      
                      const bestMetric = getBestMetric(statistics.available_models, (m) => statistics.models[m].sheet_level_personal_data);
                      const isBest = stats.sheet_level_personal_data[bestMetric.metric] === bestMetric.value;
                      
                      return (
                        <tr
                          key={model}
                          className={`border-b border-white/10 hover:bg-white/5 transition ${
                            isBest ? 'bg-green-500/10' : ''
                          }`}
                        >
                          <td className="py-3 px-4 text-white font-medium">
                            {model}
                            {isBest && <span className="ml-2 text-xs bg-green-500/30 text-green-200 px-2 py-0.5 rounded">Best ({bestMetric.metric})</span>}
                          </td>
                          <td className="text-center py-3 px-3 text-white">
                            {formatPercent(stats.sheet_level_personal_data.accuracy)}
                          </td>
                          <td className="text-center py-3 px-3 text-white">
                            {formatPercent(stats.sheet_level_personal_data.precision)}
                          </td>
                          <td className="text-center py-3 px-3 text-white">
                            {formatPercent(stats.sheet_level_personal_data.recall)}
                          </td>
                          <td className="text-center py-3 px-3 text-white font-semibold">
                            {stats.sheet_level_personal_data.f1.toFixed(3)}
                          </td>
                          <td className="text-center py-3 px-3 text-white/70">
                            {stats.sheet_level_personal_data.total_sheets || 0}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          {/* Sheet-Level Non Personal Sensitive Data Performance */}
          <div>
            <div className="mb-4">
              <h3 className="text-xl font-bold text-white mb-3">🔒 Sheet-Level Non Personal Sensitive Data Detection</h3>
              <div className="bg-teal-500/10 border border-teal-500/30 rounded-lg p-4 mb-3">
                <h4 className="font-semibold text-teal-200 mb-2">What This Measures</h4>
                <p className="text-sm text-teal-100/90 leading-relaxed">
                  This measures non personal sensitive data detection at the <strong>individual sheet level</strong>, 
                  identifying which specific sheets contain confidential information.
                </p>
                <p className="text-sm text-teal-100/90 mt-2 leading-relaxed">
                  <strong>Example:</strong> A budget file where "Public Summary" can be shared, but "Detailed Expenses" and 
                  "Partner Funding" sheets contain confidential financial information.
                </p>
              </div>
              <div className="text-xs text-white/60 italic">
                🎯 Enables selective sharing - share safe sheets while protecting sensitive ones.
              </div>
            </div>
            
            <div className="border border-white/20 rounded-lg overflow-hidden bg-white/5">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-[#009edb]/20 border-b border-white/20">
                    <tr>
                      <th className="text-left py-3 px-4 text-white font-semibold">Model</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Accuracy</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Precision</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Recall</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">F1 Score</th>
                      <th className="text-center py-3 px-3 text-white font-semibold">Sheets Tested</th>
                    </tr>
                  </thead>
                  <tbody>
                    {statistics.available_models.map((model, idx) => {
                      const stats = statistics.models[model];
                      if (stats.sheet_level_non_personal_data.error) return null;
                      
                      const bestMetric = getBestMetric(statistics.available_models, (m) => statistics.models[m].sheet_level_non_personal_data);
                      const isBest = stats.sheet_level_non_personal_data[bestMetric.metric] === bestMetric.value;
                      
                      return (
                        <tr
                          key={model}
                          className={`border-b border-white/10 hover:bg-white/5 transition ${
                            isBest ? 'bg-green-500/10' : ''
                          }`}
                        >
                          <td className="py-3 px-4 text-white font-medium">
                            {model}
                            {isBest && <span className="ml-2 text-xs bg-green-500/30 text-green-200 px-2 py-0.5 rounded">Best ({bestMetric.metric})</span>}
                          </td>
                          <td className="text-center py-3 px-3 text-white">
                            {formatPercent(stats.sheet_level_non_personal_data.accuracy)}
                          </td>
                          <td className="text-center py-3 px-3 text-white">
                            {formatPercent(stats.sheet_level_non_personal_data.precision)}
                          </td>
                          <td className="text-center py-3 px-3 text-white">
                            {formatPercent(stats.sheet_level_non_personal_data.recall)}
                          </td>
                          <td className="text-center py-3 px-3 text-white font-semibold">
                            {stats.sheet_level_non_personal_data.f1.toFixed(3)}
                          </td>
                          <td className="text-center py-3 px-3 text-white/70">
                            {stats.sheet_level_non_personal_data.total_sheets || 0}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          {/* Key Insights */}
          <div className="bg-gradient-to-r from-[#009edb]/10 to-purple-500/10 border border-[#009edb]/30 rounded-lg p-6">
            <h3 className="text-lg font-semibold text-white mb-3 flex items-center gap-2">
              <svg className="w-5 h-5 text-[#009edb]" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
              </svg>
              Quick Guide
            </h3>
            <div className="grid md:grid-cols-3 gap-4 text-sm text-white/80">
              <div>
                <div className="font-semibold text-white mb-1">📊 Accuracy</div>
                <div>Overall percentage of correct predictions (both sensitive and not sensitive)</div>
              </div>
              <div>
                <div className="font-semibold text-white mb-1">🎯 Precision</div>
                <div>When model says "sensitive", how often is it correct? (fewer false alarms)</div>
              </div>
              <div>
                <div className="font-semibold text-white mb-1">🔍 Recall</div>
                <div>Of all truly sensitive data, how much did the model catch? (fewer misses)</div>
              </div>
              </div>
            </div>
          </div>
        // </div>
      )}

      {/* Cost Analysis Tab */}
      {activeView === 'costs' && (
        <div className="space-y-8">
          {/* Introduction */}
          <div className="bg-gradient-to-r from-green-500/10 to-emerald-500/10 border border-green-500/30 rounded-lg p-6">
            <h3 className="text-xl font-bold text-white mb-4 flex items-center gap-2">
              <svg className="w-6 h-6 text-green-400" fill="currentColor" viewBox="0 0 20 20">
                <path d="M8.433 7.418c.155-.103.346-.196.567-.267v1.698a2.305 2.305 0 01-.567-.267C8.07 8.34 8 8.114 8 8c0-.114.07-.34.433-.582zM11 12.849v-1.698c.22.071.412.164.567.267.364.243.433.468.433.582 0 .114-.07.34-.433.582a2.305 2.305 0 01-.567.267z" />
                <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm1-13a1 1 0 10-2 0v.092a4.535 4.535 0 00-1.676.662C6.602 6.234 6 7.009 6 8c0 .99.602 1.765 1.324 2.246.48.32 1.054.545 1.676.662v1.941c-.391-.127-.68-.317-.843-.504a1 1 0 10-1.51 1.31c.562.649 1.413 1.076 2.353 1.253V15a1 1 0 102 0v-.092a4.535 4.535 0 001.676-.662C13.398 13.766 14 12.991 14 12c0-.99-.602-1.765-1.324-2.246A4.535 4.535 0 0011 9.092V7.151c.391.127.68.317.843.504a1 1 0 101.511-1.31c-.563-.649-1.413-1.076-2.354-1.253V5z" clipRule="evenodd" />
              </svg>
              Token Usage & Cost Analysis
            </h3>
            <p className="text-sm text-white/90 leading-relaxed">
              This analysis shows the total token consumption and associated costs for each AI model across all processed reports.
              Costs are calculated based on the combined prompt and completion tokens used during report generation.
            </p>
          </div>

          {loadingCosts ? (
            <div className="flex items-center justify-center p-12">
              <div className="text-center">
                <svg
                  className="animate-spin h-12 w-12 text-[#009edb] mx-auto mb-4"
                  xmlns="http://www.w3.org/2000/svg"
                  fill="none"
                  viewBox="0 0 24 24"
                >
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                </svg>
                <p className="text-white/80">Loading cost analysis...</p>
              </div>
            </div>
          ) : !costAnalysis ? (
            <div className="text-sm text-white/80 border rounded-lg p-6">
              No cost analysis data available.
            </div>
          ) : (
            <div className="space-y-6">
              {/* Summary Cards */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className="bg-gradient-to-br from-blue-500/20 to-blue-600/20 border border-blue-500/30 rounded-lg p-6">
                  <div className="text-blue-300 text-sm font-medium mb-2">Total Models</div>
                  <div className="text-3xl font-bold text-white">
                    {Object.keys(costAnalysis.models || {}).length}
                  </div>
                </div>
                <div className="bg-gradient-to-br from-purple-500/20 to-purple-600/20 border border-purple-500/30 rounded-lg p-6">
                  <div className="text-purple-300 text-sm font-medium mb-2">Total Tokens</div>
                  <div className="text-3xl font-bold text-white">
                    {Object.values(costAnalysis.models || {}).reduce((sum: number, model: any) => sum + model.total_tokens, 0).toLocaleString()}
                  </div>
                </div>
                <div className="bg-gradient-to-br from-green-500/20 to-green-600/20 border border-green-500/30 rounded-lg p-6">
                  <div className="text-green-300 text-sm font-medium mb-2">Total Cost</div>
                  <div className="text-3xl font-bold text-white">
                    ${Object.values(costAnalysis.models || {}).reduce((sum: number, model: any) => sum + model.total_cost_usd, 0).toFixed(4)}
                  </div>
                </div>
              </div>

              {/* Detailed Cost Table */}
              <div className="border border-white/20 rounded-lg overflow-hidden bg-white/5">
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="bg-[#009edb]/20 border-b border-white/20">
                      <tr>
                        <th className="text-left py-3 px-4 text-white font-semibold">Model</th>
                        <th className="text-center py-3 px-3 text-white font-semibold">Reports</th>
                        <th className="text-right py-3 px-3 text-white font-semibold">Prompt Tokens</th>
                        <th className="text-right py-3 px-3 text-white font-semibold">Completion Tokens</th>
                        <th className="text-right py-3 px-3 text-white font-semibold">Total Tokens</th>
                        <th className="text-right py-3 px-3 text-white font-semibold">Price/1M</th>
                        <th className="text-right py-3 px-3 text-white font-semibold">Total Cost</th>
                        <th className="text-right py-3 px-3 text-white font-semibold">Cost/Report</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.entries(costAnalysis.models || {})
                        .sort(([, a]: [string, any], [, b]: [string, any]) => b.total_cost_usd - a.total_cost_usd)
                        .map(([modelName, data]: [string, any]) => (
                          <tr key={modelName} className="border-b border-white/10 hover:bg-white/5">
                            <td className="py-3 px-4 text-white font-medium">{modelName}</td>
                            <td className="py-3 px-3 text-center text-white">
                              {data.reports_processed}
                              {data.reports_with_errors > 0 && (
                                <span className="ml-1 text-xs text-red-400" title={`${data.reports_with_errors} errors`}>
                                  ⚠️
                                </span>
                              )}
                            </td>
                            <td className="py-3 px-3 text-right text-white/80 font-mono text-xs">
                              {data.prompt_tokens.toLocaleString()}
                            </td>
                            <td className="py-3 px-3 text-right text-white/80 font-mono text-xs">
                              {data.completion_tokens.toLocaleString()}
                            </td>
                            <td className="py-3 px-3 text-right text-white font-semibold font-mono text-xs">
                              {data.total_tokens.toLocaleString()}
                            </td>
                            <td className="py-3 px-3 text-right text-white/80 font-mono text-xs">
                              ${data.price_per_million.toFixed(2)}
                            </td>
                            <td className="py-3 px-3 text-right text-green-400 font-bold font-mono">
                              ${data.total_cost_usd.toFixed(4)}
                            </td>
                            <td className="py-3 px-3 text-right text-white/80 font-mono text-xs">
                              ${data.cost_per_report.toFixed(4)}
                            </td>
                          </tr>
                        ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* Pricing Reference */}
              <div className="bg-white/5 border border-white/20 rounded-lg p-6">
                <h4 className="font-semibold text-white mb-4 flex items-center gap-2">
                  <svg className="w-5 h-5 text-blue-400" fill="currentColor" viewBox="0 0 20 20">
                    <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
                  </svg>
                  Pricing Information
                </h4>
                <div className="grid grid-cols-2 md:grid-cols-3 gap-4 text-sm">
                  {Object.entries(costAnalysis.pricing || {}).map(([model, price]: [string, any]) => (
                    <div key={model} className="flex justify-between items-center bg-white/5 rounded px-3 py-2">
                      <span className="text-white/80">{model}</span>
                      <span className="text-white font-mono font-medium">${price}/1M</span>
                    </div>
                  ))}
                </div>
                <p className="text-xs text-white/60 mt-4 italic">
                  * Prices are per 1 million tokens (both prompt and completion tokens use the same rate)
                </p>
              </div>
            </div>
          )}
        </div>
      )}

    </div>
  );
}
