"use client";
import { useState, useEffect } from "react";
import { BarChart3, TrendingUp, Users, FileText, Activity, RefreshCw } from "lucide-react";
import { getApiUrl } from "../services/api";

export default function AnalyticsTab() {
  const [batchStatus, setBatchStatus] = useState<any>(null);
  const [datasets, setDatasets] = useState<any[]>([]);
  const [models, setModels] = useState<string[]>([]);

  const fetchBatchStatus = async () => {
    try {
      const response = await fetch(getApiUrl("api/batch-status"));
      if (response.ok) {
        const data = await response.json();
        setBatchStatus(data);
      }
    } catch (error) {
      console.error("Failed to fetch batch status:", error);
    }
  };

  const fetchDatasets = async () => {
    try {
      const response = await fetch(getApiUrl("api/datasets"));
      if (response.ok) {
        const data = await response.json();
        setDatasets(data.datasets || []);
      }
    } catch (error) {
      console.error("Failed to fetch datasets:", error);
    }
  };

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

  useEffect(() => {
    fetchDatasets();
    fetchModels();
    fetchBatchStatus();
    
    // Poll batch status every 5 seconds if processing
    const interval = setInterval(() => {
      fetchBatchStatus();
    }, 5000);

    return () => clearInterval(interval);
  }, []);

  const mockAnalytics = {
    totalDatasets: datasets.length,
    totalProcessed: batchStatus?.completed_models?.length || 0,
    avgProcessingTime: "2.4 min",
    accuracyRate: "94.2%",
    piiDetected: 1247,
    highRiskDatasets: 3,
  };

  return (
    <div className="p-8">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900 mb-2">Analytics</h1>
          <p className="text-gray-600">Overview of pipeline performance and metrics</p>
        </div>

        {/* Batch Processing Status */}
        {batchStatus && (
          <div className="bg-white rounded-xl border border-gray-200 p-6 mb-8">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-semibold text-gray-800">Batch Processing Status</h3>
              <button
                onClick={fetchBatchStatus}
                className="flex items-center gap-2 px-3 py-1 text-sm border border-gray-300 rounded-lg hover:bg-gray-50"
              >
                <RefreshCw className="w-4 h-4" />
                Refresh
              </button>
            </div>
            
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-gray-600">Status:</span>
                <span className={`px-2 py-1 text-xs font-medium rounded-full ${
                  batchStatus.is_running 
                    ? 'bg-yellow-100 text-yellow-800' 
                    : 'bg-green-100 text-green-800'
                }`}>
                  {batchStatus.is_running ? 'Running' : 'Idle'}
                </span>
              </div>
              
              {batchStatus.is_running && batchStatus.current_model && (
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium text-gray-600">Current Model:</span>
                  <span className="text-sm text-gray-900">{batchStatus.current_model}</span>
                </div>
              )}
              
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-gray-600">Progress:</span>
                <span className="text-sm text-gray-900">{batchStatus.progress || 0}%</span>
              </div>
              
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div 
                  className="bg-blue-600 h-2 rounded-full transition-all duration-300"
                  style={{ width: `${batchStatus.progress || 0}%` }}
                ></div>
              </div>
              
              <div className="grid grid-cols-2 gap-4 mt-4">
                <div>
                  <span className="text-sm font-medium text-gray-600">Completed:</span>
                  <div className="flex flex-wrap gap-1 mt-1">
                    {batchStatus.completed_models?.map((model: string) => (
                      <span key={model} className="px-2 py-1 bg-green-100 text-green-800 text-xs rounded">
                        {model}
                      </span>
                    ))}
                  </div>
                </div>
                
                {batchStatus.failed_models?.length > 0 && (
                  <div>
                    <span className="text-sm font-medium text-gray-600">Failed:</span>
                    <div className="flex flex-wrap gap-1 mt-1">
                      {batchStatus.failed_models.map((model: string) => (
                        <span key={model} className="px-2 py-1 bg-red-100 text-red-800 text-xs rounded">
                          {model}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Key Metrics */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
          <div className="bg-white rounded-xl border border-gray-200 p-6">
            <div className="flex items-center justify-between mb-2">
              <FileText className="w-8 h-8 text-blue-600" />
              <span className="text-sm text-green-600 font-medium">+12%</span>
            </div>
            <h3 className="text-2xl font-bold text-gray-900">{mockAnalytics.totalDatasets}</h3>
            <p className="text-sm text-gray-600">Total Datasets</p>
          </div>

          <div className="bg-white rounded-xl border border-gray-200 p-6">
            <div className="flex items-center justify-between mb-2">
              <Activity className="w-8 h-8 text-green-600" />
              <span className="text-sm text-green-600 font-medium">+8%</span>
            </div>
            <h3 className="text-2xl font-bold text-gray-900">{mockAnalytics.totalProcessed}</h3>
            <p className="text-sm text-gray-600">Processed</p>
          </div>

          <div className="bg-white rounded-xl border border-gray-200 p-6">
            <div className="flex items-center justify-between mb-2">
              <TrendingUp className="w-8 h-8 text-purple-600" />
              <span className="text-sm text-green-600 font-medium">+2.1%</span>
            </div>
            <h3 className="text-2xl font-bold text-gray-900">{mockAnalytics.accuracyRate}</h3>
            <p className="text-sm text-gray-600">Accuracy Rate</p>
          </div>

          <div className="bg-white rounded-xl border border-gray-200 p-6">
            <div className="flex items-center justify-between mb-2">
              <Users className="w-8 h-8 text-red-600" />
              <span className="text-sm text-red-600 font-medium">Alert</span>
            </div>
            <h3 className="text-2xl font-bold text-gray-900">{mockAnalytics.piiDetected.toLocaleString()}</h3>
            <p className="text-sm text-gray-600">PII Detected</p>
          </div>
        </div>

        {/* Available Models */}
        <div className="bg-white rounded-xl border border-gray-200 p-6 mb-8">
          <h3 className="text-lg font-semibold text-gray-800 mb-4">Available Models</h3>
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
            {models.map((model) => (
              <div key={model} className="px-3 py-2 bg-gray-50 border border-gray-200 rounded-lg text-sm font-medium text-gray-700">
                {model}
              </div>
            ))}
          </div>
        </div>

        {/* Recent Datasets */}
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-800 mb-4">Recent Datasets</h3>
          <div className="space-y-3">
            {datasets.slice(0, 5).map((dataset) => (
              <div key={dataset.name} className="flex items-center gap-3 p-3 bg-gray-50 rounded-lg">
                <div className="w-2 h-2 bg-green-600 rounded-full"></div>
                <div className="flex-1">
                  <p className="text-sm font-medium text-gray-900">{dataset.name}</p>
                  <p className="text-xs text-gray-600">
                    {dataset.status} • {new Date(dataset.created_at).toLocaleDateString()}
                  </p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
