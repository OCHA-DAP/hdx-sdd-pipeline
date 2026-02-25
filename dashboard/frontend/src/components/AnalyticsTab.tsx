"use client";
import { useState } from "react";
import { BarChart3, TrendingUp, Users, FileText, Activity } from "lucide-react";

export default function AnalyticsTab() {
  const mockAnalytics = {
    totalDatasets: 24,
    totalProcessed: 18,
    avgProcessingTime: "2.4 min",
    accuracyRate: "94.2%",
    piiDetected: 1247,
    highRiskDatasets: 3,
  };

  const mockTrends = [
    { name: "Mon", processed: 4, pii: 89 },
    { name: "Tue", processed: 6, pii: 156 },
    { name: "Wed", processed: 3, pii: 67 },
    { name: "Thu", processed: 8, pii: 234 },
    { name: "Fri", processed: 5, pii: 123 },
    { name: "Sat", processed: 2, pii: 45 },
    { name: "Sun", processed: 1, pii: 23 },
  ];

  const modelPerformance = [
    { model: "gpt-4.1", accuracy: 96.2, speed: "Fast", usage: 45 },
    { model: "DeepSeek-V3.1", accuracy: 94.8, speed: "Medium", usage: 30 },
    { model: "gpt-5-mini", accuracy: 92.1, speed: "Fast", usage: 25 },
  ];

  return (
    <div className="p-8">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900 mb-2">Analytics</h1>
          <p className="text-gray-600">Overview of pipeline performance and metrics</p>
        </div>

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

        {/* Charts Section */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
          {/* Weekly Trend */}
          <div className="bg-white rounded-xl border border-gray-200 p-6">
            <h3 className="text-lg font-semibold text-gray-800 mb-4">Weekly Processing Trend</h3>
            <div className="space-y-3">
              {mockTrends.map((day) => (
                <div key={day.name} className="flex items-center gap-4">
                  <div className="w-12 text-sm font-medium text-gray-600">{day.name}</div>
                  <div className="flex-1">
                    <div className="flex items-center gap-2">
                      <div className="flex-1 bg-gray-200 rounded-full h-6 relative overflow-hidden">
                        <div
                          className="bg-blue-600 h-full rounded-full flex items-center justify-end pr-2"
                          style={{ width: `${(day.processed / 8) * 100}%` }}
                        >
                          <span className="text-xs text-white font-medium">{day.processed}</span>
                        </div>
                      </div>
                      <div className="w-16 text-xs text-gray-500 text-right">{day.pii} PII</div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Model Performance */}
          <div className="bg-white rounded-xl border border-gray-200 p-6">
            <h3 className="text-lg font-semibold text-gray-800 mb-4">Model Performance</h3>
            <div className="space-y-4">
              {modelPerformance.map((model) => (
                <div key={model.model} className="border-b border-gray-100 pb-4 last:border-0">
                  <div className="flex items-center justify-between mb-2">
                    <h4 className="font-medium text-gray-900">{model.model}</h4>
                    <span className="text-sm text-gray-600">{model.speed}</span>
                  </div>
                  <div className="space-y-2">
                    <div className="flex items-center justify-between text-sm">
                      <span className="text-gray-600">Accuracy:</span>
                      <span className="font-medium text-gray-900">{model.accuracy}%</span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div
                        className="bg-green-600 h-2 rounded-full"
                        style={{ width: `${model.accuracy}%` }}
                      ></div>
                    </div>
                    <div className="flex items-center justify-between text-sm">
                      <span className="text-gray-600">Usage:</span>
                      <span className="font-medium text-gray-900">{model.usage}%</span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div
                        className="bg-blue-600 h-2 rounded-full"
                        style={{ width: `${model.usage}%` }}
                      ></div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Recent Activity */}
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-800 mb-4">Recent Activity</h3>
          <div className="space-y-3">
            <div className="flex items-center gap-3 p-3 bg-gray-50 rounded-lg">
              <div className="w-2 h-2 bg-green-600 rounded-full"></div>
              <div className="flex-1">
                <p className="text-sm font-medium text-gray-900">humanitarian_data.csv processed</p>
                <p className="text-xs text-gray-600">2 minutes ago • gpt-4.1 • Medium sensitivity</p>
              </div>
            </div>
            <div className="flex items-center gap-3 p-3 bg-gray-50 rounded-lg">
              <div className="w-2 h-2 bg-blue-600 rounded-full"></div>
              <div className="flex-1">
                <p className="text-sm font-medium text-gray-900">refugee_data.xlsx uploaded</p>
                <p className="text-xs text-gray-600">15 minutes ago • 12.5 MB</p>
              </div>
            </div>
            <div className="flex items-center gap-3 p-3 bg-gray-50 rounded-lg">
              <div className="w-2 h-2 bg-yellow-600 rounded-full"></div>
              <div className="flex-1">
                <p className="text-sm font-medium text-gray-900">aid_distribution.csv processing</p>
                <p className="text-xs text-gray-600">30 minutes ago • DeepSeek-V3.1 • In progress</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
