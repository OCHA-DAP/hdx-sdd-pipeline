"use client";
import { useState } from "react";
import { getApiUrl } from "../services/api";

const MODEL_OPTIONS = [
    "gpt-5-nano",
    "gpt-5-mini",
    "gpt-4.1-nano",
    "gpt-4.1-mini",
    "gpt-4.1",
    "DeepSeek-V3.1",
];

interface UploadedFile {
  name: string;
  size: number;
  uploadedAt: Date;
}

interface Props {
  datasets?: any[];
  onRefresh?: () => void;
}

export default function UploadTab({ onRefresh }: Props) {
  const [uploadedFiles, setUploadedFiles] = useState<UploadedFile[]>([]);
  const [generating, setGenerating] = useState<Record<string, boolean>>({});
  const [generatingAll, setGeneratingAll] = useState<Record<string, boolean>>({});
  const [creatingTemplate, setCreatingTemplate] = useState<Record<string, boolean>>({});
  const [modelName, setModelName] = useState(MODEL_OPTIONS[0]);

  const handleUpload = async (files: FileList | null) => {
    if (!files) return;

    const formData = new FormData();
    const newFiles: UploadedFile[] = [];

    Array.from(files).forEach((file) => {
      formData.append("file", file);
      newFiles.push({
        name: file.name,
        size: file.size,
        uploadedAt: new Date(),
      });
    });

    try {
      const response = await fetch(getApiUrl("api/upload"), {
        method: "POST",
        body: formData,
      });

      if (response.ok) {
        const data = await response.json();
        console.log("Upload successful:", data);
        // Add newly uploaded files to the list
        setUploadedFiles((prev) => [...prev, ...newFiles]);
        if (onRefresh) {
          onRefresh();
        }
      } else {
        console.error("Failed to upload files");
        alert("Failed to upload files. Please try again.");
      }
    } catch (error) {
      console.error("Error uploading files:", error);
      alert("Error uploading files. Please try again.");
    }
  };

  const handleGenerateReport = async (datasetName: string) => {
    setGenerating((prev) => ({ ...prev, [datasetName]: true }));
    try {
      const params = new URLSearchParams({
        dataset_filename: datasetName,
        model_name: modelName,
      });

      const response = await fetch(getApiUrl(`api/generate-report?${params}`), {
        method: "POST",
      });

      if (response.ok) {
        const data = await response.json();
        console.log("Report generated:", data);
        alert(`Report generated successfully! Sensitivity: ${data.sensitivity || 'N/A'}`);
      } else {
        const error = await response.json();
        alert(`Failed to generate report: ${error.detail || 'Unknown error'}`);
      }
    } catch (error) {
      console.error("Error generating report:", error);
      alert("Failed to generate report");
    } finally {
      setGenerating((prev) => ({ ...prev, [datasetName]: false }));
    }
  };

  const handleGenerateAllReports = async (datasetName: string) => {
    setGeneratingAll((prev) => ({ ...prev, [datasetName]: true }));
    try {
      const params = new URLSearchParams({
        dataset_filename: datasetName,
      });

      const response = await fetch(getApiUrl(`api/generate-all-reports?${params}`), {
        method: "POST",
      });

      if (response.ok) {
        const data = await response.json();
        console.log("All reports generated:", data);
        const summary = Object.entries(data.results)
          .map(([model, result]: [string, any]) => `${model}: ${result.status}`)
          .join('\n');
        alert(`Batch generation complete!\n\n${summary}`);
      } else {
        const error = await response.json();
        alert(`Failed to generate reports: ${error.detail || 'Unknown error'}`);
      }
    } catch (error) {
      console.error("Error generating all reports:", error);
      alert("Failed to generate reports");
    } finally {
      setGeneratingAll((prev) => ({ ...prev, [datasetName]: false }));
    }
  };

  const handleCreateGroundTruthTemplate = async (datasetName: string) => {
    setCreatingTemplate((prev) => ({ ...prev, [datasetName]: true }));
    try {
      const params = new URLSearchParams({
        dataset_filename: datasetName,
      });

      const response = await fetch(getApiUrl(`api/create-groundtruth-template?${params}`), {
        method: "POST",
      });

      if (response.ok) {
        const data = await response.json();
        console.log("Template created:", data);
        if (data.exists) {
          alert(`Ground truth template already exists at:\n${data.template_path}`);
        } else {
          alert(`Ground truth template created successfully!\n\nPath: ${data.template_path}\n\nYou can now manually annotate this file.`);
        }
      } else {
        const error = await response.json();
        alert(`Failed to create template: ${error.detail || 'Unknown error'}`);
      }
    } catch (error) {
      console.error("Error creating template:", error);
      alert("Failed to create template");
    } finally {
      setCreatingTemplate((prev) => ({ ...prev, [datasetName]: false }));
    }
  };

  return (
    <div className="space-y-8 max-w-7xl mx-auto">
      {/* Hero Section */}
      <div className="relative overflow-hidden rounded-2xl bg-gradient-to-br from-blue-600/20 via-purple-600/20 to-pink-600/20 border border-white/20 p-8 md:p-12">
        <div className="relative z-10">
          <h1 className="text-4xl md:text-5xl font-bold text-white mb-4">
            📤 Dataset Upload & Processing
          </h1>
          <p className="text-xl text-white/80 max-w-3xl">
            Upload your humanitarian datasets and generate sensitivity reports using state-of-the-art AI models
          </p>
        </div>
        <div className="absolute top-0 right-0 w-64 h-64 bg-blue-500/10 rounded-full blur-3xl"></div>
        <div className="absolute bottom-0 left-0 w-64 h-64 bg-purple-500/10 rounded-full blur-3xl"></div>
      </div>

      {/* Upload Section */}
      <div className="bg-white/5 border border-white/20 rounded-xl p-8 backdrop-blur-sm">
        <div className="flex items-center gap-3 mb-6">
          <span className="text-3xl">☁️</span>
          <h2 className="text-2xl font-bold text-white">Upload New Dataset</h2>
        </div>
        
        <label className="group relative flex flex-col items-center justify-center border-2 border-dashed border-white/30 rounded-xl p-12 cursor-pointer hover:border-[#009edb] hover:bg-[#009edb]/10 transition-all duration-300">
          <input
            type="file"
            accept=".csv,.xlsx"
            multiple
            onChange={(e) => handleUpload(e.target.files)}
            className="hidden"
          />
          <div className="text-center">
            <div className="w-20 h-20 mx-auto mb-4 rounded-full bg-gradient-to-br from-blue-500/20 to-purple-500/20 flex items-center justify-center group-hover:scale-110 transition-transform">
              <svg className="w-10 h-10 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
              </svg>
            </div>
            <p className="text-lg font-semibold text-white mb-2">Click to upload or drag and drop</p>
            <p className="text-sm text-white/60">CSV or XLSX files • Multiple files supported</p>
          </div>
        </label>
      </div>

      {/* Model Selection */}
      <div className="bg-gradient-to-br from-purple-500/20 to-blue-500/20 border border-purple-500/30 rounded-xl p-6 backdrop-blur-sm">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <span className="text-2xl">🤖</span>
            <div>
              <h3 className="text-lg font-semibold text-white">AI Model Selection</h3>
              <p className="text-sm text-white/60">Choose which model to use for single-file processing</p>
            </div>
          </div>
          <select
            value={modelName}
            onChange={(e) => setModelName(e.target.value)}
            className="px-4 py-2 rounded-lg text-sm font-medium bg-white/10 text-white border border-white/20 hover:bg-white/20 transition cursor-pointer backdrop-blur-sm"
          >
            {MODEL_OPTIONS.map((model) => (
              <option key={model} value={model} className="bg-gray-900">
                {model}
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Datasets Grid */}
      <div>
        <div className="flex items-center gap-3 mb-6">
          <span className="text-3xl">📊</span>
          <h2 className="text-2xl font-bold text-white">Uploaded Files</h2>
          <span className="px-3 py-1 bg-blue-500/20 text-blue-300 rounded-full text-sm font-medium">
            {uploadedFiles.length} {uploadedFiles.length === 1 ? 'file' : 'files'}
          </span>
        </div>

        {uploadedFiles.length === 0 ? (
          <div className="text-center py-16 border border-white/20 rounded-xl bg-white/5">
            <div className="text-6xl mb-4">📁</div>
            <p className="text-xl text-white/80 mb-2">No files uploaded yet</p>
            <p className="text-sm text-white/60">Upload your first dataset to get started</p>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {uploadedFiles.map((file) => (
              <div
                key={file.name}
                className="group relative bg-gradient-to-br from-white/10 to-white/5 border border-white/20 rounded-xl p-6 hover:border-[#009edb]/50 hover:shadow-xl hover:shadow-[#009edb]/20 transition-all duration-300 backdrop-blur-sm"
              >
                {/* File Icon & Name */}
                <div className="flex items-start justify-between mb-4">
                  <div className="flex-1">
                    <div className="flex items-center gap-2 mb-2">
                      <span className="text-2xl">
                        {file.name.endsWith('.csv') ? '📄' : '📊'}
                      </span>
                      <h3 className="font-semibold text-white truncate" title={file.name}>
                        {file.name}
                      </h3>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="px-2 py-1 bg-gradient-to-r from-blue-500/30 to-purple-500/30 text-white rounded text-xs font-medium">
                        {file.name.split(".").pop()?.toUpperCase()}
                      </span>
                      <span className="text-xs text-white/60">
                        {(file.size / 1024).toFixed(1)} KB
                      </span>
                    </div>
                  </div>
                </div>

                {/* Action Buttons */}
                <div className="space-y-2">
                  {/* Single Model Generation */}
                  <button
                    onClick={() => handleGenerateReport(file.name)}
                    disabled={generating[file.name]}
                    className="w-full px-4 py-2.5 bg-gradient-to-r from-[#009edb] to-[#0088c2] text-white rounded-lg hover:from-[#0088c2] hover:to-[#007ab3] disabled:opacity-50 disabled:cursor-not-allowed text-sm font-medium transition-all duration-300 flex items-center justify-center gap-2 group-hover:shadow-lg group-hover:shadow-[#009edb]/30"
                  >
                    {generating[file.name] ? (
                      <>
                        <svg className="animate-spin h-4 w-4" fill="none" viewBox="0 0 24 24">
                          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                        </svg>
                        Generating...
                      </>
                    ) : (
                      <>
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                        </svg>
                        Generate ({modelName})
                      </>
                    )}
                  </button>

                  {/* All Models Generation */}
                  <button
                    onClick={() => handleGenerateAllReports(file.name)}
                    disabled={generatingAll[file.name]}
                    className="w-full px-4 py-2.5 bg-gradient-to-r from-purple-600 to-purple-700 text-white rounded-lg hover:from-purple-700 hover:to-purple-800 disabled:opacity-50 disabled:cursor-not-allowed text-sm font-medium transition-all duration-300 flex items-center justify-center gap-2"
                  >
                    {generatingAll[file.name] ? (
                      <>
                        <svg className="animate-spin h-4 w-4" fill="none" viewBox="0 0 24 24">
                          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                        </svg>
                        Processing All...
                      </>
                    ) : (
                      <>
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
                        </svg>
                        All Models
                      </>
                    )}
                  </button>

                  {/* Ground Truth Template */}
                  <button
                    onClick={() => handleCreateGroundTruthTemplate(file.name)}
                    disabled={creatingTemplate[file.name]}
                    className="w-full px-4 py-2.5 bg-gradient-to-r from-green-600 to-green-700 text-white rounded-lg hover:from-green-700 hover:to-green-800 disabled:opacity-50 disabled:cursor-not-allowed text-sm font-medium transition-all duration-300 flex items-center justify-center gap-2"
                  >
                    {creatingTemplate[file.name] ? (
                      <>
                        <svg className="animate-spin h-4 w-4" fill="none" viewBox="0 0 24 24">
                          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                        </svg>
                        Creating...
                      </>
                    ) : (
                      <>
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                        </svg>
                        GT Template
                      </>
                    )}
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Info Section */}
      <div className="bg-gradient-to-br from-blue-500/20 to-teal-500/20 border border-blue-500/30 rounded-xl p-6 backdrop-blur-sm">
        <div className="flex items-start gap-4">
          <span className="text-3xl">💡</span>
          <div>
            <h3 className="text-lg font-semibold text-white mb-2">Quick Guide</h3>
            <ul className="space-y-2 text-white/80 text-sm">
              <li className="flex items-start gap-2">
                <span className="text-blue-400 mt-0.5">•</span>
                <span><strong className="text-white">Generate (Model):</strong> Create a report using the selected AI model</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-purple-400 mt-0.5">•</span>
                <span><strong className="text-white">All Models:</strong> Generate reports for all available models at once</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-green-400 mt-0.5">•</span>
                <span><strong className="text-white">GT Template:</strong> Create a ground truth template for manual annotation</span>
              </li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}
