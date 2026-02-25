"use client";
import { useState } from "react";
import { Upload, Play, FileText, CheckCircle } from "lucide-react";
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

export default function RunPipelineTab() {
  const [uploadedFiles, setUploadedFiles] = useState<UploadedFile[]>([]);
  const [processing, setProcessing] = useState<Record<string, boolean>>({});
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
        setUploadedFiles((prev) => [...prev, ...newFiles]);
      } else {
        alert("Failed to upload files. Please try again.");
      }
    } catch (error) {
      alert("Error uploading files. Please try again.");
    }
  };

  const handleProcessFile = async (fileName: string) => {
    setProcessing((prev) => ({ ...prev, [fileName]: true }));
    try {
      const params = new URLSearchParams({
        dataset_filename: fileName,
        model_name: modelName,
      });

      const response = await fetch(getApiUrl(`api/generate-report?${params}`), {
        method: "POST",
      });

      if (response.ok) {
        const data = await response.json();
        alert(`Processing complete! Sensitivity: ${data.sensitivity || "N/A"}`);
      } else {
        const error = await response.json();
        alert(`Failed to process: ${error.detail || "Unknown error"}`);
      }
    } catch (error) {
      alert("Failed to process file");
    } finally {
      setProcessing((prev) => ({ ...prev, [fileName]: false }));
    }
  };

  return (
    <div className="p-8">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900 mb-2">Run Pipeline</h1>
          <p className="text-gray-600">Upload datasets and process them with AI models</p>
        </div>

        {/* Upload Section */}
        <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
          <div className="flex items-center gap-3 mb-4">
            <Upload className="w-6 h-6 text-blue-600" />
            <h2 className="text-xl font-semibold text-gray-800">Upload Dataset</h2>
          </div>

          <label className="flex flex-col items-center justify-center border-2 border-dashed border-gray-300 rounded-lg p-8 cursor-pointer hover:border-blue-400 hover:bg-blue-50 transition-colors">
            <input
              type="file"
              accept=".csv,.xlsx"
              multiple
              onChange={(e) => handleUpload(e.target.files)}
              className="hidden"
            />
            <div className="text-center">
              <Upload className="w-12 h-12 text-gray-400 mx-auto mb-3" />
              <p className="text-lg font-medium text-gray-700 mb-1">
                Click to upload or drag and drop
              </p>
              <p className="text-sm text-gray-500">
                CSV or XLSX files
              </p>
            </div>
          </label>
        </div>

        {/* Model Selection */}
        <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
          <div className="flex items-center justify-between">
            <div>
              <h3 className="text-lg font-semibold text-gray-800">AI Model</h3>
              <p className="text-sm text-gray-600">Choose model for processing</p>
            </div>
            <select
              value={modelName}
              onChange={(e) => setModelName(e.target.value)}
              className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium bg-white text-gray-800 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {MODEL_OPTIONS.map((model) => (
                <option key={model} value={model}>
                  {model}
                </option>
              ))}
            </select>
          </div>
        </div>

        {/* Files List */}
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <h2 className="text-xl font-semibold text-gray-800 mb-4">Uploaded Files</h2>
          
          {uploadedFiles.length === 0 ? (
            <div className="text-center py-12">
              <FileText className="w-12 h-12 text-gray-300 mx-auto mb-3" />
              <p className="text-gray-500">No files uploaded yet</p>
            </div>
          ) : (
            <div className="space-y-3">
              {uploadedFiles.map((file) => (
                <div
                  key={file.name}
                  className="flex items-center justify-between p-4 border border-gray-200 rounded-lg hover:bg-gray-50"
                >
                  <div className="flex items-center gap-3">
                    <FileText className="w-5 h-5 text-gray-400" />
                    <div>
                      <p className="font-medium text-gray-800">{file.name}</p>
                      <p className="text-sm text-gray-500">
                        {(file.size / 1024).toFixed(1)} KB
                      </p>
                    </div>
                  </div>
                  
                  <button
                    onClick={() => handleProcessFile(file.name)}
                    disabled={processing[file.name]}
                    className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                  >
                    {processing[file.name] ? (
                      <>
                        <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                        Processing...
                      </>
                    ) : (
                      <>
                        <Play className="w-4 h-4" />
                        Process
                      </>
                    )}
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
