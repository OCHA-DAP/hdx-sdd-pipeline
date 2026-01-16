"use client";
import { useState } from "react";
import { Dataset } from "../types/dataset";
import DatasetCard from "./DatasetCard";

const MODEL_OPTIONS = [
  'gpt-5-nano',
  'gpt-5-mini',
  'gpt-4.1-nano',
  'DeepSeek-V3.1',
  'gpt-4.1-mini',
];

interface Props {
  datasets: Dataset[];
  onRefresh?: () => void;
}

const handleUpload = async (files: FileList | null, onRefresh?: () => void) => {
  console.log("Uploading files", files);
  if (files) {
    const formData = new FormData();
    Array.from(files).forEach((file) => {
      formData.append("file", file);
    });
    const response = await fetch("http://localhost:8000/api/upload", {
      method: "POST",
      body: formData,
    });
    if (response.ok) {
      const data = await response.json();
      console.log(data);
      if (onRefresh) {
        onRefresh();
      }
    } else {
      console.error("Failed to upload files");
    }
  }
}

export default function UploadTab({ datasets, onRefresh }: Props) {
  const [generating, setGenerating] = useState<Record<string, boolean>>({});
  const [modelName, setModelName] = useState(MODEL_OPTIONS[0]);

  const handleGenerateReport = async (datasetName: string) => {
    setGenerating((prev) => ({ ...prev, [datasetName]: true }));
    try {
      const params = new URLSearchParams({
        dataset_filename: datasetName,
        model_name: modelName,
      });

      const response = await fetch(`http://localhost:8000/api/generate-report?${params}`, {
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

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl text-white font-semibold mb-2">Upload datasets</h2>
        <p className="text-sm text-white/80 mb-4">
          Upload CSV or Excel files.
        </p>

        <label className="flex flex-col items-center justify-center border-2 border-dashed rounded-lg p-6 cursor-pointer hover:bg-gray-50 hover:text-[#009edb] transition text-white">
          <input
            type="file"
            accept=".csv,.xlsx"
            multiple
            onChange={(e) => handleUpload(e.target.files, onRefresh)}
            className="hidden"
          />
          <span className="text-sm font-medium">Click to upload</span>
          <span className="text-xs ">CSV or XLSX</span>
        </label>
      </div>

      <div>
        <div className="flex items-center justify-between mb-3">
          <h3 className="font-semibold text-white">Uploaded datasets</h3>
          <div className="flex items-center gap-2">
            <label className="text-sm text-white/80">Model:</label>
            <select
              value={modelName}
              onChange={(e) => setModelName(e.target.value)}
              className="px-2 py-1 rounded text-sm text-black"
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
            No datasets uploaded yet
          </div>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            {datasets.map((d) => (
              <div key={d.id} className="border rounded-lg p-4 text-white/80 shadow-sm hover:shadow-md transition">
                <div className="flex items-center justify-between mb-2">
                  <h3 className="font-medium truncate">{d.name}</h3>
                  <span className="text-xs px-2 py-1 rounded bg-gray-100 text-black">
                    {d.name.split(".").pop()?.toUpperCase()}
                  </span>
                </div>
                <p className="text-sm mb-3">Ready for analysis</p>
                <button
                  onClick={() => handleGenerateReport(d.name)}
                  disabled={generating[d.name]}
                  className="w-full px-4 py-2 bg-[#009edb] text-white rounded hover:bg-[#0088c2] disabled:opacity-50 disabled:cursor-not-allowed text-sm font-medium"
                >
                  {generating[d.name] ? "Generating..." : "Generate Report"}
                </button>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
