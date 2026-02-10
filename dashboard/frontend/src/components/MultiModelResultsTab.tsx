"use client";
import { useEffect, useState } from "react";
import { Dataset } from "../types/dataset";
import { ModelReportView } from "./ModelReportView";
import { getApiUrl } from "../services/api";

const MODEL_OPTIONS = [
    "gpt-5-nano",
    "gpt-5-mini",
    "gpt-4.1-nano",
    "gpt-4.1-mini",
    "gpt-4.1",
    "DeepSeek-V3.1",
];

interface Props {
  datasets: Dataset[];
}

export default function MultiModelResultsTab({ datasets }: Props) {
  const [selectedDataset, setSelectedDataset] = useState<string | null>(null);
  const [reports, setReports] = useState<Record<string, any>>({});
  const [loadingModels, setLoadingModels] = useState<Record<string, boolean>>({});
  const [expandedModels, setExpandedModels] = useState<Record<string, boolean>>({});

  const fetchModelReport = async (dataset: string, model: string) => {
    setLoadingModels((p) => ({ ...p, [model]: true }));

    try {
      const params = new URLSearchParams({
        dataset_filename: dataset,
        model_name: model,
      });

      const res = await fetch(
        getApiUrl(`api/generate-report?${params}`),
        { method: "POST" }
      );

      if (res.ok) {
        const data = await res.json();
        setReports((p) => ({ ...p, [model]: data }));
      }
    } finally {
      setLoadingModels((p) => ({ ...p, [model]: false }));
    }
  };

  const fetchAllModels = async (dataset: string) => {
    setReports({});
    await Promise.all(
      MODEL_OPTIONS.map((model) => fetchModelReport(dataset, model))
    );
  };

  useEffect(() => {
    if (selectedDataset) {
      fetchAllModels(selectedDataset);
    }
  }, [selectedDataset]);

  return (
    <div className="flex gap-6 h-[calc(100vh-200px)]">
      {/* Dataset list (reuse yours almost verbatim) */}
      <div className="w-1/3 border-r pr-6 overflow-y-auto">
        <h2 className="text-xl text-white font-semibold mb-4">Datasets</h2>

        {datasets.map((d) => (
          <button
            key={d.id}
            onClick={() => setSelectedDataset(d.name)}
            className={`w-full text-left border rounded-lg p-3 mb-2 transition ${
              selectedDataset === d.name
                ? "border-[#009edb] bg-white/10"
                : "border-white/20 hover:bg-white/5"
            }`}
          >
            <h3 className="text-white font-medium truncate">{d.name}</h3>
          </button>
        ))}
      </div>

      {/* Reports */}
      <div className="flex-1 overflow-y-auto">
        {!selectedDataset ? (
          <div className="border rounded-lg p-6 text-white/70">
            Select a dataset to compare model predictions.
          </div>
        ) : (
          <div className="space-y-4">
            <h2 className="text-xl font-semibold text-white">
              Model Comparison
            </h2>

            {MODEL_OPTIONS.map((model) => {
              const report = reports[model];
              const isOpen = expandedModels[model];

              return (
                <div
                  key={model}
                  className="border rounded-lg bg-white/5 overflow-hidden"
                >
                  {/* Header */}
                  <button
                    onClick={() =>
                      setExpandedModels((p) => ({
                        ...p,
                        [model]: !p[model],
                      }))
                    }
                    className="w-full flex items-center justify-between px-4 py-3 hover:bg-white/10"
                  >
                    <div className="flex items-center gap-3">
                      <span className="font-medium text-white">{model}</span>
                      {loadingModels[model] && (
                        <span className="text-xs text-white/60">
                          Loading…
                        </span>
                      )}
                    </div>

                    <span className="text-white/60">
                      {isOpen ? "−" : "+"}
                    </span>
                  </button>

                  {/* Body */}
                  {isOpen && (
                    <div className="p-4 border-t border-white/10">
                      {!report || !report.report ? (
                        <div className="text-sm text-white/60">
                          No report available
                        </div>
                      ) : (
                        <ModelReportView report={report} />
                      )}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
