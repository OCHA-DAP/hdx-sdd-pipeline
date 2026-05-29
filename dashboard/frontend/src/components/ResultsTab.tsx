
"use client";
import { useState, useEffect } from "react";
import {
  FileText, Download, Search, Filter, Info,
  CheckCircle, XCircle, Eye,
} from "lucide-react";
import { getApiUrl } from "../services/api";

// ---------------------------------------------------------------------------
// Types — aligned with the refactored backend
// ---------------------------------------------------------------------------

interface ModelResult {
  model: string;
  dataset: string;
  processed_at: string;
  sensitive: string;
  sheet_count: number;
  total_rows: number;
  status: "completed" | "failed" | "error";
}

interface ColumnReport {
  column_name: string;
  sample_values: (string | number | null)[];
  personal_data: Record<string, unknown>;
}

interface SheetDetail {
  sheet_name: string;
  n_records: number;
  personal_data_sensitive: boolean;
  non_personal_data_sensitive: boolean;
  personal_data: { sensitivity: string; explanation?: string };
  non_personal_data: {
    sensitivity: string;
    explanation?: string;
    sensitive_columns?: string[];
    cited_isp_rules?: string[];
    isp_name?: string;
  };
  columns: ColumnReport[];
  is_readme: boolean;
  groundtruth: {
    personal_data_sensitive: boolean | null;
    non_personal_data_sensitive: boolean | null;
  };
}

interface ReportDetail {
  dataset_name: string;
  model: string;
  timestamp: string;
  sensitive: string;
  groundtruth_sensitive: string | null;
  sheets: Record<string, SheetDetail>;
}

interface ErrorRow {
  dataset: string;
  sheet: string;
  errorType: "false_positive" | "false_negative";
  dimension: "personal" | "non_personal";
  gt: boolean;
  pred: boolean;
}

interface Props {
  selectedModel?: string;
}

function sensitivityBadge(s: string) {
  if (s.includes("pd-and-non-pd")) return "bg-red-100 text-red-800";
  if (s.includes("sensitive-pd")) return "bg-red-100 text-red-800";
  if (s.includes("non-pd")) return "bg-yellow-100 text-yellow-800";
  return "bg-green-100 text-green-800";
}

function sensitivityLabel(s: string) {
  const map: Record<string, string> = {
    "not-sensitive": "Not Sensitive",
    "sensitive-pd": "Sensitive (PD)",
    "sensitive-non-pd": "Sensitive (Non-PD)",
    "sensitive-pd-and-non-pd": "Sensitive (PD + Non-PD)",
  };
  return map[s] ?? s;
}

function BoolIcon({ value }: { value: boolean | null }) {
  if (value === null) {
    return <span className="text-gray-400 text-xs">N/A</span>;
  }

  return value ? (
    <CheckCircle className="w-4 h-4 text-green-600 inline" />
  ) : (
    <XCircle className="w-4 h-4 text-red-400 inline" />
  );
}

function collectErrors(detail: ReportDetail, dataset: string): ErrorRow[] {
  const rows: ErrorRow[] = [];

  for (const [name, sheet] of Object.entries(detail.sheets)) {
    const gt = sheet.groundtruth;

    if (gt.personal_data_sensitive === null) continue;

    const dims: Array<{
      dim: "personal" | "non_personal";
      pred: boolean;
      gt: boolean;
    }> = [
      {
        dim: "personal",
        pred: sheet.personal_data_sensitive,
        gt: !!gt.personal_data_sensitive,
      },
      {
        dim: "non_personal",
        pred: sheet.non_personal_data_sensitive,
        gt: !!gt.non_personal_data_sensitive,
      },
    ];

    for (const { dim, pred, gt: g } of dims) {
      if (pred && !g) {
        rows.push({
          dataset,
          sheet: name,
          errorType: "false_positive",
          dimension: dim,
          gt: g,
          pred,
        });
      }

      if (!pred && g) {
        rows.push({
          dataset,
          sheet: name,
          errorType: "false_negative",
          dimension: dim,
          gt: g,
          pred,
        });
      }
    }
  }

  return rows;
}

export default function ResultsTab({ selectedModel = "" }: Props) {
  const [results, setResults] = useState<ModelResult[]>([]);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [details, setDetails] = useState<Record<string, ReportDetail>>({});
  const [errors, setErrors] = useState<ErrorRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [search, setSearch] = useState("");

  useEffect(() => {
    if (!selectedModel) {
      setResults([]);
      return;
    }

    setLoading(true);
    setExpanded(new Set());
    setDetails({});
    setErrors([]);

    fetch(getApiUrl(`api/results/${selectedModel}`))
      .then((r) => (r.ok ? r.json() : Promise.reject(r.status)))
      .then((data) => setResults(data.results ?? []))
      .catch((e) => console.error("Failed to load results:", e))
      .finally(() => setLoading(false));
  }, [selectedModel]);

  useEffect(() => {
    results.forEach((r) => loadDetail(selectedModel, r.dataset));
  }, [results]);

  async function loadDetail(model: string, dataset: string) {
    const key = `${model}::${dataset}`;

    if (details[key]) return;

    try {
      const r = await fetch(getApiUrl(`api/report/${model}/${dataset}`));

      if (!r.ok) return;

      const data: ReportDetail = await r.json();

      setDetails((prev) => ({
        ...prev,
        [key]: data,
      }));

      setErrors((prev) => {
        const kept = prev.filter((e) => !(e.dataset === dataset));
        return [...kept, ...collectErrors(data, dataset)];
      });
    } catch (e) {
      console.error("Failed to load detail:", e);
    }
  }

  function toggleExpand(dataset: string) {
    const key = `${selectedModel}::${dataset}`;

    loadDetail(selectedModel, dataset);

    setExpanded((prev) => {
      const next = new Set(prev);

      next.has(key) ? next.delete(key) : next.add(key);

      return next;
    });
  }

  const filtered = results.filter((r) =>
    r.dataset.toLowerCase().includes(search.toLowerCase())
  );

  const modelErrors = errors.filter(() => true);

  if (!selectedModel) {
    return (
      <div className="p-8 flex flex-col items-center justify-center min-h-[400px] text-center">
        <FileText className="w-16 h-16 text-gray-300 mb-4" />
        <h3 className="text-lg font-medium text-gray-900 mb-1">
          Select a Model
        </h3>
        <p className="text-gray-500">
          Choose a model from the sidebar to view its results.
        </p>
      </div>
    );
  }

  return (
    <div className="p-8">
      <div className="max-w-7xl mx-auto space-y-6">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Results</h1>
          <p className="text-gray-500 mt-1">
            Predictions for{" "}
            <span className="font-medium text-gray-700">
              {selectedModel}
            </span>
          </p>
        </div>

        <div className="bg-white rounded-xl border border-gray-200 p-4 flex items-center gap-4">
          <div className="relative flex-1">
            <Search className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2" />

            <input
              type="text"
              placeholder="Search datasets…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="w-full pl-9 pr-4 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>

          <button className="flex items-center gap-2 px-4 py-2 border border-gray-300 rounded-lg text-sm hover:bg-gray-50">
            <Filter className="w-4 h-4" />
            Filter
          </button>
        </div>

        {modelErrors.length > 0 && (
          <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
            <div className="px-6 py-4 border-b border-gray-100">
              <h2 className="text-base font-semibold text-gray-900">
                False Positives & False Negatives
              </h2>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead className="bg-gray-50 text-gray-600 uppercase tracking-wide">
                  <tr>
                    {[
                      "Dataset",
                      "Sheet",
                      "Dimension",
                      "Error Type",
                      "Ground Truth",
                      "Prediction",
                    ].map((h) => (
                      <th
                        key={h}
                        className="px-4 py-2 text-left font-medium"
                      >
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>

                <tbody className="divide-y divide-gray-100">
                  {modelErrors.map((e, i) => (
                    <tr key={i} className="hover:bg-gray-50">
                      <td className="px-4 py-2 font-medium text-gray-900">
                        {e.dataset}
                      </td>

                      <td className="px-4 py-2 text-gray-600">
                        {e.sheet}
                      </td>

                      <td className="px-4 py-2 text-gray-600">
                        {e.dimension === "personal"
                          ? "Personal"
                          : "Non-Personal"}
                      </td>

                      <td className="px-4 py-2">
                        <span
                          className={`px-2 py-0.5 rounded font-medium ${
                            e.errorType === "false_positive"
                              ? "bg-red-100 text-red-700"
                              : "bg-orange-100 text-orange-700"
                          }`}
                        >
                          {e.errorType === "false_positive"
                            ? "False Positive"
                            : "False Negative"}
                        </span>
                      </td>

                      <td className="px-4 py-2">
                        <BoolIcon value={e.gt} />
                        <span className="ml-1 text-gray-600">
                          {e.gt ? "Sensitive" : "Not Sensitive"}
                        </span>
                      </td>

                      <td className="px-4 py-2">
                        <BoolIcon value={e.pred} />
                        <span className="ml-1 text-gray-600">
                          {e.pred ? "Sensitive" : "Not Sensitive"}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {loading ? (
          <div className="text-center py-16">
            <div className="w-8 h-8 border-2 border-blue-600 border-t-transparent rounded-full animate-spin mx-auto mb-3" />
            <p className="text-gray-500">Loading results…</p>
          </div>
        ) : filtered.length === 0 ? (
          <div className="text-center py-16">
            <FileText className="w-12 h-12 text-gray-300 mx-auto mb-3" />
            <p className="text-gray-500">
              No results found for <strong>{selectedModel}</strong>
            </p>
          </div>
        ) : (
          <div className="space-y-4">
            {filtered.map((result) => {
              const key = `${selectedModel}::${result.dataset}`;
              const isOpen = expanded.has(key);
              const detail = details[key];

              return (
                <div
                  key={key}
                  className="bg-white rounded-xl border border-gray-200 overflow-hidden"
                >
                  <div className="p-6">
                    <div className="flex items-start justify-between mb-4">
                      <div>
                        <h3 className="text-base font-semibold text-gray-900">
                          {result.dataset}
                        </h3>

                        <p className="text-sm text-gray-500 mt-0.5">
                          {new Date(
                            result.processed_at
                          ).toLocaleString()}
                        </p>
                      </div>

                      <span
                        className={`px-2 py-1 text-xs font-medium rounded-full ${sensitivityBadge(
                          result.sensitive
                        )}`}
                      >
                        {sensitivityLabel(result.sensitive)}
                      </span>
                    </div>

                    <div className="grid grid-cols-3 gap-3 mb-4">
                      {[
                        {
                          label: "Sheets",
                          value: result.sheet_count,
                        },
                        {
                          label: "Total Rows",
                          value: result.total_rows.toLocaleString(),
                        },
                        {
                          label: "Sensitivity",
                          value: sensitivityLabel(result.sensitive),
                        },
                      ].map(({ label, value }) => (
                        <div
                          key={label}
                          className="p-3 bg-gray-50 rounded-lg text-center"
                        >
                          <div className="text-sm font-semibold text-gray-900">
                            {value}
                          </div>

                          <div className="text-xs text-gray-500 mt-0.5">
                            {label}
                          </div>
                        </div>
                      ))}
                    </div>

                    <div className="flex gap-2">
                      <button
                        onClick={() => toggleExpand(result.dataset)}
                        className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 text-sm transition-colors"
                      >
                        <Eye className="w-4 h-4" />

                        {isOpen ? "Hide Details" : "Show Details"}
                      </button>

                      <button className="flex items-center gap-2 px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 text-sm transition-colors">
                        <Download className="w-4 h-4" />
                        Download
                      </button>
                    </div>
                  </div>

                  {isOpen && detail && (
                    <div className="border-t border-gray-100 bg-gray-50 p-6 space-y-6">
                      {Object.values(detail.sheets).map((sheet) => (
                        <SheetSection
                          key={sheet.sheet_name}
                          sheet={sheet}
                        />
                      ))}
                    </div>
                  )}

                  {isOpen && !detail && (
                    <div className="border-t border-gray-100 p-6 text-center text-sm text-gray-500">
                      <div className="w-5 h-5 border-2 border-blue-600 border-t-transparent rounded-full animate-spin mx-auto mb-2" />
                      Loading…
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

function SheetSection({ sheet }: { sheet: SheetDetail }) {
  const gt = sheet.groundtruth;
  const hasGroundtruth = gt.personal_data_sensitive !== null;

  return (
    <div>
      <h4 className="font-semibold text-gray-800 mb-3 flex items-center gap-2">
        {sheet.sheet_name}

        {sheet.is_readme && (
          <span className="text-xs bg-gray-200 text-gray-600 px-2 py-0.5 rounded">
            README
          </span>
        )}
      </h4>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-4">
        {[
          {
            label: "Rows",
            value: sheet.n_records.toLocaleString(),
          },
          {
            label: "Personal PD",
            value: sheet.personal_data_sensitive
              ? "Sensitive"
              : "Clean",
          },
          {
            label: "Non-PD",
            value: sheet.non_personal_data_sensitive
              ? "Sensitive"
              : "Clean",
          },
          {
            label: "ISP",
            value: sheet.non_personal_data.isp_name ?? "N/A",
          },
        ].map(({ label, value }) => (
          <div
            key={label}
            className="p-3 bg-white border rounded-lg text-center"
          >
            <div className="text-sm font-semibold text-gray-900">
              {value}
            </div>

            <div className="text-xs text-gray-500 mt-0.5">
              {label}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}