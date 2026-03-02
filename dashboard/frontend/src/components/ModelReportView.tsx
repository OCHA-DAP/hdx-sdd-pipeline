"use client";
import { useState } from "react";

interface ModelReportViewProps {
  report: {
    sensitivity?: string;
    report?: Array<{
      sheet_name: string;
      processing_success: boolean;
      n_records?: number;
      n_columns?: number;
      personal_data_sensitive?: boolean;
      non_personal_data_sensitive?: boolean;
      error_source?: string;
      error_message?: string;
      columns?: Array<{
        column_name: string;
        sample_values: string[];
        pii?: {
          entity_type?: string;
          sensitive?: boolean;
        };
        non_pii?: {
          sensitivity?: string;
        };
      }>;
    }>;
  };
}

export function ModelReportView({ report }: ModelReportViewProps) {
  const [selectedSheetIndex, setSelectedSheetIndex] = useState(0);

  if (!report || !Array.isArray(report.report)) {
    return (
      <div className="text-sm text-white/70">
        No report data available.
      </div>
    );
  }

  const sheets = report.report;
  const sheet = sheets[selectedSheetIndex];

  return (
    <div className="space-y-4">
      {/* Overall sensitivity */}
      {report.sensitivity && (
        <div className="border rounded-lg p-4 bg-white/10">
          <h3 className="font-semibold text-white mb-1">
            Overall Sensitivity
          </h3>
          <p className="text-lg font-medium text-[#009edb]">
            {report.sensitivity}
          </p>
        </div>
      )}

      {/* Sheet tabs */}
      {sheets.length > 1 && (
        <div className="border-b border-white/20">
          <div className="flex gap-2 overflow-x-auto">
            {sheets.map((s, idx) => (
              <button
                key={idx}
                onClick={() => setSelectedSheetIndex(idx)}
                className={`px-4 py-2 text-sm font-medium transition whitespace-nowrap ${
                  selectedSheetIndex === idx
                    ? "border-b-2 border-[#009edb] text-white"
                    : "text-white/60 hover:text-white/80"
                }`}
              >
                {s.sheet_name}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Sheet summary */}
      <div className="border rounded-lg p-4 bg-white/5">
        {sheets.length === 1 && (
          <h3 className="text-lg font-semibold text-white mb-2">
            Sheet: {sheet.sheet_name}
          </h3>
        )}

        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm mb-3">
          {sheet.n_records !== undefined && (
            <div>
              <span className="text-white/60">Records:</span>
              <span className="text-white ml-2">{sheet.n_records}</span>
            </div>
          )}
          {sheet.n_columns !== undefined && (
            <div>
              <span className="text-white/60">Columns:</span>
              <span className="text-white ml-2">{sheet.n_columns}</span>
            </div>
          )}
          <div>
            <span className="text-white/60">PII Sensitive:</span>
            <span
              className={`ml-2 ${
                sheet.personal_data_sensitive ? "text-red-400" : "text-green-400"
              }`}
            >
              {sheet.personal_data_sensitive ? "Yes" : "No"}
            </span>
          </div>
          <div>
            <span className="text-white/60">Non-PII Sensitive:</span>
            <span
              className={`ml-2 ${
                sheet.non_personal_data_sensitive ? "text-red-400" : "text-green-400"
              }`}
            >
              {sheet.non_personal_data_sensitive ? "Yes" : "No"}
            </span>
          </div>
        </div>

        {/* Processing error */}
        {sheet.processing_success === false && (
          <div className="mt-2 p-3 bg-red-500/20 border border-red-500/40 rounded">
            <p className="text-sm text-red-300">
              <strong>Error:</strong>{" "}
              {sheet.error_source || "Unknown error"}
            </p>
            {sheet.error_message && (
              <p className="text-xs text-red-200 mt-1">
                {sheet.error_message}
              </p>
            )}
          </div>
        )}

        {/* Columns */}
        {sheet.columns && sheet.columns.length > 0 && (
          <div className="mt-4">
            <h4 className="font-semibold text-white mb-2">Columns</h4>

            <div className="space-y-2">
              {sheet.columns.map((col, idx) => (
                <div
                  key={idx}
                  className="border rounded p-3 bg-white/5"
                >
                  <div className="flex items-center justify-between mb-2">
                    <span className="font-medium text-white">
                      {col.column_name}
                    </span>

                    <div className="flex gap-2 flex-wrap">
                      {col.pii?.entity_type && (
                        <span className="text-xs px-2 py-1 rounded bg-blue-500/30 text-blue-200">
                          PII: {col.pii.entity_type}
                        </span>
                      )}
                      {col.pii?.sensitive && (
                        <span className="text-xs px-2 py-1 rounded bg-red-500/30 text-red-200">
                          Sensitive
                        </span>
                      )}
                      {col.non_pii?.sensitivity && (
                        <span className="text-xs px-2 py-1 rounded bg-yellow-500/30 text-yellow-200">
                          {col.non_pii.sensitivity}
                        </span>
                      )}
                    </div>
                  </div>

                  {col.sample_values?.length > 0 && (
                    <div>
                      <p className="text-xs text-white/60 mb-1">
                        Sample values:
                      </p>
                      <div className="flex flex-wrap gap-1">
                        {col.sample_values.slice(0, 5).map((v, i) => (
                          <span
                            key={i}
                            className="text-xs px-2 py-1 rounded bg-gray-700 text-white/80"
                          >
                            {String(v).substring(0, 30)}
                            {String(v).length > 30 && "..."}
                          </span>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
