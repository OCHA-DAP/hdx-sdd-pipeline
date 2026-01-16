"use client";
import { useEffect, useState } from "react";
import { Dataset } from "../types/dataset";

const MODEL_OPTIONS = [
    "gpt-5-nano",
    "gpt-5-mini",
    "gpt-4.1-nano",
    "DeepSeek-V3.1",
    "gpt-4.1-mini",
];

interface Props {
    datasets: Dataset[];
}

interface ModelPrediction {
    pii_entity_type?: string;
    pii_sensitive: boolean;
    pii_correct: boolean;
}

interface ColumnComparison {
    column_name: string;
    sample_values: string[];
    ground_truth: {
        pii_entity_type?: string;
        pii_sensitive: boolean;
    };
    model_predictions: Record<string, ModelPrediction>;
}

interface SheetComparison {
    sheet_name: string;
    n_records?: number;
    n_columns?: number;
    ground_truth: {
        pii_sensitive: boolean;
        non_pii_sensitive: boolean;
    };
    model_predictions: Record<string, {
        pii_sensitive: boolean;
        non_pii_sensitive: boolean;
        non_pii_sensitivity_explanation?: string;
        pii_correct: boolean;
        non_pii_correct: boolean;
    }>;
    columns: ColumnComparison[];
}

interface ComparisonData {
    dataset_filename: string;
    models: string[];
    sheets: SheetComparison[];
}

export default function AllModelsPredictionsTab({ datasets }: Props) {
    const [selectedDataset, setSelectedDataset] = useState<string | null>(null);
    const [comparison, setComparison] = useState<ComparisonData | null>(null);
    const [loading, setLoading] = useState(false);
    const [selectedSheetIndex, setSelectedSheetIndex] = useState(0);
    const [error, setError] = useState<string | null>(null);

    const fetchComparison = async (datasetName: string) => {
        setLoading(true);
        setError(null);

        try {
            const params = new URLSearchParams({
                dataset_filename: datasetName,
            });

            const response = await fetch(
                `http://localhost:8000/api/compare-models?${params}`,
                { method: "POST" }
            );

            if (response.ok) {
                const data: ComparisonData = await response.json();
                setComparison(data);
            } else {
                const errorData = await response.json();
                setError(errorData.detail || "Failed to fetch comparison data");
                setComparison(null);
            }
        } catch (err) {
            console.error("Error fetching comparison:", err);
            setError("Failed to fetch comparison data");
            setComparison(null);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        if (selectedDataset) {
            fetchComparison(selectedDataset);
            setSelectedSheetIndex(0);
        }
    }, [selectedDataset]);

    const renderBadge = (isCorrect: boolean, value: boolean | string | undefined, label?: string) => {
        if (value === undefined || value === null) {
            return (
                <span className="text-xs px-2 py-1 rounded bg-gray-500/30 text-gray-300">
                    N/A
                </span>
            );
        }

        const displayValue = label || (typeof value === 'boolean' ? (value ? 'Yes' : 'No') : value);

        return (
            <span
                className={`text-xs px-2 py-1 rounded font-medium ${isCorrect
                    ? "bg-green-500/30 text-green-200 border border-green-500/50"
                    : "bg-red-500/30 text-red-200 border border-red-500/50"
                    }`}
            >
                {displayValue}
            </span>
        );
    };

    return (
        <div className="flex gap-6 h-[calc(100vh-200px)]">
            {/* Left side - Dataset list */}
            <div className="w-1/3 border-r pr-6 overflow-y-auto">
                <h2 className="text-xl text-white font-semibold mb-4">Datasets</h2>

                {datasets.length === 0 ? (
                    <div className="text-sm text-white/80 border rounded-lg p-4">
                        Upload datasets to see results.
                    </div>
                ) : (
                    <div className="space-y-2">
                        {datasets.map((d) => {
                            const isSelected = selectedDataset === d.name;

                            return (
                                <button
                                    key={d.id}
                                    onClick={() => setSelectedDataset(d.name)}
                                    className={`w-full text-left border-2 rounded-lg p-3 transition-all duration-300 relative overflow-hidden ${isSelected
                                        ? "bg-white/10 text-white border-[#009edb] shadow-[0_0_15px_rgba(0,158,219,0.5)]"
                                        : "bg-white/10 text-white/80 border-white/20 hover:bg-white/20 hover:border-white/30"
                                        }`}
                                >
                                    <div className="flex items-center justify-between">
                                        <div className="flex-1 min-w-0">
                                            <h3 className={`font-medium truncate ${isSelected ? 'text-white' : ''}`}>
                                                {d.name}
                                            </h3>
                                        </div>
                                        <span className="text-xs px-2 py-1 rounded bg-gray-100 text-black ml-2 flex-shrink-0">
                                            {d.name.split(".").pop()?.toUpperCase()}
                                        </span>
                                    </div>
                                </button>
                            );
                        })}
                    </div>
                )}
            </div>

            {/* Right side - Model Comparison */}
            <div className="flex-1 overflow-y-auto overflow-x-visible">
                <h2 className="text-xl text-white font-semibold mb-4">Model Comparison</h2>

                {!selectedDataset ? (
                    <div className="text-sm text-white/80 border rounded-lg p-6">
                        Select a dataset to compare model predictions with ground truth.
                    </div>
                ) : loading ? (
                    <div className="flex items-center justify-center border rounded-lg p-8 bg-white/5 min-h-[400px]">
                        <div className="text-center">
                            <svg
                                className="animate-spin h-12 w-12 text-[#009edb] mx-auto mb-4"
                                xmlns="http://www.w3.org/2000/svg"
                                fill="none"
                                viewBox="0 0 24 24"
                            >
                                <circle
                                    className="opacity-25"
                                    cx="12"
                                    cy="12"
                                    r="10"
                                    stroke="currentColor"
                                    strokeWidth="4"
                                />
                                <path
                                    className="opacity-75"
                                    fill="currentColor"
                                    d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                                />
                            </svg>
                            <p className="text-white/80">Loading comparison...</p>
                        </div>
                    </div>
                ) : error ? (
                    <div className="border rounded-lg p-6 bg-red-500/10 border-red-500/30">
                        <p className="text-red-300">{error}</p>
                    </div>
                ) : !comparison ? (
                    <div className="text-sm text-white/80 border rounded-lg p-6">
                        No comparison data available.
                    </div>
                ) : (
                    <div className="space-y-6">
                        {/* Sheet tabs */}
                        {comparison.sheets.length > 1 && (
                            <div className="border-b border-white/20">
                                <div className="flex gap-2 overflow-x-auto">
                                    {comparison.sheets.map((sheet, idx) => (
                                        <button
                                            key={idx}
                                            onClick={() => setSelectedSheetIndex(idx)}
                                            className={`px-4 py-2 font-medium text-sm whitespace-nowrap transition ${selectedSheetIndex === idx
                                                ? 'border-b-2 border-[#009edb] font-semibold text-white'
                                                : 'text-white/60 hover:text-white/80'
                                                }`}
                                        >
                                            {sheet.sheet_name}
                                        </button>
                                    ))}
                                </div>
                            </div>
                        )}

                        {comparison.sheets[selectedSheetIndex] && (() => {
                            const sheet = comparison.sheets[selectedSheetIndex];

                            return (
                                <div className="space-y-6">
                                    {/* Sheet Summary */}
                                    <div className="border rounded-lg p-6 bg-white/5">
                                        <h3 className="text-lg font-semibold text-white mb-4">
                                            {comparison.sheets.length === 1 && `Sheet: ${sheet.sheet_name}`}
                                            {comparison.sheets.length === 1 || "Sheet Summary"}
                                        </h3>

                                        <div className="grid grid-cols-2 gap-4 text-sm mb-4">
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
                                        </div>

                                        {/* Overall Predictions Table */}
                                        <div className="overflow-x-visible">
                                            <table className="w-full text-sm">
                                                <thead>
                                                    <tr className="border-b border-white/20">
                                                        <th className="text-left py-2 px-3 text-white/80 font-medium">Model</th>
                                                        <th className="text-left py-2 px-3 text-white/80 font-medium">PII Sensitive</th>
                                                        <th className="text-left py-2 px-3 text-white/80 font-medium">Non-PII Sensitive</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    {/* Ground Truth Row */}
                                                    <tr className="border-b border-white/10 bg-blue-500/10">
                                                        <td className="py-2 px-3 font-semibold text-[#009edb]">Ground Truth</td>
                                                        <td className="py-2 px-3">
                                                            <span className="text-xs px-2 py-1 rounded bg-blue-500/30 text-blue-200 border border-blue-500/50">
                                                                {sheet.ground_truth.pii_sensitive ? 'Yes' : 'No'}
                                                            </span>
                                                        </td>
                                                        <td className="py-2 px-3">
                                                            <span className="text-xs px-2 py-1 rounded bg-blue-500/30 text-blue-200 border border-blue-500/50">
                                                                {sheet.ground_truth.non_pii_sensitive ? 'Yes' : 'No'}
                                                            </span>
                                                        </td>
                                                    </tr>

                                                    {/* Model Predictions */}
                                                    {comparison.models.map((model) => {
                                                        const pred = sheet.model_predictions[model];
                                                        if (!pred) return null;

                                                        return (
                                                            <tr key={model} className="border-b border-white/10 hover:bg-white/5">
                                                                <td className="py-2 px-3 text-white font-medium">{model}</td>
                                                                <td className="py-2 px-3">
                                                                    {renderBadge(pred.pii_correct, pred.pii_sensitive)}
                                                                </td>
                                                                <td className="py-2 px-3">
                                                                    <div className="relative group inline-block">
                                                                        {renderBadge(pred.non_pii_correct, pred.non_pii_sensitive)}
                                                                        {pred.non_pii_sensitivity_explanation && (
                                                                            <div className="absolute bottom-full left-1/2 transform -translate-x-1/2 mb-2 px-4 py-3 bg-gray-900 text-white text-xs rounded-lg shadow-xl opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none max-w-md w-max z-50 border border-gray-700">
                                                                                <div className="font-semibold mb-2 text-blue-300">Non-PII Sensitivity Explanation:</div>
                                                                                <div className="text-gray-200 leading-relaxed whitespace-normal break-words">{pred.non_pii_sensitivity_explanation}</div>
                                                                                <div className="absolute top-full left-1/2 transform -translate-x-1/2 -mt-px">
                                                                                    <div className="w-0 h-0 border-l-8 border-r-8 border-t-8 border-transparent border-t-gray-900"></div>
                                                                                </div>
                                                                            </div>
                                                                        )}
                                                                    </div>
                                                                </td>
                                                            </tr>
                                                        );
                                                    })}
                                                </tbody>
                                            </table>
                                        </div>
                                    </div>

                                    {/* Columns Comparison */}
                                    {sheet.columns && sheet.columns.length > 0 && (
                                        <div className="border rounded-lg p-6 bg-white/5">
                                            <h4 className="font-semibold text-white mb-4">Column-Level Predictions</h4>

                                            <div className="space-y-4">
                                                {sheet.columns.map((col, colIdx) => (
                                                    <div key={colIdx} className="border rounded-lg p-4 bg-white/5">
                                                        {/* Column Header */}
                                                        <div className="mb-3">
                                                            <h5 className="font-medium text-white text-base mb-2">{col.column_name}</h5>

                                                            {/* Sample Values */}
                                                            {col.sample_values && col.sample_values.length > 0 && (
                                                                <div className="mb-3">
                                                                    <p className="text-xs text-white/60 mb-1">Sample values:</p>
                                                                    <div className="flex flex-wrap gap-1">
                                                                        {col.sample_values.slice(0, 3).map((val, valIdx) => (
                                                                            <span
                                                                                key={valIdx}
                                                                                className="text-xs px-2 py-1 rounded bg-gray-700 text-white/80"
                                                                            >
                                                                                {String(val).substring(0, 20)}
                                                                                {String(val).length > 20 ? '...' : ''}
                                                                            </span>
                                                                        ))}
                                                                    </div>
                                                                </div>
                                                            )}
                                                        </div>

                                                        {/* Predictions Table */}
                                                        <div className="overflow-x-auto">
                                                            <table className="w-full text-xs">
                                                                <thead>
                                                                    <tr className="border-b border-white/20">
                                                                        <th className="text-left py-2 px-2 text-white/70 font-medium w-32">Model</th>
                                                                        <th className="text-left py-2 px-2 text-white/70 font-medium">PII Type</th>
                                                                        <th className="text-left py-2 px-2 text-white/70 font-medium">PII Sensitive</th>
                                                                    </tr>
                                                                </thead>
                                                                <tbody>
                                                                    {/* Ground Truth */}
                                                                    <tr className="border-b border-white/10 bg-blue-500/10">
                                                                        <td className="py-2 px-2 font-semibold text-[#009edb]">Ground Truth</td>
                                                                        <td className="py-2 px-2">
                                                                            <span className="text-xs px-2 py-1 rounded bg-blue-500/30 text-blue-200">
                                                                                {col.ground_truth.pii_entity_type || 'N/A'}
                                                                            </span>
                                                                        </td>
                                                                        <td className="py-2 px-2">
                                                                            <span className="text-xs px-2 py-1 rounded bg-blue-500/30 text-blue-200">
                                                                                {col.ground_truth.pii_sensitive ? 'Yes' : 'No'}
                                                                            </span>
                                                                        </td>
                                                                    </tr>

                                                                    {/* Model Predictions */}
                                                                    {comparison.models.map((model) => {
                                                                        const pred = col.model_predictions[model];
                                                                        if (!pred) {
                                                                            return (
                                                                                <tr key={model} className="border-b border-white/10">
                                                                                    <td className="py-2 px-2 text-white/60">{model}</td>
                                                                                    <td className="py-2 px-2 text-white/40" colSpan={2}>No data</td>
                                                                                </tr>
                                                                            );
                                                                        }

                                                                        return (
                                                                            <tr key={model} className="border-b border-white/10 hover:bg-white/5">
                                                                                <td className="py-2 px-2 text-white font-medium">{model}</td>
                                                                                <td className="py-2 px-2">
                                                                                    <span className="text-xs px-2 py-1 rounded bg-gray-600/30 text-gray-200">
                                                                                        {pred.pii_entity_type || 'N/A'}
                                                                                    </span>
                                                                                </td>
                                                                                <td className="py-2 px-2">
                                                                                    {renderBadge(pred.pii_correct, pred.pii_sensitive)}
                                                                                </td>
                                                                            </tr>
                                                                        );
                                                                    })}
                                                                </tbody>
                                                            </table>
                                                        </div>
                                                    </div>
                                                ))}
                                            </div>
                                        </div>
                                    )}
                                </div>
                            );
                        })()}
                    </div>
                )}
            </div>
        </div>
    );
}
