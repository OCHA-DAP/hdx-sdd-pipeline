"use client";
import { useState, useEffect } from "react";
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
    sheet_level_pii: Metrics;
    sheet_level_non_personal_data: Metrics;
}

interface StatisticsResponse {
    models: Record<string, ModelStatistics>;
    available_models: string[];
}

export default function InsightsTab() {
    const [statistics, setStatistics] = useState<StatisticsResponse | null>(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        fetchStatistics();
    }, []);

    const fetchStatistics = async () => {
        setLoading(true);
        try {
            const response = await fetch(getApiUrl('api/statistics'));
            if (response.ok) {
                const data: StatisticsResponse = await response.json();
                setStatistics(data);
            }
        } catch (error) {
            console.error("Error fetching statistics:", error);
        } finally {
            setLoading(false);
        }
    };

    if (loading) {
        return (
            <div className="flex items-center justify-center min-h-[600px]">
                <div className="text-center">
                    <svg
                        className="animate-spin h-16 w-16 text-white mx-auto mb-4"
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
                    <p className="text-white text-lg">Analyzing LLM Performance...</p>
                </div>
            </div>
        );
    }

    if (!statistics || statistics.available_models.length === 0) {
        return (
            <div className="text-white border border-white/20 rounded-lg p-8 text-center">
                <p className="text-xl">No data available. Please generate model predictions first.</p>
            </div>
        );
    }

    // Calculate insights
    const models = statistics.available_models;
    const modelStats = models.map(model => ({
        name: model,
        stats: statistics.models[model]
    })).filter(m => !m.stats.file_level.error);

    // Find best performing model
    // Determine which metric to use for ranking
    const maxF1 = Math.max(...modelStats.map(m => m.stats.file_level.f1));
    const maxRecall = Math.max(...modelStats.map(m => m.stats.file_level.recall));
    const maxPrecision = Math.max(...modelStats.map(m => m.stats.file_level.precision));
    
    let rankingMetric: 'f1' | 'recall' | 'precision' | 'accuracy' = 'f1';
    if (maxF1 === 0) {
        if (maxRecall > 0) {
            rankingMetric = 'recall';
        } else if (maxPrecision > 0) {
            rankingMetric = 'precision';
        } else {
            rankingMetric = 'accuracy';
        }
    }

    // Find best performing model based on the determined metric
    const bestModel = modelStats.reduce((best, current) => {
        const bestScore = best.stats.file_level[rankingMetric];
        const currentScore = current.stats.file_level[rankingMetric];
        return currentScore > bestScore ? current : best;
    }, modelStats[0]);

    // Calculate average performance
    const avgAccuracy = modelStats.reduce((sum, m) => sum + m.stats.file_level.accuracy, 0) / modelStats.length;

    // Calculate false negative rate (most critical - missing sensitive data)
    const totalFalseNegatives = modelStats.reduce((sum, m) =>
        sum + m.stats.file_level.confusion_matrix.false_negative, 0
    );
    const totalActualPositives = modelStats.reduce((sum, m) =>
        sum + m.stats.file_level.confusion_matrix.true_positive + m.stats.file_level.confusion_matrix.false_negative, 0
    );
    const avgFalseNegativeRate = totalActualPositives > 0 ? totalFalseNegatives / totalActualPositives : 0;

    const formatPercent = (value: number) => `${(value * 100).toFixed(1)}%`;

    return (
        <div className="space-y-8 max-w-7xl mx-auto">
            {/* Hero Section */}
            <div className="relative overflow-hidden rounded-2xl bg-gradient-to-br from-blue-600/20 via-purple-600/20 to-pink-600/20 border border-white/20 p-8 md:p-12">
                <div className="relative z-10">
                    <h1 className="text-4xl md:text-5xl font-bold text-white mb-4">
                        🎯 LLM Performance Insights
                    </h1>
                    <p className="text-xl text-white/80 max-w-3xl">
                        Understanding how our AI models detect sensitive data in humanitarian datasets
                    </p>
                </div>
                <div className="absolute top-0 right-0 w-64 h-64 bg-blue-500/10 rounded-full blur-3xl"></div>
                <div className="absolute bottom-0 left-0 w-64 h-64 bg-purple-500/10 rounded-full blur-3xl"></div>
            </div>

            {/* Executive Summary */}
            <div className="grid md:grid-cols-4 gap-4">
                <div className="bg-gradient-to-br from-green-500/20 to-green-600/20 border border-green-500/30 rounded-xl p-6 backdrop-blur-sm">
                    <div className="flex items-center gap-3 mb-2">
                        <div className="text-3xl">🏆</div>
                        <h3 className="text-sm font-medium text-white/70">Best Model</h3>
                    </div>
                    <p className="text-2xl font-bold text-white mb-1">{bestModel.name}</p>
                    <p className="text-sm text-green-300">{formatPercent(bestModel.stats.file_level.accuracy)} accuracy</p>
                </div>

                <div className="bg-gradient-to-br from-blue-500/20 to-blue-600/20 border border-blue-500/30 rounded-xl p-6 backdrop-blur-sm">
                    <div className="flex items-center gap-3 mb-2">
                        <div className="text-3xl">📊</div>
                        <h3 className="text-sm font-medium text-white/70">Models Tested</h3>
                    </div>
                    <p className="text-2xl font-bold text-white mb-1">{modelStats.length}</p>
                    <p className="text-sm text-blue-300">Avg accuracy: {formatPercent(avgAccuracy)}</p>
                </div>

                <div className="bg-gradient-to-br from-purple-500/20 to-purple-600/20 border border-purple-500/30 rounded-xl p-6 backdrop-blur-sm">
                    <div className="flex items-center gap-3 mb-2">
                        <div className="text-3xl">📁</div>
                        <h3 className="text-sm font-medium text-white/70">Files Analyzed</h3>
                    </div>
                    <p className="text-2xl font-bold text-white mb-1">{bestModel.stats.file_level.total_files || 0}</p>
                    <p className="text-sm text-purple-300">Humanitarian datasets</p>
                </div>

                <div className="bg-gradient-to-br from-orange-500/20 to-orange-600/20 border border-orange-500/30 rounded-xl p-6 backdrop-blur-sm">
                    <div className="flex items-center gap-3 mb-2">
                        <div className="text-3xl">⚠️</div>
                        <h3 className="text-sm font-medium text-white/70">Miss Rate</h3>
                    </div>
                    <p className="text-2xl font-bold text-white mb-1">{formatPercent(avgFalseNegativeRate)}</p>
                    <p className="text-sm text-orange-300">Sensitive data missed</p>
                </div>
            </div>

            {/* The Story Section */}
            <div className="bg-white/5 border border-white/20 rounded-xl p-8 backdrop-blur-sm">
                <h2 className="text-3xl font-bold text-white mb-6 flex items-center gap-3">
                    <span>📖</span> The Story: What We Learned
                </h2>

                <div className="space-y-6">
                    <div className="border-l-4 border-blue-500 pl-6 py-2">
                        <h3 className="text-xl font-semibold text-white mb-2">🎯 Overall Performance</h3>
                        <p className="text-white/80 text-lg leading-relaxed">
                            We tested <strong className="text-white">{modelStats.length} different AI models</strong> to see how well they can
                            automatically detect sensitive information in humanitarian data. On average, the models achieved{" "}
                            <strong className="text-green-400">{formatPercent(avgAccuracy)} accuracy</strong>, which means they correctly
                            identified whether files contain sensitive data about {(avgAccuracy * 100).toFixed(0)} times out of 100.
                        </p>
                    </div>

                    <div className="border-l-4 border-green-500 pl-6 py-2">
                        <h3 className="text-xl font-semibold text-white mb-2">✅ What They're Good At</h3>
                        <p className="text-white/80 text-lg leading-relaxed mb-3">
                            The best performing model, <strong className="text-green-400">{bestModel.name}</strong>, correctly identified
                            sensitive files with <strong className="text-green-400">{formatPercent(bestModel.stats.file_level.precision)} precision</strong>.
                            This means when it says a file is sensitive, it's usually right.
                        </p>
                        <div className="grid md:grid-cols-2 gap-4 mt-4">
                            <div className="bg-green-500/10 border border-green-500/30 rounded-lg p-4">
                                <div className="text-green-400 font-semibold mb-1">True Positives</div>
                                <div className="text-3xl font-bold text-white">{bestModel.stats.file_level.confusion_matrix.true_positive}</div>
                                <div className="text-sm text-white/60 mt-1">Correctly identified sensitive files</div>
                            </div>
                            <div className="bg-green-500/10 border border-green-500/30 rounded-lg p-4">
                                <div className="text-green-400 font-semibold mb-1">True Negatives</div>
                                <div className="text-3xl font-bold text-white">{bestModel.stats.file_level.confusion_matrix.true_negative}</div>
                                <div className="text-sm text-white/60 mt-1">Correctly identified non-sensitive files</div>
                            </div>
                        </div>
                    </div>

                    <div className="border-l-4 border-orange-500 pl-6 py-2">
                        <h3 className="text-xl font-semibold text-white mb-2">⚠️ Where They Struggle</h3>
                        <p className="text-white/80 text-lg leading-relaxed mb-3">
                            The most critical issue is <strong className="text-orange-400">false negatives</strong> - when the model
                            misses sensitive data that should be protected. Across all models, this happened{" "}
                            <strong className="text-orange-400">{formatPercent(avgFalseNegativeRate)}</strong> of the time.
                        </p>
                        <div className="grid md:grid-cols-2 gap-4 mt-4">
                            <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-4">
                                <div className="text-red-400 font-semibold mb-1">False Negatives ❌</div>
                                <div className="text-3xl font-bold text-white">{bestModel.stats.file_level.confusion_matrix.false_negative}</div>
                                <div className="text-sm text-white/60 mt-1">Missed sensitive files (HIGH RISK)</div>
                            </div>
                            <div className="bg-orange-500/10 border border-orange-500/30 rounded-lg p-4">
                                <div className="text-orange-400 font-semibold mb-1">False Positives ⚠️</div>
                                <div className="text-3xl font-bold text-white">{bestModel.stats.file_level.confusion_matrix.false_positive}</div>
                                <div className="text-sm text-white/60 mt-1">Incorrectly flagged as sensitive</div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Model Comparison */}
            <div className="bg-white/5 border border-white/20 rounded-xl p-8 backdrop-blur-sm">
                <h2 className="text-3xl font-bold text-white mb-6 flex items-center gap-3">
                    <span>🔬</span> Model Comparison
                </h2>

                <div className="space-y-4">
                    {modelStats.map((model, idx) => {
                        const accuracy = model.stats.file_level.accuracy;
                        const isTopPerformer = model.name === bestModel.name;

                        return (
                            <div
                                key={model.name}
                                className={`border rounded-xl p-6 transition-all ${isTopPerformer
                                        ? 'bg-gradient-to-r from-yellow-500/20 to-orange-500/20 border-yellow-500/50'
                                        : 'bg-white/5 border-white/20 hover:border-white/40'
                                    }`}
                            >
                                <div className="flex items-center justify-between mb-4">
                                    <div className="flex items-center gap-3">
                                        {isTopPerformer && <span className="text-2xl">🏆</span>}
                                        <h3 className="text-xl font-bold text-white">{model.name}</h3>
                                        {isTopPerformer && (
                                            <span className="px-3 py-1 bg-yellow-500/30 text-yellow-200 rounded-full text-sm font-medium">
                                                Top Performer
                                            </span>
                                        )}
                                    </div>
                                    <div className="text-right">
                                        <div className="text-3xl font-bold text-white">{formatPercent(accuracy)}</div>
                                        <div className="text-sm text-white/60">Accuracy</div>
                                    </div>
                                </div>

                                {/* Performance bar */}
                                <div className="mb-4">
                                    <div className="h-3 bg-white/10 rounded-full overflow-hidden">
                                        <div
                                            className={`h-full rounded-full transition-all ${accuracy > 0.9 ? 'bg-gradient-to-r from-green-500 to-green-400' :
                                                    accuracy > 0.8 ? 'bg-gradient-to-r from-blue-500 to-blue-400' :
                                                        accuracy > 0.7 ? 'bg-gradient-to-r from-yellow-500 to-yellow-400' :
                                                            'bg-gradient-to-r from-orange-500 to-red-400'
                                                }`}
                                            style={{ width: `${accuracy * 100}%` }}
                                        ></div>
                                    </div>
                                </div>

                                {/* Key metrics */}
                                <div className="grid grid-cols-4 gap-4">
                                    <div>
                                        <div className="text-xs text-white/60 mb-1">Precision</div>
                                        <div className="text-lg font-semibold text-white">
                                            {formatPercent(model.stats.file_level.precision)}
                                        </div>
                                    </div>
                                    <div>
                                        <div className="text-xs text-white/60 mb-1">Recall</div>
                                        <div className="text-lg font-semibold text-white">
                                            {formatPercent(model.stats.file_level.recall)}
                                        </div>
                                    </div>
                                    <div>
                                        <div className="text-xs text-white/60 mb-1">F1 Score</div>
                                        <div className="text-lg font-semibold text-white">
                                            {model.stats.file_level.f1.toFixed(3)}
                                        </div>
                                    </div>
                                    <div>
                                        <div className="text-xs text-white/60 mb-1">Errors</div>
                                        <div className="text-lg font-semibold text-red-400">
                                            {model.stats.file_level.misclassifications.length}
                                        </div>
                                    </div>
                                </div>
                            </div>
                        );
                    })}
                </div>
            </div>

            {/* Real-World Impact */}
            <div className="bg-gradient-to-br from-purple-500/20 to-blue-500/20 border border-purple-500/30 rounded-xl p-8 backdrop-blur-sm">
                <h2 className="text-3xl font-bold text-white mb-6 flex items-center gap-3">
                    <span>🌍</span> Real-World Impact
                </h2>

                <div className="grid md:grid-cols-2 gap-6">
                    <div className="bg-black/20 rounded-lg p-6">
                        <h3 className="text-xl font-semibold text-white mb-3 flex items-center gap-2">
                            <span className="text-2xl">✅</span> What This Means for Success
                        </h3>
                        <ul className="space-y-3 text-white/80">
                            <li className="flex items-start gap-2">
                                <span className="text-green-400 mt-1">•</span>
                                <span>
                                    <strong className="text-white">Automated Protection:</strong> The models can automatically scan
                                    and flag sensitive humanitarian data, saving hours of manual review.
                                </span>
                            </li>
                            <li className="flex items-start gap-2">
                                <span className="text-green-400 mt-1">•</span>
                                <span>
                                    <strong className="text-white">Faster Response:</strong> With {formatPercent(avgAccuracy)} average
                                    accuracy, teams can quickly identify which datasets need extra protection.
                                </span>
                            </li>
                            <li className="flex items-start gap-2">
                                <span className="text-green-400 mt-1">•</span>
                                <span>
                                    <strong className="text-white">Scalability:</strong> These models can process thousands of files
                                    in minutes, something impossible to do manually.
                                </span>
                            </li>
                        </ul>
                    </div>

                    <div className="bg-black/20 rounded-lg p-6">
                        <h3 className="text-xl font-semibold text-white mb-3 flex items-center gap-2">
                            <span className="text-2xl">⚠️</span> Important Limitations
                        </h3>
                        <ul className="space-y-3 text-white/80">
                            <li className="flex items-start gap-2">
                                <span className="text-orange-400 mt-1">•</span>
                                <span>
                                    <strong className="text-white">Not Perfect:</strong> With a {formatPercent(avgFalseNegativeRate)} miss
                                    rate, some sensitive data may slip through undetected.
                                </span>
                            </li>
                            <li className="flex items-start gap-2">
                                <span className="text-orange-400 mt-1">•</span>
                                <span>
                                    <strong className="text-white">Human Review Still Needed:</strong> Critical datasets should still
                                    be reviewed by data protection experts.
                                </span>
                            </li>
                            <li className="flex items-start gap-2">
                                <span className="text-orange-400 mt-1">•</span>
                                <span>
                                    <strong className="text-white">Context Matters:</strong> Models may struggle with nuanced cases
                                    that require domain expertise.
                                </span>
                            </li>
                        </ul>
                    </div>
                </div>
            </div>

            {/* Recommendations */}
            <div className="bg-gradient-to-br from-green-500/20 to-teal-500/20 border border-green-500/30 rounded-xl p-8 backdrop-blur-sm">
                <h2 className="text-3xl font-bold text-white mb-6 flex items-center gap-3">
                    <span>💡</span> Recommendations
                </h2>

                <div className="space-y-4">
                    <div className="flex items-start gap-4 bg-black/20 rounded-lg p-5">
                        <div className="text-3xl">1️⃣</div>
                        <div>
                            <h3 className="text-xl font-semibold text-white mb-2">Use {bestModel.name} as Primary Classifier</h3>
                            <p className="text-white/80">
                                With {formatPercent(bestModel.stats.file_level.accuracy)} accuracy and the best {rankingMetric.toUpperCase().replace('_', ' ')} score
                                {rankingMetric === 'f1' ? ` (${bestModel.stats.file_level.f1.toFixed(3)})` : 
                                 rankingMetric === 'recall' ? ` (${formatPercent(bestModel.stats.file_level.recall)})` :
                                 rankingMetric === 'precision' ? ` (${formatPercent(bestModel.stats.file_level.precision)})` :
                                 ` (${formatPercent(bestModel.stats.file_level.accuracy)})`}, this model should be your go-to for automated screening.
                            </p>
                        </div>
                    </div>

                    <div className="flex items-start gap-4 bg-black/20 rounded-lg p-5">
                        <div className="text-3xl">2️⃣</div>
                        <div>
                            <h3 className="text-xl font-semibold text-white mb-2">Implement a Two-Stage Review Process</h3>
                            <p className="text-white/80">
                                Use AI for initial screening, then have human experts review all files flagged as sensitive
                                or borderline cases to catch the {formatPercent(avgFalseNegativeRate)} that might be missed.
                            </p>
                        </div>
                    </div>

                    <div className="flex items-start gap-4 bg-black/20 rounded-lg p-5">
                        <div className="text-3xl">3️⃣</div>
                        <div>
                            <h3 className="text-xl font-semibold text-white mb-2">Continuous Improvement</h3>
                            <p className="text-white/80">
                                Collect feedback on misclassifications and use them to fine-tune the models. Each correction
                                makes the system smarter and more reliable.
                            </p>
                        </div>
                    </div>

                    <div className="flex items-start gap-4 bg-black/20 rounded-lg p-5">
                        <div className="text-3xl">4️⃣</div>
                        <div>
                            <h3 className="text-xl font-semibold text-white mb-2">Monitor Performance Over Time</h3>
                            <p className="text-white/80">
                                Track accuracy metrics monthly to ensure the models maintain their performance as new types
                                of humanitarian data are processed.
                            </p>
                        </div>
                    </div>
                </div>
            </div>

            {/* Technical Deep Dive (Collapsible) */}
            <details className="bg-white/5 border border-white/20 rounded-xl p-8 backdrop-blur-sm">
                <summary className="text-2xl font-bold text-white cursor-pointer hover:text-white/80 transition">
                    🔍 Technical Deep Dive (Click to expand)
                </summary>

                <div className="mt-6 space-y-6">
                    <div>
                        <h3 className="text-xl font-semibold text-white mb-3">Understanding the Metrics</h3>
                        <div className="grid md:grid-cols-2 gap-4">
                            <div className="bg-black/20 rounded-lg p-4">
                                <h4 className="font-semibold text-blue-400 mb-2">Accuracy</h4>
                                <p className="text-white/70 text-sm">
                                    Percentage of all predictions (both sensitive and non-sensitive) that were correct.
                                    Good for overall performance but can be misleading with imbalanced datasets.
                                </p>
                            </div>
                            <div className="bg-black/20 rounded-lg p-4">
                                <h4 className="font-semibold text-blue-400 mb-2">Precision</h4>
                                <p className="text-white/70 text-sm">
                                    When the model says data is sensitive, how often is it right? High precision means
                                    fewer false alarms.
                                </p>
                            </div>
                            <div className="bg-black/20 rounded-lg p-4">
                                <h4 className="font-semibold text-blue-400 mb-2">Recall</h4>
                                <p className="text-white/70 text-sm">
                                    Of all the actually sensitive files, how many did the model catch? High recall means
                                    fewer sensitive files slip through.
                                </p>
                            </div>
                            <div className="bg-black/20 rounded-lg p-4">
                                <h4 className="font-semibold text-blue-400 mb-2">F1 Score</h4>
                                <p className="text-white/70 text-sm">
                                    Harmonic mean of precision and recall. Best single metric for balanced performance.
                                    Ranges from 0 (worst) to 1 (perfect).
                                </p>
                            </div>
                        </div>
                    </div>

                    <div>
                        <h3 className="text-xl font-semibold text-white mb-3">Confusion Matrix Explained</h3>
                        <div className="bg-black/20 rounded-lg p-6">
                            <div className="grid grid-cols-2 gap-4 mb-4">
                                <div className="bg-green-500/20 border border-green-500/50 rounded p-4">
                                    <div className="font-semibold text-green-400 mb-1">True Negative (TN)</div>
                                    <div className="text-white/70 text-sm">
                                        Non-sensitive file correctly identified as non-sensitive ✅
                                    </div>
                                </div>
                                <div className="bg-red-500/20 border border-red-500/50 rounded p-4">
                                    <div className="font-semibold text-red-400 mb-1">False Positive (FP)</div>
                                    <div className="text-white/70 text-sm">
                                        Non-sensitive file incorrectly flagged as sensitive ⚠️
                                    </div>
                                </div>
                                <div className="bg-orange-500/20 border border-orange-500/50 rounded p-4">
                                    <div className="font-semibold text-orange-400 mb-1">False Negative (FN)</div>
                                    <div className="text-white/70 text-sm">
                                        Sensitive file missed - NOT flagged as sensitive ❌ (MOST CRITICAL)
                                    </div>
                                </div>
                                <div className="bg-blue-500/20 border border-blue-500/50 rounded p-4">
                                    <div className="font-semibold text-blue-400 mb-1">True Positive (TP)</div>
                                    <div className="text-white/70 text-sm">
                                        Sensitive file correctly identified as sensitive ✅
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </details>
        </div>
    );
}
