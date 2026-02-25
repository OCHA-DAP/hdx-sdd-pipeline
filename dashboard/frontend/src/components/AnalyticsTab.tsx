"use client";
import { useState, useEffect } from "react";
import { BarChart3, TrendingUp, Users, FileText, Activity, RefreshCw, Download } from "lucide-react";
import { getApiUrl } from "../services/api";
import jsPDF from 'jspdf';
import html2canvas from 'html2canvas';

interface ModelPerformance {
  model: string;
  accuracy: number;
  precision: number;
  recall: number;
  f1_score: number;
  files_tested: number;
  true_positives: number;
  false_positives: number;
  true_negatives: number;
  false_negatives: number;
}

interface SheetModelPerformance {
  model: string;
  accuracy: number;
  precision: number;
  recall: number;
  f1_score: number;
  sheets_tested: number;
  true_positives: number;
  false_positives: number;
  true_negatives: number;
  false_negatives: number;
}

interface CostAnalysis {
  model: string;
  reports: number;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  price_per_1m: number;
  total_cost: number;
  cost_per_report: number;
}

export default function AnalyticsTab() {
  const [batchStatus, setBatchStatus] = useState<any>(null);
  const [datasets, setDatasets] = useState<string[]>([]);
  const [models, setModels] = useState<string[]>([]);
  const [overallPerformance, setOverallPerformance] = useState<ModelPerformance[]>([]);
  const [personalSensitive, setPersonalSensitive] = useState<ModelPerformance[]>([]);
  const [nonPersonalSensitive, setNonPersonalSensitive] = useState<ModelPerformance[]>([]);
  const [sheetPersonalSensitive, setSheetPersonalSensitive] = useState<SheetModelPerformance[]>([]);
  const [sheetNonPersonalSensitive, setSheetNonPersonalSensitive] = useState<SheetModelPerformance[]>([]);
  const [costAnalysis, setCostAnalysis] = useState<CostAnalysis[]>([]);
  const [loading, setLoading] = useState(false);


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

  const fetchAnalytics = async () => {
    setLoading(true);
    try {
      // Fetch real performance metrics from the API
      const performanceResponse = await fetch(getApiUrl("api/analytics/performance"));
      const costResponse = await fetch(getApiUrl("api/analytics/cost"));
      
      if (performanceResponse.ok && costResponse.ok) {
        const performanceData = await performanceResponse.json();
        const costData = await costResponse.json();
        
        setOverallPerformance(performanceData.overall_performance || []);
        setPersonalSensitive(performanceData.personal_sensitive || []);
        setNonPersonalSensitive(performanceData.non_personal_sensitive || []);
        setSheetPersonalSensitive(performanceData.sheet_personal_sensitive || []);
        setSheetNonPersonalSensitive(performanceData.sheet_non_personal_sensitive || []);
        setCostAnalysis(costData.cost_analysis || []);
      }
    } catch (error) {
      console.error("Failed to fetch analytics:", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchDatasets();
    fetchModels();
  }, []);

  useEffect(() => {
    if (datasets.length > 0 && models.length > 0) {
      fetchAnalytics();
    }
  }, [datasets, models]);

  const renderPerformanceTable = (title: string, data: ModelPerformance[] | SheetModelPerformance[], testedType: string) => {
    // Find the model with the highest F1 score
    const bestF1Model = data.length > 0 ? data.reduce((best, current) => 
      current.f1_score > best.f1_score ? current : best
    ).model : null;

    return (
      <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">{title}</h3>
        <div className="border border-gray-200 rounded-lg overflow-hidden">
          <table className="w-full">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-2 text-left text-xs font-medium text-gray-900">Model</th>
                <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">Accuracy</th>
                <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">Precision</th>
                <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">Recall</th>
                <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">F1 Score</th>
                <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">True Positives</th>
                <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">False Positives</th>
                <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">True Negatives</th>
                <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">False Negatives</th>
                <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">{testedType}</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {data.map((perf) => (
                <tr key={perf.model} className={perf.model === bestF1Model ? "bg-green-50" : "hover:bg-gray-50"}>
                  <td className="px-4 py-2 text-xs font-medium text-gray-900">{perf.model}</td>
                  <td className="px-4 py-2 text-center text-xs text-gray-600">{(perf.accuracy * 100).toFixed(1)}%</td>
                  <td className="px-4 py-2 text-center text-xs text-gray-600">{(perf.precision * 100).toFixed(1)}%</td>
                  <td className="px-4 py-2 text-center text-xs text-gray-600">{(perf.recall * 100).toFixed(1)}%</td>
                  <td className="px-4 py-2 text-center">
                    <span className={`inline-block px-2 py-1 text-xs font-medium rounded ${
                      perf.model === bestF1Model ? 'bg-green-100 text-green-800 border-2 border-green-300' : 'text-gray-700'
                    }`}>
                      {perf.f1_score.toFixed(3)}
                    </span>
                  </td>
                  <td className="px-4 py-2 text-center text-xs text-gray-600">{perf.true_positives || 0}</td>
                  <td className="px-4 py-2 text-center text-xs text-gray-600">{perf.false_positives || 0}</td>
                  <td className="px-4 py-2 text-center text-xs text-gray-600">{perf.true_negatives || 0}</td>
                  <td className="px-4 py-2 text-center text-xs text-gray-600">{perf.false_negatives || 0}</td>
                  <td className="px-4 py-2 text-center text-xs text-gray-600">{(perf as any).files_tested || (perf as any).sheets_tested}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  const renderCostTable = () => (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <h3 className="text-lg font-semibold text-gray-900 mb-4">💰 Cost Analysis</h3>
      <div className="mb-4">
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 text-sm text-gray-600">
          <div>
            <span className="font-medium text-gray-900">Total Models:</span> <span className="text-gray-700">{costAnalysis.length}</span>
          </div>
          <div>
            <span className="font-medium text-gray-900">Total Tokens:</span> <span className="text-gray-700">{costAnalysis.reduce((sum, cost) => sum + cost.total_tokens, 0).toLocaleString()}</span>
          </div>
          <div>
            <span className="font-medium text-gray-900">Total Cost:</span> <span className="text-gray-700">${costAnalysis.reduce((sum, cost) => sum + cost.total_cost, 0).toFixed(4)}</span>
          </div>
        </div>
      </div>
      <div className="border border-gray-200 rounded-lg overflow-hidden">
        <table className="w-full">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-900">Model</th>
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">Reports</th>
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">Prompt Tokens</th>
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">Completion Tokens</th>
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">Total Tokens</th>
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">Price/1M</th>
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">Total Cost</th>
              <th className="px-4 py-2 text-center text-xs font-medium text-gray-900">Cost/Report</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200">
            {costAnalysis.map((cost) => (
              <tr key={cost.model} className="hover:bg-gray-50">
                <td className="px-4 py-2 text-xs font-medium text-gray-900">{cost.model}</td>
                <td className="px-4 py-2 text-center text-xs text-gray-600">{cost.reports}</td>
                <td className="px-4 py-2 text-center text-xs text-gray-600">{cost.prompt_tokens.toLocaleString()}</td>
                <td className="px-4 py-2 text-center text-xs text-gray-600">{cost.completion_tokens.toLocaleString()}</td>
                <td className="px-4 py-2 text-center text-xs text-gray-600">{cost.total_tokens.toLocaleString()}</td>
                <td className="px-4 py-2 text-center text-xs text-gray-600">{cost.price_per_1m}</td>
                <td className="px-4 py-2 text-center text-xs text-gray-600">{cost.total_cost.toFixed(4)}</td>
                <td className="px-4 py-2 text-center text-xs text-gray-600">{cost.cost_per_report.toFixed(4)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      
      <div className="mt-4 p-4 bg-gray-50 rounded-lg">
        <h4 className="text-sm font-semibold text-gray-900 mb-2">📋 Pricing Reference</h4>
        <div className="grid grid-cols-3 lg:grid-cols-6 gap-2 text-xs text-gray-600">
          {costAnalysis.map((cost) => (
            <div key={cost.model}>
              <span className="font-medium">{cost.model}:</span> <span>{cost.price_per_1m}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );

  const exportToPDF = async () => {
    try {
      setLoading(true);
      
      // Create a new jsPDF instance
      const pdf = new jsPDF('p', 'mm', 'a4');
      const pageWidth = pdf.internal.pageSize.getWidth();
      const pageHeight = pdf.internal.pageSize.getHeight();
      let yPosition = 20;
      
      // Helper function to add new page if needed
      const checkPageBreak = (requiredHeight: number) => {
        if (yPosition + requiredHeight > pageHeight - 20) {
          pdf.addPage();
          yPosition = 20;
        }
      };
      
      // Title
      pdf.setFontSize(18);
      pdf.setFont('helvetica', 'bold');
      pdf.text('Evaluation of Large Language Models on Sensitive Data Detection', pageWidth / 2, yPosition, { align: 'center' });
      yPosition += 10;
      
      pdf.setFontSize(12);
      pdf.setFont('helvetica', 'normal');
      pdf.text('HDX Sensitive Data Detection Pipeline - Model Performance Report', pageWidth / 2, yPosition, { align: 'center' });
      yPosition += 15;
      
      // Helper function to render table
      const renderTable = (title: string, data: any[], testedType: string) => {
        checkPageBreak(40);
        
        // Table title
        pdf.setFontSize(14);
        pdf.setFont('helvetica', 'bold');
        pdf.text(title, 20, yPosition);
        yPosition += 8;
        
        if (data.length === 0) {
          pdf.setFontSize(10);
          pdf.setFont('helvetica', 'normal');
          pdf.text('No data available', 20, yPosition);
          yPosition += 10;
          return;
        }
        
        // Find best F1 model
        const bestF1Model = data.length > 0 ? data.reduce((best, current) => 
          current.f1_score > best.f1_score ? current : best
        ).model : null;
        
        // Table headers
        const headers = ['Model', 'Accuracy', 'Precision', 'Recall', 'F1', 'TP', 'FP', 'TN', 'FN', testedType];
        const columnWidths = [25, 15, 15, 15, 12, 8, 8, 8, 8, 20];
        let xPosition = 20;
        
        pdf.setFontSize(8);
        pdf.setFont('helvetica', 'bold');
        headers.forEach((header, index) => {
          pdf.text(header, xPosition, yPosition);
          xPosition += columnWidths[index];
        });
        yPosition += 5;
        
        // Table rows
        pdf.setFont('helvetica', 'normal');
        data.forEach((row) => {
          checkPageBreak(8);
          xPosition = 20;
          
          const rowData = [
            row.model,
            `${(row.accuracy * 100).toFixed(1)}%`,
            `${(row.precision * 100).toFixed(1)}%`,
            `${(row.recall * 100).toFixed(1)}%`,
            row.f1_score.toFixed(3),
            row.true_positives?.toString() || '0',
            row.false_positives?.toString() || '0',
            row.true_negatives?.toString() || '0',
            row.false_negatives?.toString() || '0',
            row.files_tested?.toString() || row.sheets_tested?.toString() || '0'
          ];
          
          // Highlight best F1 model
          if (row.model === bestF1Model) {
            pdf.setFillColor(240, 253, 244); // Light green background
            pdf.rect(xPosition - 2, yPosition - 4, columnWidths.reduce((a, b) => a + b, 0), 6, 'F');
          }
          
          rowData.forEach((cell, index) => {
            if (row.model === bestF1Model && index === 4) { // F1 column
              pdf.setFont('helvetica', 'bold');
            }
            pdf.text(cell, xPosition, yPosition);
            if (row.model === bestF1Model && index === 4) {
              pdf.setFont('helvetica', 'normal');
            }
            xPosition += columnWidths[index];
          });
          yPosition += 6;
        });
        
        yPosition += 10;
      };
      
      // Render all performance tables
      renderTable('Overall File-Level Performance', overallPerformance, 'Files');
      renderTable('File-Level Personal Sensitive Data Detection', personalSensitive, 'Files');
      renderTable('File-Level Non Personal Sensitive Data Detection', nonPersonalSensitive, 'Files');
      renderTable('Sheet-Level Personal Sensitive Data Detection', sheetPersonalSensitive, 'Sheets');
      renderTable('Sheet-Level Non Personal Sensitive Data Detection', sheetNonPersonalSensitive, 'Sheets');
      
      // Cost Analysis
      if (costAnalysis.length > 0) {
        checkPageBreak(40);
        
        pdf.setFontSize(14);
        pdf.setFont('helvetica', 'bold');
        pdf.text('Cost Analysis', 20, yPosition);
        yPosition += 8;
        
        const costHeaders = ['Model', 'Reports', 'Total Tokens', 'Total Cost', 'Cost/Report'];
        const costColumnWidths = [30, 15, 25, 20, 20];
        let xPosition = 20;
        
        pdf.setFontSize(8);
        pdf.setFont('helvetica', 'bold');
        costHeaders.forEach((header, index) => {
          pdf.text(header, xPosition, yPosition);
          xPosition += costColumnWidths[index];
        });
        yPosition += 5;
        
        pdf.setFont('helvetica', 'normal');
        costAnalysis.forEach((row) => {
          checkPageBreak(8);
          xPosition = 20;
          
          const costRowData = [
            row.model,
            row.reports.toString(),
            row.total_tokens.toLocaleString(),
            `$${row.total_cost.toFixed(4)}`,
            `$${row.cost_per_report.toFixed(4)}`
          ];
          
          costRowData.forEach((cell, index) => {
            pdf.text(cell, xPosition, yPosition);
            xPosition += costColumnWidths[index];
          });
          yPosition += 6;
        });
      }
      
      // Footer
      checkPageBreak(20);
      pdf.setFontSize(8);
      pdf.setFont('helvetica', 'italic');
      pdf.text(`Generated on ${new Date().toLocaleDateString()} at ${new Date().toLocaleTimeString()}`, pageWidth / 2, pageHeight - 10, { align: 'center' });
      
      // Save the PDF
      pdf.save(`analytics-report-${new Date().toISOString().split('T')[0]}.pdf`);
      
    } catch (error) {
      console.error('Error generating PDF:', error);
      alert('Failed to generate PDF. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="p-8">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900 mb-2">Evaluation of Large Language Models on Sensitive Data Detection</h1>
          <p className="text-gray-600">HDX Sensitive Data Detection Pipeline - Model Performance Report</p>
        </div>


        {/* Performance Tables */}
        <div className="mb-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-2xl font-bold text-gray-900">Model Performance Report</h2>
            <div className="flex items-center gap-3">
              <button
                onClick={exportToPDF}
                disabled={loading}
                className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors disabled:opacity-50"
              >
                <Download className="w-4 h-4" />
                Export PDF
              </button>
              <button
                onClick={fetchAnalytics}
                disabled={loading}
                className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50"
              >
                <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
                Refresh
              </button>
            </div>
          </div>
        </div>

        {loading ? (
          <div className="text-center py-12">
            <div className="w-8 h-8 border-2 border-blue-600 border-t-transparent rounded-full animate-spin mx-auto mb-4"></div>
            <p className="text-gray-600">Loading analytics...</p>
          </div>
        ) : (
          <div className="space-y-6">
            {renderPerformanceTable("📁 Overall File-Level Performance", overallPerformance, "Files Tested")}
            {renderPerformanceTable("👤 File-Level Personal Sensitive Data Detection", personalSensitive, "Files Tested")}
            {renderPerformanceTable("🔒 File-Level Non Personal Sensitive Data Detection", nonPersonalSensitive, "Files Tested")}
            {renderPerformanceTable("👤 Sheet-Level Personal Sensitive Data Detection", sheetPersonalSensitive, "Sheets Tested")}
            {renderPerformanceTable("🔒 Sheet-Level Non Personal Sensitive Data Detection", sheetNonPersonalSensitive, "Sheets Tested")}
            {renderCostTable()}
          </div>
        )}

        {/* Footer */}
        <div className="mt-8 text-center text-xs text-gray-500">
          <p>HDX Sensitive Data Detection Pipeline - Model Performance Report</p>
          <p>Generated automatically from test results</p>
        </div>
      </div>
    </div>
  );
}
