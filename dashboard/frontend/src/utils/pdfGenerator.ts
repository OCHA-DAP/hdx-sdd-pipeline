import jsPDF from 'jspdf';
import autoTable from 'jspdf-autotable';

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
  sheet_level_non_pii: Metrics;
}

interface StatisticsResponse {
  models: Record<string, ModelStatistics>;
  available_models: string[];
}

export const generateStatisticsPDF = (statistics: StatisticsResponse, selectedModel: string) => {
  const doc = new jsPDF();
  const modelStats = statistics.models[selectedModel];
  
  let yPosition = 20;
  const pageWidth = doc.internal.pageSize.getWidth();
  const margin = 20;
  const contentWidth = pageWidth - 2 * margin;

  // Helper function to add page if needed
  const checkPageBreak = (requiredSpace: number) => {
    if (yPosition + requiredSpace > doc.internal.pageSize.getHeight() - 20) {
      doc.addPage();
      yPosition = 20;
      return true;
    }
    return false;
  };

  // Helper function to format percentage
  const formatPercent = (value: number) => `${(value * 100).toFixed(1)}%`;

  // ========== TITLE PAGE ==========
  doc.setFillColor(0, 158, 219); // OCHA blue
  doc.rect(0, 0, pageWidth, 50, 'F');
  
  doc.setTextColor(255, 255, 255);
  doc.setFontSize(24);
  doc.setFont('helvetica', 'bold');
  doc.text('Sensitive Data Detection', pageWidth / 2, 25, { align: 'center' });
  
  doc.setFontSize(16);
  doc.setFont('helvetica', 'normal');
  doc.text('Performance Report', pageWidth / 2, 38, { align: 'center' });
  
  yPosition = 65;
  doc.setTextColor(0, 0, 0);
  
  // Model info
  doc.setFontSize(14);
  doc.setFont('helvetica', 'bold');
  doc.text(`Model: ${selectedModel}`, margin, yPosition);
  yPosition += 10;
  
  doc.setFontSize(10);
  doc.setFont('helvetica', 'normal');
  doc.text(`Generated: ${new Date().toLocaleString()}`, margin, yPosition);
  yPosition += 20;

  // ========== INTRODUCTION ==========
  checkPageBreak(60);
  doc.setFontSize(16);
  doc.setFont('helvetica', 'bold');
  doc.setTextColor(0, 158, 219);
  doc.text('What is This Report?', margin, yPosition);
  yPosition += 8;
  
  doc.setFontSize(10);
  doc.setFont('helvetica', 'normal');
  doc.setTextColor(0, 0, 0);
  
  const introText = [
    'This report evaluates how well our AI model identifies sensitive data in datasets.',
    'We test the model at three levels:',
    '',
    '1. FILE LEVEL: Does the entire file contain any sensitive information?',
    '   - Sensitive = file contains PII or other sensitive data',
    '   - Not Sensitive = file is safe to share publicly',
    '',
    '2. SHEET LEVEL (PII): Does each sheet contain Personally Identifiable Information?',
    '   - PII includes names, emails, phone numbers, addresses, etc.',
    '',
    '3. SHEET LEVEL (Non-PII Sensitive): Does each sheet contain other sensitive data?',
    '   - This includes confidential business data, financial info, etc.',
  ];
  
  introText.forEach(line => {
    checkPageBreak(6);
    doc.text(line, margin, yPosition, { maxWidth: contentWidth });
    yPosition += 5;
  });
  
  yPosition += 10;

  // ========== KEY METRICS EXPLAINED ==========
  checkPageBreak(80);
  doc.setFontSize(16);
  doc.setFont('helvetica', 'bold');
  doc.setTextColor(0, 158, 219);
  doc.text('Understanding the Metrics', margin, yPosition);
  yPosition += 8;
  
  doc.setFontSize(10);
  doc.setFont('helvetica', 'normal');
  doc.setTextColor(0, 0, 0);
  
  const metricsExplanation = [
    'ACCURACY: Percentage of correct predictions (both sensitive and not sensitive)',
    '  → Higher is better. 90% means 9 out of 10 predictions were correct.',
    '',
    'PRECISION: When the model says "sensitive", how often is it right?',
    '  → High precision = fewer false alarms',
    '',
    'RECALL: Of all truly sensitive data, how much did the model catch?',
    '  → High recall = fewer missed sensitive items',
    '',
    'F1 SCORE: Balance between precision and recall (0 to 1)',
    '  → Higher is better. Combines precision and recall into one number.',
    '',
    'FALSE POSITIVES: Safe data incorrectly flagged as sensitive',
    '  → Can cause unnecessary restrictions',
    '',
    'FALSE NEGATIVES: Sensitive data incorrectly marked as safe',
    '  → More serious - could expose private information!',
  ];
  
  metricsExplanation.forEach(line => {
    checkPageBreak(6);
    doc.text(line, margin, yPosition, { maxWidth: contentWidth });
    yPosition += 5;
  });
  
  yPosition += 10;

  // ========== FILE-LEVEL RESULTS ==========
  doc.addPage();
  yPosition = 20;
  
  doc.setFontSize(18);
  doc.setFont('helvetica', 'bold');
  doc.setTextColor(0, 158, 219);
  doc.text('1. File-Level Performance', margin, yPosition);
  yPosition += 10;
  
  doc.setFontSize(10);
  doc.setFont('helvetica', 'normal');
  doc.setTextColor(0, 0, 0);
  doc.text('This measures whether the model correctly identifies if entire files are sensitive or not.', margin, yPosition, { maxWidth: contentWidth });
  yPosition += 15;

  // File-level metrics table
  autoTable(doc, {
    startY: yPosition,
    head: [['Metric', 'Score', 'Interpretation']],
    body: [
      ['Accuracy', formatPercent(modelStats.file_level.accuracy), 'Overall correctness'],
      ['Precision', formatPercent(modelStats.file_level.precision), 'Accuracy of "sensitive" predictions'],
      ['Recall', formatPercent(modelStats.file_level.recall), 'Percentage of sensitive files caught'],
      ['F1 Score', modelStats.file_level.f1.toFixed(3), 'Balance of precision and recall'],
      ['Total Files', String(modelStats.file_level.total_files || 0), 'Number of files tested'],
    ],
    theme: 'grid',
    headStyles: { fillColor: [0, 158, 219], textColor: 255 },
    margin: { left: margin, right: margin },
  });
  
  yPosition = (doc as any).lastAutoTable.finalY + 10;

  // Confusion Matrix
  checkPageBreak(60);
  doc.setFontSize(12);
  doc.setFont('helvetica', 'bold');
  doc.text('Confusion Matrix - File Level', margin, yPosition);
  yPosition += 8;
  
  const cm = modelStats.file_level.confusion_matrix;
  autoTable(doc, {
    startY: yPosition,
    head: [['', 'Predicted: Not Sensitive', 'Predicted: Sensitive']],
    body: [
      ['Actually Not Sensitive', `✓ ${cm.true_negative} (Correct)`, `✗ ${cm.false_positive} (False Alarm)`],
      ['Actually Sensitive', `✗ ${cm.false_negative} (MISSED!)`, `✓ ${cm.true_positive} (Correct)`],
    ],
    theme: 'grid',
    headStyles: { fillColor: [0, 158, 219], textColor: 255 },
    margin: { left: margin, right: margin },
    columnStyles: {
      0: { fontStyle: 'bold' },
    },
  });
  
  yPosition = (doc as any).lastAutoTable.finalY + 15;

  // File-level misclassifications
  if (modelStats.file_level.misclassifications.length > 0) {
    checkPageBreak(40);
    doc.setFontSize(12);
    doc.setFont('helvetica', 'bold');
    doc.text(`Misclassified Files (${modelStats.file_level.misclassifications.length})`, margin, yPosition);
    yPosition += 8;
    
    const misclassData = modelStats.file_level.misclassifications.slice(0, 20).map(m => [
      m.file,
      m.true_label,
      m.predicted_label,
      m.error_type,
    ]);
    
    autoTable(doc, {
      startY: yPosition,
      head: [['File', 'True Label', 'Predicted', 'Error Type']],
      body: misclassData,
      theme: 'striped',
      headStyles: { fillColor: [0, 158, 219], textColor: 255 },
      margin: { left: margin, right: margin },
      styles: { fontSize: 8 },
    });
    
    yPosition = (doc as any).lastAutoTable.finalY + 10;
  }

  // ========== SHEET-LEVEL PII RESULTS ==========
  doc.addPage();
  yPosition = 20;
  
  doc.setFontSize(18);
  doc.setFont('helvetica', 'bold');
  doc.setTextColor(0, 158, 219);
  doc.text('2. Sheet-Level PII Detection', margin, yPosition);
  yPosition += 10;
  
  doc.setFontSize(10);
  doc.setFont('helvetica', 'normal');
  doc.setTextColor(0, 0, 0);
  doc.text('This measures how well the model identifies sheets containing Personally Identifiable Information (PII).', margin, yPosition, { maxWidth: contentWidth });
  yPosition += 8;
  doc.text('PII includes: names, email addresses, phone numbers, ID numbers, addresses, etc.', margin, yPosition, { maxWidth: contentWidth });
  yPosition += 15;

  // PII metrics table
  autoTable(doc, {
    startY: yPosition,
    head: [['Metric', 'Score', 'Interpretation']],
    body: [
      ['Accuracy', formatPercent(modelStats.sheet_level_pii.accuracy), 'Overall correctness'],
      ['Precision', formatPercent(modelStats.sheet_level_pii.precision), 'Accuracy of PII predictions'],
      ['Recall', formatPercent(modelStats.sheet_level_pii.recall), 'Percentage of PII sheets caught'],
      ['F1 Score', modelStats.sheet_level_pii.f1.toFixed(3), 'Balance of precision and recall'],
      ['Total Sheets', String(modelStats.sheet_level_pii.total_sheets || 0), 'Number of sheets tested'],
    ],
    theme: 'grid',
    headStyles: { fillColor: [0, 158, 219], textColor: 255 },
    margin: { left: margin, right: margin },
  });
  
  yPosition = (doc as any).lastAutoTable.finalY + 10;

  // PII Confusion Matrix
  checkPageBreak(60);
  doc.setFontSize(12);
  doc.setFont('helvetica', 'bold');
  doc.text('Confusion Matrix - PII Detection', margin, yPosition);
  yPosition += 8;
  
  const cmPii = modelStats.sheet_level_pii.confusion_matrix;
  autoTable(doc, {
    startY: yPosition,
    head: [['', 'Predicted: No PII', 'Predicted: Has PII']],
    body: [
      ['Actually No PII', `✓ ${cmPii.true_negative} (Correct)`, `✗ ${cmPii.false_positive} (False Alarm)`],
      ['Actually Has PII', `✗ ${cmPii.false_negative} (MISSED!)`, `✓ ${cmPii.true_positive} (Correct)`],
    ],
    theme: 'grid',
    headStyles: { fillColor: [0, 158, 219], textColor: 255 },
    margin: { left: margin, right: margin },
    columnStyles: {
      0: { fontStyle: 'bold' },
    },
  });
  
  yPosition = (doc as any).lastAutoTable.finalY + 15;

  // PII misclassifications (limited to first 15)
  if (modelStats.sheet_level_pii.misclassifications.length > 0) {
    checkPageBreak(40);
    doc.setFontSize(12);
    doc.setFont('helvetica', 'bold');
    doc.text(`PII Misclassifications (showing ${Math.min(15, modelStats.sheet_level_pii.misclassifications.length)} of ${modelStats.sheet_level_pii.misclassifications.length})`, margin, yPosition);
    yPosition += 8;
    
    const piiMisclass = modelStats.sheet_level_pii.misclassifications.slice(0, 15).map(m => [
      m.file,
      m.sheet_name || 'N/A',
      m.error_type,
    ]);
    
    autoTable(doc, {
      startY: yPosition,
      head: [['File', 'Sheet', 'Error Type']],
      body: piiMisclass,
      theme: 'striped',
      headStyles: { fillColor: [0, 158, 219], textColor: 255 },
      margin: { left: margin, right: margin },
      styles: { fontSize: 8 },
    });
    
    yPosition = (doc as any).lastAutoTable.finalY + 10;
  }

  // ========== SHEET-LEVEL NON-PII RESULTS ==========
  doc.addPage();
  yPosition = 20;
  
  doc.setFontSize(18);
  doc.setFont('helvetica', 'bold');
  doc.setTextColor(0, 158, 219);
  doc.text('3. Sheet-Level Non-PII Sensitive Detection', margin, yPosition);
  yPosition += 10;
  
  doc.setFontSize(10);
  doc.setFont('helvetica', 'normal');
  doc.setTextColor(0, 0, 0);
  doc.text('This measures how well the model identifies sheets with sensitive data that is NOT personal information.', margin, yPosition, { maxWidth: contentWidth });
  yPosition += 8;
  doc.text('Non-PII sensitive data includes: confidential business info, financial data, security details, etc.', margin, yPosition, { maxWidth: contentWidth });
  yPosition += 15;

  // Non-PII metrics table
  autoTable(doc, {
    startY: yPosition,
    head: [['Metric', 'Score', 'Interpretation']],
    body: [
      ['Accuracy', formatPercent(modelStats.sheet_level_non_pii.accuracy), 'Overall correctness'],
      ['Precision', formatPercent(modelStats.sheet_level_non_pii.precision), 'Accuracy of sensitive predictions'],
      ['Recall', formatPercent(modelStats.sheet_level_non_pii.recall), 'Percentage of sensitive sheets caught'],
      ['F1 Score', modelStats.sheet_level_non_pii.f1.toFixed(3), 'Balance of precision and recall'],
      ['Total Sheets', String(modelStats.sheet_level_non_pii.total_sheets || 0), 'Number of sheets tested'],
    ],
    theme: 'grid',
    headStyles: { fillColor: [0, 158, 219], textColor: 255 },
    margin: { left: margin, right: margin },
  });
  
  yPosition = (doc as any).lastAutoTable.finalY + 10;

  // Non-PII Confusion Matrix
  checkPageBreak(60);
  doc.setFontSize(12);
  doc.setFont('helvetica', 'bold');
  doc.text('Confusion Matrix - Non-PII Sensitive Detection', margin, yPosition);
  yPosition += 8;
  
  const cmNonPii = modelStats.sheet_level_non_pii.confusion_matrix;
  autoTable(doc, {
    startY: yPosition,
    head: [['', 'Predicted: Not Sensitive', 'Predicted: Sensitive']],
    body: [
      ['Actually Not Sensitive', `✓ ${cmNonPii.true_negative} (Correct)`, `✗ ${cmNonPii.false_positive} (False Alarm)`],
      ['Actually Sensitive', `✗ ${cmNonPii.false_negative} (MISSED!)`, `✓ ${cmNonPii.true_positive} (Correct)`],
    ],
    theme: 'grid',
    headStyles: { fillColor: [0, 158, 219], textColor: 255 },
    margin: { left: margin, right: margin },
    columnStyles: {
      0: { fontStyle: 'bold' },
    },
  });
  
  yPosition = (doc as any).lastAutoTable.finalY + 15;

  // Non-PII misclassifications (limited to first 15)
  if (modelStats.sheet_level_non_pii.misclassifications.length > 0) {
    checkPageBreak(40);
    doc.setFontSize(12);
    doc.setFont('helvetica', 'bold');
    doc.text(`Non-PII Misclassifications (showing ${Math.min(15, modelStats.sheet_level_non_pii.misclassifications.length)} of ${modelStats.sheet_level_non_pii.misclassifications.length})`, margin, yPosition);
    yPosition += 8;
    
    const nonPiiMisclass = modelStats.sheet_level_non_pii.misclassifications.slice(0, 15).map(m => [
      m.file,
      m.sheet_name || 'N/A',
      m.error_type,
    ]);
    
    autoTable(doc, {
      startY: yPosition,
      head: [['File', 'Sheet', 'Error Type']],
      body: nonPiiMisclass,
      theme: 'striped',
      headStyles: { fillColor: [0, 158, 219], textColor: 255 },
      margin: { left: margin, right: margin },
      styles: { fontSize: 8 },
    });
    
    yPosition = (doc as any).lastAutoTable.finalY + 10;
  }

  // ========== MODEL COMPARISON ==========
  if (statistics.available_models.length > 1) {
    doc.addPage();
    yPosition = 20;
    
    doc.setFontSize(18);
    doc.setFont('helvetica', 'bold');
    doc.setTextColor(0, 158, 219);
    doc.text('Model Comparison', margin, yPosition);
    yPosition += 10;
    
    doc.setFontSize(10);
    doc.setFont('helvetica', 'normal');
    doc.setTextColor(0, 0, 0);
    doc.text('Comparing all available models across key metrics:', margin, yPosition, { maxWidth: contentWidth });
    yPosition += 15;

    const comparisonData = statistics.available_models
      .filter(model => !statistics.models[model].file_level.error)
      .map(model => {
        const stats = statistics.models[model];
        return [
          model,
          formatPercent(stats.file_level.accuracy),
          stats.file_level.f1.toFixed(3),
          formatPercent(stats.sheet_level_pii.accuracy),
          stats.sheet_level_pii.f1.toFixed(3),
          formatPercent(stats.sheet_level_non_pii.accuracy),
          stats.sheet_level_non_pii.f1.toFixed(3),
        ];
      });

    autoTable(doc, {
      startY: yPosition,
      head: [['Model', 'File Acc', 'File F1', 'PII Acc', 'PII F1', 'Non-PII Acc', 'Non-PII F1']],
      body: comparisonData,
      theme: 'grid',
      headStyles: { fillColor: [0, 158, 219], textColor: 255, fontSize: 8 },
      margin: { left: margin, right: margin },
      styles: { fontSize: 8 },
      columnStyles: {
        0: { fontStyle: 'bold', cellWidth: 35 },
      },
    });
  }

  // ========== SUMMARY PAGE ==========
  doc.addPage();
  yPosition = 20;
  
  doc.setFontSize(18);
  doc.setFont('helvetica', 'bold');
  doc.setTextColor(0, 158, 219);
  doc.text('Summary & Recommendations', margin, yPosition);
  yPosition += 15;
  
  doc.setFontSize(10);
  doc.setFont('helvetica', 'normal');
  doc.setTextColor(0, 0, 0);

  // Calculate summary stats
  const totalFP = cm.false_positive + cmPii.false_positive + cmNonPii.false_positive;
  const totalFN = cm.false_negative + cmPii.false_negative + cmNonPii.false_negative;
  
  const summaryText = [
    `Overall Performance for ${selectedModel}:`,
    '',
    `• File-Level Accuracy: ${formatPercent(modelStats.file_level.accuracy)}`,
    `• PII Detection Accuracy: ${formatPercent(modelStats.sheet_level_pii.accuracy)}`,
    `• Non-PII Detection Accuracy: ${formatPercent(modelStats.sheet_level_non_pii.accuracy)}`,
    '',
    `Total False Positives (False Alarms): ${totalFP}`,
    `  → These are safe items incorrectly flagged as sensitive`,
    '',
    `Total False Negatives (Missed Items): ${totalFN}`,
    `  → These are sensitive items that were missed - the most critical errors!`,
    '',
    'What This Means:',
    '',
    totalFN === 0 
      ? '✓ Excellent! The model caught all sensitive data.'
      : `⚠ The model missed ${totalFN} sensitive items. Review these carefully.`,
    '',
    totalFP === 0
      ? '✓ Perfect! No false alarms.'
      : `⚠ ${totalFP} safe items were flagged as sensitive. This may cause unnecessary restrictions.`,
    '',
    'Recommendations:',
    '',
    modelStats.file_level.recall < 0.9
      ? '• Consider using a more conservative threshold to catch more sensitive data'
      : '• Current recall is good - most sensitive data is being detected',
    '',
    modelStats.file_level.precision < 0.8
      ? '• High false positive rate - consider refining the model or adjusting thresholds'
      : '• Precision is acceptable - most "sensitive" predictions are correct',
  ];
  
  summaryText.forEach(line => {
    checkPageBreak(6);
    doc.text(line, margin, yPosition, { maxWidth: contentWidth });
    yPosition += 5;
  });

  // Save the PDF
  const filename = `Sensitivity_Report_${selectedModel}_${new Date().toISOString().split('T')[0]}.pdf`;
  doc.save(filename);
};
