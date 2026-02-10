"use client";
import { useEffect, useState } from "react";
import { Dataset } from "../types/dataset";
import UploadTab from "./UploadTab";
import TabButton from "./TabButton";
import ResultsTab from "./ResultsTab";
import StatisticsTab from "./StatisticsTab";
import MultiModelResultsTab from "./MultiModelResultsTab";
import AllModelsPredictionsTab from "./AllModelsPredictionsTab";
import InsightsTab from "./InsightsTab";

export default function DatasetApp() {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState('insights');

  const handleUpload = (files: FileList | null) => {
    if (files) {
      const newDatasets = Array.from(files).map((file) => ({
        id: file.name,
        name: file.name,
        file: file,
      }));
      setDatasets([...datasets, ...newDatasets]);
    }
  };

  const fetchDatasets = async () => {
    setLoading(true);
    try {
      const res = await fetch("http://localhost:8000/api/list-datasets");
      if (!res.ok) throw new Error("Failed to fetch datasets");

      const data = await res.json();
      // Convert to Dataset type (frontend)
      const mapped: Dataset[] = data.datasets.map((d: any) => ({
        id: d.name, // use name as unique ID for now
        name: d.name,
        file: new File([], d.name), // dummy File object
      }));
      setDatasets(mapped);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  // Fetch datasets from FastAPI
  useEffect(() => {
    fetchDatasets();
  }, []);

  return (
    <div className="container mx-auto px-4 py-8 ">
      <h1 className="text-3xl font-bold mb-8">Dataset Processing Dashboard</h1>

      {/* Tab Navigation */}
      <div className="mb-8 bg-gray-50 p-2 rounded-2xl border border-gray-200 shadow-sm">
        <div className="flex flex-wrap gap-2">
          <TabButton label="📊 Insights" active={activeTab === 'insights'} onClick={() => setActiveTab('insights')} />
          <TabButton label="📤 Upload" active={activeTab === 'upload'} onClick={() => setActiveTab('upload')} />
          <TabButton label="📋 Results" active={activeTab === 'results'} onClick={() => setActiveTab('results')} />
          <TabButton label="🔀 Multi-Model" active={activeTab === 'multi-model-results'} onClick={() => setActiveTab('multi-model-results')} />
          <TabButton label="🎯 All Models" active={activeTab === 'all-models'} onClick={() => setActiveTab('all-models')} />
          <TabButton label="📈 Statistics" active={activeTab === 'statistics'} onClick={() => setActiveTab('statistics')} />
        </div>
      </div>

      {loading ? (
        <div className="flex justify-center items-center py-20">
          <div className="animate-spin rounded-full h-16 w-16 border-t-4 border-b-4 border-blue-600"></div>
        </div>
      ) : (
        <>
          {activeTab === 'insights' && <InsightsTab />}
          {activeTab === 'upload' && <UploadTab datasets={datasets} onRefresh={fetchDatasets} />}
          {activeTab === 'results' && <ResultsTab datasets={datasets} />}
          {activeTab === 'multi-model-results' && <MultiModelResultsTab datasets={datasets} />}
          {activeTab === 'all-models' && <AllModelsPredictionsTab datasets={datasets} />}
          {activeTab === 'statistics' && <StatisticsTab />}
        </>
      )}
    </div>
  );
}   
