"use client";
import { useState, useEffect } from "react";
import { ChevronLeft, ChevronRight, BarChart3, FileText } from "lucide-react";
import ResultsTab from "./ResultsTab";
import AnalyticsTab from "./AnalyticsTab";
import { getApiUrl } from "../services/api";

export default function Dashboard() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [activeTab, setActiveTab] = useState('pipeline');
  const [models, setModels] = useState<string[]>([]);
  const [selectedModel, setSelectedModel] = useState<string>("");
  const [showModelDropdown, setShowModelDropdown] = useState(false);

  const menuItems = [
    { id: 'results', label: 'Results', icon: FileText },
    { id: 'analytics', label: 'Analytics', icon: BarChart3 },
  ];

  useEffect(() => {
    fetchModels();
  }, []);

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

  const handleResultsClick = (model?: string) => {
    if (model) {
      setSelectedModel(model);
      setActiveTab('results');
    } else {
      setActiveTab('results');
    }
    setShowModelDropdown(false);
  };

  return (
    <div className="flex h-screen bg-gray-50">
      {/* Sidebar */}
      <div className={`${sidebarCollapsed ? 'w-16' : 'w-64'} bg-white border-r border-gray-200 transition-all duration-300 ease-in-out flex flex-col shadow-sm`}>
        {/* Header */}
        <div className="p-4 border-b border-gray-200">
          <div className="flex items-center justify-between">
            {!sidebarCollapsed && (
              <h1 className="text-xl font-semibold text-gray-800">Dashboard</h1>
            )}
            <button
              onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
              className="p-2 rounded-lg hover:bg-gray-100 transition-colors"
            >
              {sidebarCollapsed ? (
                <ChevronRight className="w-5 h-5 text-gray-600" />
              ) : (
                <ChevronLeft className="w-5 h-5 text-gray-600" />
              )}
            </button>
          </div>
        </div>

        {/* Navigation */}
        <nav className="flex-1 p-4">
          <div className="space-y-2">
            {menuItems.map((item) => {
              const Icon = item.icon;
              if (item.id === 'results') {
                return (
                  <div key={item.id} className="relative">
                    <button
                      className={`w-full flex items-center ${sidebarCollapsed ? 'justify-center' : 'justify-start'} px-3 py-2.5 rounded-lg transition-colors ${
                        activeTab === 'results' ? 'bg-blue-50 text-blue-700 border border-blue-200' : 'text-gray-600 hover:bg-gray-100'
                      }`}
                      onMouseEnter={() => !sidebarCollapsed && setShowModelDropdown(true)}
                      onMouseLeave={() => setShowModelDropdown(false)}
                      onClick={() => handleResultsClick()}
                    >
                      <Icon className={`w-5 h-5 ${sidebarCollapsed ? '' : 'mr-3'}`} />
                      {!sidebarCollapsed && (
                        <span className="font-medium">{item.label}</span>
                      )}
                    </button>
                    
                    {/* Model Dropdown */}
                    {showModelDropdown && !sidebarCollapsed && (
                      <div 
                        className="absolute left-full top-0 ml-2 bg-white border border-gray-200 rounded-lg shadow-lg py-2 z-50 min-w-[200px]"
                        onMouseEnter={() => setShowModelDropdown(true)}
                        onMouseLeave={() => setShowModelDropdown(false)}
                      >
                        <div className="px-3 py-2 text-xs font-semibold text-gray-500 border-b border-gray-100">
                          Choose Model
                        </div>
                        {models.map((model) => (
                          <button
                            key={model}
                            onClick={() => handleResultsClick(model)}
                            className={`w-full text-left px-3 py-2 text-sm hover:bg-gray-50 transition-colors ${
                              selectedModel === model ? 'bg-blue-50 text-blue-700' : 'text-gray-700'
                            }`}
                          >
                            {model}
                          </button>
                        ))}
                      </div>
                    )}
                  </div>
                );
              }
              
              return (
                <button
                  key={item.id}
                  onClick={() => setActiveTab(item.id)}
                  className={`w-full flex items-center ${sidebarCollapsed ? 'justify-center' : 'justify-start'} px-3 py-2.5 rounded-lg transition-colors ${
                    activeTab === item.id
                      ? 'bg-blue-50 text-blue-700 border border-blue-200'
                      : 'text-gray-600 hover:bg-gray-100'
                  }`}
                >
                  <Icon className={`w-5 h-5 ${sidebarCollapsed ? '' : 'mr-3'}`} />
                  {!sidebarCollapsed && (
                    <span className="font-medium">{item.label}</span>
                  )}
                </button>
              );
            })}
          </div>
        </nav>
      </div>

      {/* Main Content */}
      <div className="flex-1 overflow-auto">
        {activeTab === 'results' && <ResultsTab selectedModel={selectedModel} />}
        {activeTab === 'analytics' && <AnalyticsTab />}
      </div>
    </div>
  );
}
