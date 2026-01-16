import { Dataset } from "../types/dataset";

export default function DatasetCard({ dataset }: { dataset: Dataset }) {
  const extension = dataset.name.split(".").pop()?.toUpperCase();

  return (
    <div className="border rounded-lg p-4 text-white/80 shadow-sm hover:shadow-md transition">
      <div className="flex items-center justify-between mb-1">
        <h3 className="font-medium truncate">{dataset.name}</h3>
        <span className="text-xs px-2 py-1 rounded bg-gray-100 text-black">
          {extension}
        </span>
      </div>

      <p className="text-sm">Ready for analysis</p>
    </div>
  );
}
