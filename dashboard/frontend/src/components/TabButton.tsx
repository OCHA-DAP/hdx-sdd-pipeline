interface TabButtonProps {
    label: string;
    active: boolean;
    onClick: () => void;
  }
  
  export default function TabButton({ label, active, onClick }: TabButtonProps) {
    return (
      <button
        onClick={onClick}
        className={`
          px-4 py-2 rounded-lg font-medium transition
          ${active ? "bg-blue-600 text-white shadow-md" : "bg-gray-200 text-gray-700 hover:bg-gray-300"}
        `}
      >
        {label}
      </button>
    );
  }
  