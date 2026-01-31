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
        relative px-6 py-3 font-semibold text-sm
        transition-all duration-300 ease-in-out
        rounded-xl
        ${
          active
            ? "text-white bg-gradient-to-r from-blue-600 to-indigo-600 shadow-lg shadow-blue-500/50 scale-105"
            : "text-gray-600 bg-white hover:bg-gray-50 hover:text-gray-900 hover:shadow-md border border-gray-200"
        }
        transform hover:scale-105
        focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2
      `}
    >
      <span className="relative z-10">{label}</span>
      
      {/* Active indicator line */}
      {active && (
        <span className="absolute bottom-0 left-1/2 transform -translate-x-1/2 w-1/2 h-1 bg-white rounded-t-full opacity-50"></span>
      )}
    </button>
  );
}
  