import React from 'react';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import Dashboard from './pages/Dashboard';
import Upload from './pages/Upload';
import DatasetDetails from './pages/DatasetDetails';
import MergeStudio from './pages/MergeStudio';
import { Database, UploadCloud, Link as LinkIcon } from 'lucide-react';

function App() {
    return (
        <Router future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
            <div className="min-h-screen bg-[#0d1117] text-[#c9d1d9] font-sans flex">
                {/* Sidebar Sidebar */}
                <aside className="w-64 bg-[#161b22] border-r border-[#30363d] flex flex-col p-4">
                    <div className="flex items-center gap-3 mb-8">
                        <div className="w-8 h-8 rounded-lg bg-blue-600 flex items-center justify-center font-bold text-white text-xl">D</div>
                        <h1 className="text-xl font-bold tracking-wider text-white">DataSage</h1>
                    </div>

                    <nav className="flex-1 flex flex-col gap-2">
                        <Link to="/" className="flex items-center gap-3 px-3 py-2 rounded-md hover:bg-[#21262d] transition-colors">
                            <Database size={18} /> Dashboard
                        </Link>
                        <Link to="/upload" className="flex items-center gap-3 px-3 py-2 rounded-md hover:bg-[#21262d] transition-colors">
                            <UploadCloud size={18} /> Upload Data
                        </Link>
                        <Link to="/merge" className="flex items-center gap-3 px-3 py-2 rounded-md hover:bg-[#21262d] transition-colors">
                            <LinkIcon size={18} /> Merge Studio
                        </Link>
                    </nav>
                </aside>

                {/* Main Content */}
                <main className="flex-1 p-8 overflow-y-auto">
                    <Routes>
                        <Route path="/" element={<Dashboard />} />
                        <Route path="/upload" element={<Upload />} />
                        <Route path="/dataset/:id" element={<DatasetDetails />} />
                        <Route path="/merge" element={<MergeStudio />} />
                    </Routes>
                </main>
            </div>
        </Router>
    );
}

export default App;
