import React from 'react';
import { useState } from 'react';
import { Network, Database, ChevronRight, Zap } from 'lucide-react';

export default function MergeStudio() {
    const [step, setStep] = useState(1);
    const [leftDs, setLeftDs] = useState('');
    const [rightDs, setRightDs] = useState('');

    // Mock data for scaffolding UI
    const suggestions = [
        { left: 'user_id', right: 'customer_id', score: 0.95, overlap: '89%', join: 'inner', reason: 'PK-FK match detected. Strong value overlap.' },
        { left: 'email', right: 'email_address', score: 0.72, overlap: '41%', join: 'left', reason: 'Similar column names. Partial overlap.' }
    ];

    return (
        <div className="max-w-6xl mx-auto space-y-6">
            <div className="flex items-center gap-3 mb-8 pb-4 border-b border-[#30363d]">
                <Network size={28} className="text-[#a371f7]" />
                <h2 className="text-3xl font-bold text-white">Dataset Join Studio</h2>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                {/* Left Side: Selection */}
                <div className="space-y-6">
                    <div className="bg-[#161b22] border border-[#30363d] rounded-xl p-6">
                        <h3 className="text-white font-bold mb-4 flex items-center gap-2"><Database size={16} /> Select Base Dataset (Left)</h3>
                        <select
                            className="w-full bg-[#0d1117] border border-[#30363d] text-white rounded-md p-3 focus:ring-2 focus:ring-[#1f6feb] outline-none"
                            value={leftDs} onChange={e => setLeftDs(e.target.value)}
                        >
                            <option value="">Choose a dataset...</option>
                            <option value="1">Sales_Q3_2024.csv (14.2M)</option>
                            <option value="2">User_Profiles.parquet (3.1M)</option>
                        </select>
                    </div>

                    <div className="bg-[#161b22] border border-[#30363d] rounded-xl p-6">
                        <h3 className="text-white font-bold mb-4 flex items-center gap-2"><Database size={16} /> Select Target Dataset (Right)</h3>
                        <select
                            className="w-full bg-[#0d1117] border border-[#30363d] text-white rounded-md p-3 focus:ring-2 focus:ring-[#1f6feb] outline-none"
                            value={rightDs} onChange={e => setRightDs(e.target.value)}
                        >
                            <option value="">Choose a dataset...</option>
                            <option value="1">Sales_Q3_2024.csv (14.2M)</option>
                            <option value="2">User_Profiles.parquet (3.1M)</option>
                        </select>
                    </div>

                    <button
                        disabled={!leftDs || !rightDs}
                        onClick={() => setStep(2)}
                        className={`w-full py-4 rounded-xl font-bold flex items-center justify-center gap-2 transition-all ${leftDs && rightDs ? 'bg-gradient-to-r from-[#a371f7] to-[#1f6feb] text-white hover:opacity-90 shadow-lg' : 'bg-[#21262d] text-gray-500 cursor-not-allowed'
                            }`}
                    >
                        <Zap size={20} /> Auto-Detect Join Keys
                    </button>
                </div>

                {/* Right Side: Suggestions & Execution */}
                <div className={`transition-opacity duration-500 ${step === 2 ? 'opacity-100' : 'opacity-30 pointer-events-none'}`}>
                    <div className="bg-[#161b22] border border-[#a371f7] border-opacity-50 rounded-xl p-6 shadow-[0_0_15px_rgba(163,113,247,0.1)]">
                        <h3 className="text-white font-bold text-xl mb-6">AI Merge Suggestions</h3>

                        <div className="space-y-4">
                            {suggestions.map((s, i) => (
                                <div key={i} className="bg-[#0d1117] border border-[#30363d] hover:border-[#a371f7] cursor-pointer rounded-lg p-4 transition-colors group">
                                    <div className="flex justify-between items-start mb-2">
                                        <div className="flex items-center gap-3 font-mono text-sm text-gray-300">
                                            <span className="bg-[#21262d] px-2 py-1 rounded text-[#58a6ff]">{s.left}</span>
                                            <ChevronRight size={14} />
                                            <span className="bg-[#21262d] px-2 py-1 rounded text-[#58a6ff]">{s.right}</span>
                                        </div>
                                        <span className="bg-[#238636] text-[#3fb950] bg-opacity-20 px-2 py-0.5 rounded text-xs font-bold border border-[#238636]">
                                            {(s.score * 100).toFixed(0)}% Confidence
                                        </span>
                                    </div>
                                    <p className="text-sm text-gray-400 mt-3">{s.reason}</p>
                                    <div className="mt-4 flex gap-4 text-xs font-medium uppercase tracking-wider text-gray-500">
                                        <span>TYPE: <b className="text-[#a371f7]">{s.join}</b></span>
                                        <span>OVERLAP: <b className="text-gray-300">{s.overlap}</b></span>
                                    </div>
                                </div>
                            ))}
                        </div>

                        <div className="mt-8 pt-6 border-t border-[#30363d]">
                            <div className="flex justify-between items-center mb-6">
                                <div className="text-sm text-gray-400">Estimated Output: <b className="font-mono text-white text-lg ml-2">~15.3M Rows</b></div>
                                <div className="text-sm text-gray-400">Fan-out Warning: <b className="text-[#3fb950] ml-2">None</b></div>
                            </div>
                            <button className="w-full bg-[#238636] hover:bg-[#2ea043] text-white py-3 rounded-lg font-bold shadow-md transition-colors">
                                Execute Merge to Dask Cluster
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
