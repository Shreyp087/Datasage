import { useEffect, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

type AgentReport = {
  id: string;
  agent_name: string;
  report_markdown: string | null;
  tokens_used?: number;
  model_used?: string;
};

type AgentReportCardProps = {
  report: AgentReport;
  defaultExpanded?: boolean;
};

const agentIcons: Record<string, string> = {
  'Quality Inspector': '🔍',
  'Feature Analyst': '🧬',
  'Statistical Analyst': '📐',
  'ML Advisor': '🤖',
  'Report Synthesizer': '📄',
};

export default function AgentReportCard({
  report,
  defaultExpanded = false,
}: AgentReportCardProps) {
  const [expanded, setExpanded] = useState(defaultExpanded);

  useEffect(() => {
    setExpanded(defaultExpanded);
  }, [defaultExpanded, report.id]);

  return (
    <div className="border border-gray-700 rounded-xl overflow-hidden mb-4">
      <div
        className="flex items-center justify-between p-4 bg-gray-800 cursor-pointer"
        onClick={() => setExpanded((prev) => !prev)}
      >
        <div className="flex items-center gap-3">
          <span className="text-2xl">{agentIcons[report.agent_name] || '🤖'}</span>
          <div>
            <div className="font-semibold text-white">{report.agent_name}</div>
            <div className="text-xs text-gray-400">
              {report.tokens_used?.toLocaleString() || 0} tokens
              {report.model_used ? ` · ${report.model_used}` : ''}
            </div>
          </div>
        </div>
        <span className="text-gray-400 text-sm">{expanded ? '▲' : '▼'}</span>
      </div>

      {expanded && (
        <div className="p-6 bg-gray-900">
          <ReactMarkdown
            remarkPlugins={[remarkGfm]}
            components={{
              table: (props) => (
                <div className="overflow-x-auto my-4">
                  <table className="w-full border-collapse text-sm" {...props} />
                </div>
              ),
              th: (props) => (
                <th
                  className="bg-gray-800 text-gray-300 text-left px-4 py-2 text-xs uppercase tracking-wide border border-gray-700"
                  {...props}
                />
              ),
              td: (props) => (
                <td className="px-4 py-2 border border-gray-700 text-gray-200 align-top" {...props} />
              ),
              tr: (props) => <tr className="hover:bg-gray-800 transition-colors" {...props} />,
              code: ({ className, children, ...props }) => {
                const text = String(children || '');
                const isBlock = text.includes('\n') || Boolean(className);
                return (
                  <code
                    className={
                      isBlock
                        ? 'block bg-gray-800 p-4 rounded-lg text-sm font-mono overflow-x-auto'
                        : 'bg-gray-800 text-red-400 px-1.5 py-0.5 rounded text-xs font-mono'
                    }
                    {...props}
                  >
                    {children}
                  </code>
                );
              },
              blockquote: (props) => (
                <blockquote
                  className="border-l-4 border-blue-500 bg-blue-900/20 px-4 py-3 my-3 rounded-r-lg text-blue-200 text-sm"
                  {...props}
                />
              ),
              h2: (props) => (
                <h2 className="text-xl font-bold text-white mt-6 mb-4 pb-2 border-b border-gray-700" {...props} />
              ),
              h3: (props) => <h3 className="text-base font-semibold text-gray-200 mt-5 mb-3" {...props} />,
              hr: () => <hr className="border-gray-700 my-6" />,
              ul: (props) => <ul className="list-none space-y-1.5 my-3 ml-2" {...props} />,
              li: (props) => <li className="text-gray-300 text-sm flex items-start gap-2" {...props} />,
              strong: (props) => <strong className="text-white font-semibold" {...props} />,
              p: (props) => <p className="text-gray-300 text-sm leading-relaxed my-2" {...props} />,
            }}
          >
            {report.report_markdown || 'No report content available.'}
          </ReactMarkdown>
        </div>
      )}
    </div>
  );
}
