"use client";

import React, { useEffect, useRef, useState } from "react";
import mermaid from "mermaid";

mermaid.initialize({
  startOnLoad: false,
  theme: "default",
  securityLevel: "loose",
  flowchart: {
    useMaxWidth: true,
    htmlLabels: true,
    curve: "basis",
  },
});

let mermaidCounter = 0;

interface MermaidDiagramProps {
  chart: string;
}

export default function MermaidDiagram({ chart }: MermaidDiagramProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [svg, setSvg] = useState<string>("");
  const [error, setError] = useState<string>("");

  useEffect(() => {
    const renderChart = async () => {
      if (!chart.trim()) return;

      try {
        const id = `mermaid-${Date.now()}-${mermaidCounter++}`;
        const { svg: renderedSvg } = await mermaid.render(id, chart.trim());
        setSvg(renderedSvg);
        setError("");
      } catch (err: any) {
        console.error("Mermaid render error:", err);
        setError(err?.message || "Failed to render diagram");
      }
    };

    renderChart();
  }, [chart]);

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-md p-3 text-sm text-red-700">
        <p className="font-medium">Diagram rendering error</p>
        <pre className="mt-1 text-xs whitespace-pre-wrap">{chart}</pre>
      </div>
    );
  }

  if (!svg) {
    return (
      <div className="bg-slate-100 rounded-md p-4 text-center text-sm text-slate-500 animate-pulse">
        Rendering diagram...
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      className="bg-white rounded-md p-4 overflow-x-auto border border-slate-200"
      dangerouslySetInnerHTML={{ __html: svg }}
    />
  );
}
