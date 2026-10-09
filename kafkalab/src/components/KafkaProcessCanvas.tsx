"use client";

import { memo, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState, type HTMLAttributes, type Ref } from "react";
import { createPortal } from "react-dom";
import {
  Background,
  BaseEdge,
  Controls,
  Handle,
  MiniMap,
  Position,
  ReactFlow,
  getBezierPath,
  useNodesInitialized,
  useReactFlow,
  useStore,
  type Edge,
  type EdgeChange,
  type EdgeProps,
  type Node,
  type NodeChange,
  type NodeProps,
  type XYPosition,
} from "@xyflow/react";
import type { FlowLayout, KafkaLabActions, KafkaLabSnapshot } from "../hooks/useKafkaLab";
import { useFlowActivity } from "../hooks/useFlowActivity";
import { transitMotionEnabled, type ChartTransitToken } from "./flow-transit";
import { buildChartTransitTokens } from "./chart-activity";
import { chartLayout, getChartVariant, projectChart } from "./chart-views";
import type { ChartNode, ChartNodeData, ChartNodeTooltip, ChartVariantId } from "./chart-views";
import { FlowPacket } from "./FlowPacket";
import { LabIcon } from "./LabIcon";

type RenderEdgeData = { label: string; relationship: "record" | "learning" | "checkpoint"; active: boolean; motion: boolean; tokens: ChartTransitToken[] };
type RenderEdge = Edge<RenderEdgeData, "kl-flow">;

export type KafkaProcessCanvasProps = {
  snapshot: KafkaLabSnapshot;
  actions: KafkaLabActions;
  reducedMotion?: boolean;
  className?: string;
  resetRevision?: number;
  flowLayout?: FlowLayout;
  variant?: ChartVariantId;
  panelId?: string;
  focused?: boolean;
  focusPartitionId?: number;
};

function tooltipIdFor(nodeId: string) {
  return `chart-tooltip-${nodeId.replace(/[^a-zA-Z0-9_-]/g, "-")}`;
}

export function ChartNodeTooltipView({ id, tooltip, tooltipRef, ...attributes }: { id: string; tooltip: ChartNodeTooltip; tooltipRef?: Ref<HTMLDivElement> } & Omit<HTMLAttributes<HTMLDivElement>, "id" | "children">) {
  return (
    <div {...attributes} ref={tooltipRef} className="kl-chart-tooltip" id={id} role="tooltip" tabIndex={-1}>
      <div className="kl-chart-tooltip__heading"><span>{tooltip.eyebrow}</span><strong>{tooltip.title}</strong></div>
      <p>{tooltip.summary}</p>
      <dl>{tooltip.rows.map((row) => <div key={`${row.label}:${row.value}`}><dt>{row.label}</dt><dd>{row.value}</dd></div>)}</dl>
      {tooltip.note ? <small>{tooltip.note}</small> : null}
    </div>
  );
}

export function ChartNodeFrame({ id, data, selected }: { id: string; data: ChartNodeData; selected: boolean }) {
  const [tooltipVisible, setTooltipVisible] = useState(false);
  const [tooltipPosition, setTooltipPosition] = useState({ top: 12, left: 12 });
  const nodeRef = useRef<HTMLDivElement>(null);
  const detailsButtonRef = useRef<HTMLButtonElement>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);
  const focusDetails = useRef(false);
  const [flowWrapper, setFlowWrapper] = useState<HTMLElement | null>(null);
  const tooltipId = tooltipIdFor(id);
  const attachNode = useCallback((element: HTMLDivElement | null) => {
    nodeRef.current = element;
    setFlowWrapper(element?.closest<HTMLElement>(".react-flow__node") ?? null);
  }, []);
  const handleKeyDown = useCallback((event: { key: string; preventDefault: () => void }) => {
    if (event.key === "Escape") {
      if (tooltipRef.current?.contains(document.activeElement)) detailsButtonRef.current?.focus({ preventScroll: true });
      setTooltipVisible(false);
    } else if (event.key === "F2") {
      event.preventDefault();
      if (tooltipRef.current) tooltipRef.current.focus({ preventScroll: true });
      else { focusDetails.current = true; setTooltipVisible(true); }
    }
  }, []);
  const updateTooltipPosition = useCallback(() => {
    const rect = nodeRef.current?.getBoundingClientRect();
    const tooltipRect = tooltipRef.current?.getBoundingClientRect();
    if (!rect || !tooltipRect) return;
    const { width, height } = tooltipRect;
    const margin = 12;
    const fitsRight = window.innerWidth - rect.right >= width + margin * 2;
    const fitsLeft = rect.left >= width + margin * 2;
    const left = fitsRight ? rect.right + margin : fitsLeft ? rect.left - width - margin : rect.left + (rect.width - width) / 2;
    const top = fitsRight || fitsLeft ? rect.top : window.innerHeight - rect.bottom >= height + margin * 2 ? rect.bottom + margin : rect.top - height - margin;
    const next = {
      top: Math.max(margin, Math.min(top, window.innerHeight - height - margin)),
      left: Math.max(margin, Math.min(left, window.innerWidth - width - margin)),
    };
    setTooltipPosition((current) => current.top === next.top && current.left === next.left ? current : next);
  }, []);
  useLayoutEffect(() => {
    if (!tooltipVisible) return;
    if (focusDetails.current) { tooltipRef.current?.focus({ preventScroll: true }); focusDetails.current = false; }
    let frame: number;
    const trackGeometry = () => { updateTooltipPosition(); frame = requestAnimationFrame(trackGeometry); };
    trackGeometry();
    return () => cancelAnimationFrame(frame);
  }, [tooltipVisible, updateTooltipPosition]);
  useEffect(() => {
    if (!flowWrapper) return;
    flowWrapper.addEventListener("keydown", handleKeyDown);
    return () => flowWrapper.removeEventListener("keydown", handleKeyDown);
  }, [flowWrapper, handleKeyDown]);
  useEffect(() => {
    if (!tooltipVisible || !flowWrapper) return;
    const descriptions = new Set(flowWrapper.getAttribute("aria-describedby")?.split(/\s+/).filter(Boolean));
    descriptions.add(tooltipId);
    flowWrapper.setAttribute("aria-describedby", [...descriptions].join(" "));
    return () => {
      const remaining = flowWrapper.getAttribute("aria-describedby")?.split(/\s+/).filter((value) => value && value !== tooltipId) ?? [];
      if (remaining.length) flowWrapper.setAttribute("aria-describedby", remaining.join(" "));
      else flowWrapper.removeAttribute("aria-describedby");
    };
  }, [flowWrapper, tooltipId, tooltipVisible]);
  useEffect(() => {
    if (!tooltipVisible) return;
    const dismissKey = (event: KeyboardEvent) => { if (event.key === "Escape") handleKeyDown(event); };
    const dismissOutside = (event: PointerEvent) => {
      if (event.target instanceof window.Node && !nodeRef.current?.contains(event.target) && !tooltipRef.current?.contains(event.target)) setTooltipVisible(false);
    };
    const dismissFocusOutside = (event: FocusEvent) => {
      if (event.target instanceof window.Node && !(flowWrapper ?? nodeRef.current)?.contains(event.target) && !tooltipRef.current?.contains(event.target)) setTooltipVisible(false);
    };
    const dismissFocusExit = (event: FocusEvent) => {
      const owner = flowWrapper ?? nodeRef.current;
      const withinDetails = (target: EventTarget | null) => target instanceof window.Node && (owner?.contains(target) || tooltipRef.current?.contains(target));
      if (withinDetails(event.target) && !withinDetails(event.relatedTarget)) setTooltipVisible(false);
    };
    window.addEventListener("keydown", dismissKey);
    document.addEventListener("pointerdown", dismissOutside);
    document.addEventListener("focusin", dismissFocusOutside);
    document.addEventListener("focusout", dismissFocusExit);
    return () => { window.removeEventListener("keydown", dismissKey); document.removeEventListener("pointerdown", dismissOutside); document.removeEventListener("focusin", dismissFocusOutside); document.removeEventListener("focusout", dismissFocusExit); };
  }, [flowWrapper, handleKeyDown, tooltipVisible]);
  useEffect(() => {
    const hideDetails = () => { if (document.visibilityState === "hidden") setTooltipVisible(false); };
    document.addEventListener("visibilitychange", hideDetails);
    return () => document.removeEventListener("visibilitychange", hideDetails);
  }, []);
  return (
    <>
      <div ref={attachNode} className={`kl-flow-node kl-flow-node--${data.kind} kl-flow-node--${data.state} ${selected ? "kl-flow-node--selected" : ""}`}
        tabIndex={flowWrapper ? -1 : 0} aria-label={`${data.eyebrow} ${data.title}. ${data.status}. ${data.metric}. ${data.lesson ?? data.tooltip.summary}`}
        aria-describedby={tooltipVisible ? tooltipId : undefined} aria-keyshortcuts="F2" onKeyDown={flowWrapper ? undefined : handleKeyDown}>
        <div className="kl-flow-node__body">
          <span className="kl-flow-node__icon" aria-hidden="true"><LabIcon name={data.icon} width={21} height={21} /></span>
          <div className="kl-flow-node__copy"><span className="kl-flow-node__eyebrow">{data.eyebrow}</span><strong>{data.title}</strong><small>{data.status}</small><span className="kl-flow-node__metric">{data.metric}</span></div>
        </div>
        <p className="kl-flow-node__lesson">{data.lesson ?? data.tooltip.summary}</p>
        {data.problem ? <p className="kl-flow-node__problem">{data.problem}</p> : null}
        {data.inline?.length ? <div className="kl-flow-node__inline">{data.inline.map((row) => <span key={`${row.label}:${row.value}`} className={row.tone ? `is-${row.tone}` : undefined}>{row.label} <b>{row.value}</b></span>)}</div> : null}
        <button ref={detailsButtonRef} type="button" className="kl-flow-node__details nodrag nopan" aria-label={`Details for ${data.title}`} aria-expanded={tooltipVisible} aria-controls={tooltipId}
          onClick={(event) => { event.stopPropagation(); setTooltipVisible((visible) => !visible); }}>
          {tooltipVisible ? "Close details" : "Details"}<span aria-hidden="true">{tooltipVisible ? "−" : "+"}</span>
        </button>
      </div>
      {tooltipVisible ? createPortal(<ChartNodeTooltipView id={tooltipId} tooltip={data.tooltip} tooltipRef={tooltipRef} style={{ top: tooltipPosition.top, left: tooltipPosition.left }}
        onKeyDown={(event) => { event.stopPropagation(); handleKeyDown(event); }} onMouseDown={(event) => event.stopPropagation()} onClick={(event) => event.stopPropagation()} />, document.body) : null}
    </>
  );
}

const ChartFlowNode = memo(function ChartFlowNode({ id, data, selected }: NodeProps<ChartNode>) {
  return <><Handle type="target" position={Position.Left} id="target" isConnectable={false} aria-label={`${data.title} input`} /><Handle type="source" position={Position.Right} id="source" isConnectable={false} aria-label={`${data.title} output`} />
    <Handle type="target" position={Position.Top} id="input-top" isConnectable={false} className="kl-flow-handle--logical" />
    <Handle type="target" position={Position.Right} id="input-right" isConnectable={false} className="kl-flow-handle--logical" />
    <Handle type="target" position={Position.Bottom} id="input-bottom" isConnectable={false} className="kl-flow-handle--logical" />
    <Handle type="source" position={Position.Bottom} id="output-bottom" isConnectable={false} className="kl-flow-handle--logical" />
    <Handle type="source" position={Position.Left} id="output-left" isConnectable={false} className="kl-flow-handle--logical" />
    <ChartNodeFrame id={id} data={data} selected={selected} /></>;
});

const nodeTypes = { chart: ChartFlowNode };

function KafkaFlowEdge({ id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data, selected }: EdgeProps<RenderEdge>) {
  const [path, labelX, labelY] = getBezierPath({ sourceX, sourceY, sourcePosition, targetX, targetY, targetPosition, curvature: 0.18 });
  const tokens = data?.tokens ?? [];
  const staticX = (sourceX + targetX) / 2;
  const staticY = (sourceY + targetY) / 2;
  return <>
    <BaseEdge id={id} path={path} label={data?.label} labelX={labelX} labelY={labelY} labelStyle={{ fill: data?.active ? "var(--kl-flow-accent)" : "var(--kl-flow-muted)", fontSize: 9, fontWeight: 500 }} labelBgStyle={{ fill: "var(--kl-flow-card)" }} labelBgPadding={[4, 2]} labelBgBorderRadius={3} markerEnd={`url(#${data?.active ? "kl-flow-arrow-active" : "kl-flow-arrow"})`} className={`kl-flow-edge ${data?.active ? "kl-flow-edge--active" : ""} ${data?.motion ? "kl-flow-edge--animated" : ""} ${data?.relationship !== "record" ? "kl-flow-edge--logical" : ""} ${selected ? "kl-flow-edge--selected" : ""}`.trim()} />
    {tokens.map((token) => <FlowPacket key={`${token.generation ?? 0}:${token.id}`} token={token} path={path} motion={data?.motion ?? false} staticX={staticX} staticY={staticY} />)}
  </>;
}

const edgeTypes = { "kl-flow": KafkaFlowEdge };

function FitGraphToTopology({ topologyKey, hasSavedViewport, focusNodeId }: { topologyKey: string; hasSavedViewport: boolean; focusNodeId?: string }) {
  const { fitView, getNodes, getViewport, setViewport } = useReactFlow();
  const initialized = useNodesInitialized();
  const width = useStore((state) => state.width);
  const height = useStore((state) => state.height);
  useEffect(() => {
    if (!initialized || !width || !height || (hasSavedViewport && width >= 640)) return;
    const frame = requestAnimationFrame(() => {
      if (width < 640) {
        const nodes = getNodes();
        const focus = nodes.find((node) => node.id === focusNodeId) ?? nodes[0];
        if (hasSavedViewport && focus) {
          const viewport = getViewport();
          const left = focus.position.x * viewport.zoom + viewport.x;
          const top = focus.position.y * viewport.zoom + viewport.y;
          if (left >= 0 && left + (focus.measured?.width ?? 280) * viewport.zoom <= width
            && top >= 64 && top + (focus.measured?.height ?? 100) * viewport.zoom <= height) return;
        }
        const zoom = Math.max(0.7, Math.min(1.05, (width - 24) / 280));
        void setViewport({ x: 12 - (focus?.position.x ?? 0) * zoom, y: 78 - (focus?.position.y ?? 0) * zoom, zoom });
      } else {
        void fitView({ padding: 0.045, minZoom: 0.2, maxZoom: 1.1 });
      }
    });
    return () => cancelAnimationFrame(frame);
  }, [fitView, getNodes, getViewport, focusNodeId, hasSavedViewport, height, initialized, setViewport, topologyKey, width]);
  return null;
}

function usePageVisible() {
  const [visible, setVisible] = useState(true);
  useEffect(() => {
    const update = () => setVisible(document.visibilityState !== "hidden");
    update();
    document.addEventListener("visibilitychange", update);
    return () => document.removeEventListener("visibilitychange", update);
  }, []);
  return visible;
}

export function KafkaProcessCanvas({ snapshot, actions, reducedMotion = false, className, resetRevision, flowLayout, variant = "flow", panelId, focused = false, focusPartitionId }: KafkaProcessCanvasProps) {
  const effectiveResetRevision = resetRevision ?? snapshot.resetRevision ?? 0;
  const projection = useMemo(() => projectChart(snapshot, variant, { focused, focusPartitionId }), [snapshot, variant, focused, focusPartitionId]);
  const view = getChartVariant(variant);
  const savedLayout = useMemo(() => chartLayout(flowLayout, variant), [flowLayout, variant]);
  const pageVisible = usePageVisible();
  const pulseEvents = useFlowActivity(snapshot.events, effectiveResetRevision, snapshot.flowBaselineEventId, snapshot.running, pageVisible, reducedMotion, snapshot);
  const motionEnabled = transitMotionEnabled(reducedMotion, pageVisible);
  const transitTokens = useMemo(() => buildChartTransitTokens(pulseEvents, snapshot, projection), [projection, pulseEvents, snapshot]);
  const tokensByEdge = useMemo(() => {
    const grouped = new Map<string, ChartTransitToken[]>();
    for (const token of transitTokens) grouped.set(token.edgeId, [...(grouped.get(token.edgeId) ?? []), token]);
    return grouped;
  }, [transitTokens]);
  const activeNodeIds = useMemo(() => new Set(projection.edges.filter((edge) => tokensByEdge.has(edge.id)).flatMap((edge) => [edge.source, edge.target])), [projection.edges, tokensByEdge]);
  const layoutRevision = `${effectiveResetRevision}:${variant}:${focused ? focusPartitionId ?? "first" : "all"}`;
  const [nodeUi, setNodeUi] = useState<{ revision: string; nodes: Record<string, Partial<Pick<Node, "measured" | "position" | "selected" | "dragging">>> }>({ revision: layoutRevision, nodes: {} });
  const positions = useMemo(() => nodeUi.revision === layoutRevision ? nodeUi.nodes : {}, [layoutRevision, nodeUi.nodes, nodeUi.revision]);
  const canonicalPositions = useMemo<Record<string, XYPosition>>(() => Object.fromEntries(projection.nodes.map((chartNode) => [chartNode.id, chartNode.position])), [projection.nodes]);
  const nodes = useMemo<ChartNode[]>(() => projection.nodes.map((chartNode) => {
    const ui = positions[chartNode.id] ?? {};
    const active = activeNodeIds.has(chartNode.id);
    return { ...chartNode, position: ui.position ?? (focused ? undefined : savedLayout.positions[chartNode.id]) ?? chartNode.position, data: { ...chartNode.data, state: active ? "active" : chartNode.data.state }, measured: ui.measured, selected: ui.selected, dragging: ui.dragging };
  }), [activeNodeIds, positions, projection.nodes, savedLayout.positions, focused]);
  const onNodesChange = useCallback((changes: NodeChange<ChartNode>[]) => {
    setNodeUi((current) => {
      const next: Record<string, Partial<Pick<Node, "measured" | "position" | "selected" | "dragging">>> = {};
      for (const id of Object.keys(canonicalPositions)) if (current.revision === layoutRevision && current.nodes[id]) next[id] = { ...current.nodes[id] };
      for (const change of changes) {
        if (!("id" in change) || !canonicalPositions[change.id]) continue;
        const ui = next[change.id] ?? {};
        if (change.type === "dimensions" && change.dimensions) ui.measured = change.dimensions;
        if (change.type === "position") { if (change.position) ui.position = change.position; if (change.dragging !== undefined) ui.dragging = change.dragging; }
        if (change.type === "select") ui.selected = change.selected;
        next[change.id] = ui;
      }
      return { revision: layoutRevision, nodes: next };
    });
  }, [canonicalPositions, layoutRevision]);
  const [edgeUi, setEdgeUi] = useState<{ revision: string; selected: Set<string> }>({ revision: layoutRevision, selected: new Set() });
  const edges = useMemo<RenderEdge[]>(() => projection.edges.map((projectedEdge) => {
    const tokens = tokensByEdge.get(projectedEdge.id) ?? [];
    return {
      ...projectedEdge,
      animated: motionEnabled && tokens.length > 0,
      selected: edgeUi.revision === layoutRevision && edgeUi.selected.has(projectedEdge.id),
      data: { label: projectedEdge.data?.label ?? "", relationship: projectedEdge.data?.relationship ?? "learning", active: tokens.length > 0, motion: motionEnabled && tokens.length > 0, tokens },
      ariaLabel: `Select ${projectedEdge.data?.label ?? "chart"} link from ${projectedEdge.source} to ${projectedEdge.target}`,
    };
  }), [edgeUi, layoutRevision, motionEnabled, projection.edges, tokensByEdge]);
  const onEdgesChange = useCallback((changes: EdgeChange<RenderEdge>[]) => {
    setEdgeUi((current) => {
      const existingIds = new Set(edges.map((edge) => edge.id));
      const selected = current.revision === layoutRevision ? new Set([...current.selected].filter((id) => existingIds.has(id))) : new Set<string>();
      for (const change of changes) { if (change.type !== "select") continue; if (change.selected) selected.add(change.id); else selected.delete(change.id); }
      return { revision: layoutRevision, selected };
    });
  }, [edges, layoutRevision]);
  const topologyKey = `${layoutRevision}:${nodes.map((node) => node.id).join(",")}`;
  const currentPositions = useMemo(() => Object.fromEntries(nodes.map((node) => [node.id, node.position])), [nodes]);

  return (
    <section id={panelId} role="tabpanel" aria-labelledby={`chart-tab-${variant}`} className={`kl-flow-canvas ${reducedMotion ? "kl-flow-canvas--reduced" : ""} ${className ?? ""}`.trim()} aria-label={projection.ariaLabel}>
      <svg className="kl-flow-defs" aria-hidden="true"><defs><marker id="kl-flow-arrow" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0,0 L0,6 L7,3 z" fill="var(--kl-flow-edge)" /></marker><marker id="kl-flow-arrow-active" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0,0 L0,6 L7,3 z" fill="var(--kl-flow-accent)" /></marker></defs></svg>
      <div className="kl-flow-topology-key"><strong>{view.label}{focused && !["delivery", "activity"].includes(variant) ? ` · P${focusPartitionId ?? snapshot.partitions[0]?.id ?? "—"}` : ""}</strong><span>{variant === "activity" ? "Recent simulator events" : variant === "delivery" ? "All partitions in the selected group" : "Solid: record flow · Dashed: learning relationships"}</span></div>
      <ReactFlow key={effectiveResetRevision} nodes={nodes} edges={edges} nodeTypes={nodeTypes} edgeTypes={edgeTypes} onNodesChange={onNodesChange} onEdgesChange={onEdgesChange} onNodeDragStop={(_, draggedNode) => !focused && actions.setFlowPositions?.({ ...currentPositions, [draggedNode.id]: draggedNode.position }, variant)} onMoveEnd={(_, viewport) => !focused && actions.setFlowViewport?.(viewport, variant)} defaultViewport={focused ? undefined : savedLayout.viewport ?? undefined} fitViewOptions={{ padding: 0.06 }} minZoom={0.2} maxZoom={1.5} deleteKeyCode={null} nodesDraggable nodesConnectable={false} nodesFocusable edgesFocusable panOnDrag zoomOnScroll zoomOnPinch aria-label={projection.ariaLabel}>
        <FitGraphToTopology topologyKey={topologyKey} hasSavedViewport={!focused && !!savedLayout.viewport} focusNodeId={projection.focusNodeId} />
        <Background color="var(--kl-flow-grid)" gap={22} size={1} />
        <Controls showInteractive={false} position="bottom-left" />
        {!focused ? <MiniMap pannable zoomable bgColor="var(--kl-flow-card)" nodeColor={(node) => node.data?.state === "active" ? "var(--kl-flow-accent)" : node.data?.state === "attention" ? "var(--kl-flow-attention-ink)" : "var(--kl-flow-muted)"} nodeStrokeColor="var(--kl-flow-border)" maskColor="var(--kl-flow-mask)" /> : null}
      </ReactFlow>
      {!snapshot.partitions.length ? <div className="kl-flow-empty-hint">No partitions are available in this workflow.</div> : null}
    </section>
  );
}
