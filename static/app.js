let nodeMap = new Map();
function atualizarNodeMap() {
  nodeMap.clear();
  NODES.forEach(n => nodeMap.set(n.id, n));
}
atualizarNodeMap();

let activeNodeId = null;
let selectedLayer = 'all';
let isPipelineRunning = false;

// ─── Camada Cores & Metadados ────────────────────────────────────
const LAYER_CONFIG = {
  raw: {
    name: 'Raw',
    color: '#f59e0b',
    bg: 'rgba(245, 158, 11, 0.1)',
    border: '#d97706',
    rank: 0
  },
  staging: {
    name: 'Staging',
    color: '#0284c7',
    bg: 'rgba(2, 132, 199, 0.1)',
    border: '#0369a1',
    rank: 1
  },
  intermediaria: {
    name: 'Intermediária',
    color: '#6366f1',
    bg: 'rgba(99, 102, 241, 0.1)',
    border: '#4f46e5',
    rank: 2
  },
  mart: {
    name: 'Data Mart',
    color: '#10b981',
    bg: 'rgba(16, 185, 129, 0.1)',
    border: '#059669',
    rank: 3
  }
};

// ─── Configuração do Layout Determinístico ───────────────────────
const NODE_WIDTH = 220;
const NODE_HEIGHT = 74;

function calcularLayout(width, height) {
  const cols = [[], [], [], []];

  NODES.forEach(n => {
    const rank = LAYER_CONFIG[n.camada]?.rank ?? 0;
    cols[rank].push(n);
  });

  const colSpacing = (width - 120) / 4;
  const startX = 80;

  cols.forEach((colNodes, colIdx) => {
    const totalNodes = colNodes.length;
    const totalHeight = totalNodes * NODE_HEIGHT + (totalNodes - 1) * 36;
    const startY = Math.max(60, (height - totalHeight) / 2);

    colNodes.forEach((node, rowIdx) => {
      node.x = startX + colIdx * colSpacing;
      node.y = startY + rowIdx * (NODE_HEIGHT + 36);
    });
  });
}

// ─── Renderização D3 ─────────────────────────────────────────────
const svg = d3.select("#graph-svg");
let zoomBehavior, gRoot, gEdges, gNodes;

function renderDAG() {
  const container = document.querySelector('.canvas-container');
  const width = container.clientWidth;
  const height = container.clientHeight;

  svg.selectAll("*").remove();
  calcularLayout(width, height);

  const defs = svg.append("defs");

  defs.append("marker")
    .attr("id", "arrow-default")
    .attr("viewBox", "0 -5 10 10")
    .attr("refX", 8)
    .attr("refY", 0)
    .attr("markerWidth", 6)
    .attr("markerHeight", 6)
    .attr("orient", "auto")
    .append("path")
    .attr("d", "M0,-5L10,0L0,5")
    .attr("fill", "#334155");

  defs.append("marker")
    .attr("id", "arrow-active")
    .attr("viewBox", "0 -5 10 10")
    .attr("refX", 8)
    .attr("refY", 0)
    .attr("markerWidth", 6)
    .attr("markerHeight", 6)
    .attr("orient", "auto")
    .append("path")
    .attr("d", "M0,-5L10,0L0,5")
    .attr("fill", "#38bdf8");

  // Grid sutil
  const pattern = defs.append("pattern")
    .attr("id", "grid-pattern")
    .attr("width", 32)
    .attr("height", 32)
    .attr("patternUnits", "userSpaceOnUse");

  pattern.append("circle")
    .attr("cx", 2)
    .attr("cy", 2)
    .attr("r", 1)
    .attr("fill", "#1e293b");

  svg.append("rect")
    .attr("width", "100%")
    .attr("height", "100%")
    .attr("fill", "url(#grid-pattern)");

  // Zoom container
  gRoot = svg.append("g").attr("id", "zoom-root");
  gEdges = gRoot.append("g").attr("class", "edges-layer");
  gNodes = gRoot.append("g").attr("class", "nodes-layer");

  zoomBehavior = d3.zoom()
    .scaleExtent([0.3, 2.5])
    .on("zoom", (event) => gRoot.attr("transform", event.transform));

  svg.call(zoomBehavior);

  // IMPORTANTE: Desativa o zoom padrão de duplo clique do D3 para evitar o zoom involuntário na tela
  // Desativa completamente qualquer zoom por duplo clique no SVG e no canvas
  svg.on("dblclick.zoom", null);
  svg.on("dblclick", (event) => {
    event.preventDefault();
    event.stopPropagation();
  });

  // Render Arestas Bezier Cúbicas
  EDGES.forEach(edge => {
    const sourceNode = nodeMap.get(edge.origem);
    const targetNode = nodeMap.get(edge.destino);
    if (!sourceNode || !targetNode) return;

    const x1 = sourceNode.x + NODE_WIDTH;
    const y1 = sourceNode.y + NODE_HEIGHT / 2;
    const x2 = targetNode.x;
    const y2 = targetNode.y + NODE_HEIGHT / 2;
    const dx = (x2 - x1) * 0.55;

    const pathData = `M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}`;

    gEdges.append("path")
      .attr("d", pathData)
      .attr("id", `edge-${edge.origem}-${edge.destino}`)
      .attr("class", `edge-path from-${edge.origem} to-${edge.destino}`)
      .attr("fill", "none")
      .attr("stroke", "#334155")
      .attr("stroke-width", 1.75)
      .attr("marker-end", "url(#arrow-default)")
      .style("transition", "stroke 0.2s, stroke-width 0.2s");
  });

  // Render Nós (clique simples ou duplo apenas abre detalhes, NUNCA dá zoom)
  const nodeSelection = gNodes.selectAll(".node-group")
    .data(NODES)
    .enter()
    .append("g")
    .attr("class", d => `node-group node-${d.id} layer-${d.camada}`)
    .attr("transform", d => `translate(${d.x}, ${d.y})`)
    .style("cursor", "pointer")
    .on("click", (event, d) => {
      event.stopPropagation();
      selecionarNode(d.id);
    })
    .on("dblclick", (event, d) => {
      event.preventDefault();
      event.stopPropagation();
      selecionarNode(d.id);
    });

  // Retângulo Principal do Nó
  nodeSelection.append("rect")
    .attr("width", NODE_WIDTH)
    .attr("height", NODE_HEIGHT)
    .attr("rx", 8)
    .attr("fill", "#0f172a")
    .attr("stroke", d => LAYER_CONFIG[d.camada]?.border || "#334155")
    .attr("stroke-width", 1.25)
    .attr("class", "node-rect")
    .style("transition", "all 0.15s ease");

  // Barra superior indicadora de camada
  nodeSelection.append("rect")
    .attr("width", NODE_WIDTH)
    .attr("height", 3)
    .attr("rx", 1.5)
    .attr("fill", d => LAYER_CONFIG[d.camada]?.color || "#38bdf8");

  // Badge de Camada
  nodeSelection.append("text")
    .attr("x", 12)
    .attr("y", 20)
    .attr("fill", d => LAYER_CONFIG[d.camada]?.color || "#38bdf8")
    .attr("font-size", "9px")
    .attr("font-weight", "700")
    .attr("letter-spacing", "0.06em")
    .text(d => d.camada.toUpperCase());

  // Status Indicator Dot (canto direito)
  nodeSelection.append("circle")
    .attr("cx", NODE_WIDTH - 16)
    .attr("cy", 16)
    .attr("r", 4)
    .attr("class", "node-status-dot")
    .attr("fill", d => d.status === "EXECUTANDO" ? "#38bdf8" : (d.status === "PENDENTE" ? "#475569" : "#10b981"));

  // Título do Modelo / Tabela
  nodeSelection.append("text")
    .attr("x", 12)
    .attr("y", 40)
    .attr("fill", "#f1f5f9")
    .attr("font-size", "12px")
    .attr("font-weight", "600")
    .attr("font-family", "var(--font-ui)")
    .text(d => {
      const name = d.label.replace(/^(Raw|Staging|Int|Mart):\s*/, '');
      return name.length > 24 ? name.substring(0, 22) + "..." : name;
    });

  // Metadados inferiores (Linhas + Duração)
  nodeSelection.append("text")
    .attr("x", 12)
    .attr("y", 58)
    .attr("fill", "#94a3b8")
    .attr("font-size", "10px")
    .attr("font-family", "var(--font-code)")
    .attr("class", "node-meta-text")
    .text(d => `${(d.linhas || 0).toLocaleString('pt-BR')} linhas · ${(d.duracao_s || 0).toFixed(3)}s`);

  if (activeNodeId) {
    destacarNode(activeNodeId);
  }
}

// ─── Seleção e Destaque (Sem qualquer alteração de zoom) ──────────
function selecionarNode(nodeId) {
  activeNodeId = nodeId;
  destacarNode(nodeId);
  preencherDrawer(nodeId);
  abrirDrawer();
}

function destacarNode(nodeId) {
  d3.selectAll(".node-rect")
    .attr("stroke", d => LAYER_CONFIG[d.camada]?.border || "#334155")
    .attr("stroke-width", 1.25)
    .attr("fill", "#0f172a");

  d3.selectAll(".edge-path")
    .attr("stroke", "#334155")
    .attr("stroke-width", 1.75)
    .attr("marker-end", "url(#arrow-default)");

  if (!nodeId) return;

  d3.select(`.node-${nodeId} .node-rect`)
    .attr("stroke", "#38bdf8")
    .attr("stroke-width", 2)
    .attr("fill", "#1e293b");

  d3.selectAll(`.edge-path.to-${nodeId}`)
    .attr("stroke", "#38bdf8")
    .attr("stroke-width", 2.25)
    .attr("marker-end", "url(#arrow-active)");

  d3.selectAll(`.edge-path.from-${nodeId}`)
    .attr("stroke", "#6366f1")
    .attr("stroke-width", 2.25)
    .attr("marker-end", "url(#arrow-active)");
}

// ─── Drawer e Abas ───────────────────────────────────────────────
function abrirDrawer() {
  document.getElementById("inspectorDrawer").classList.add("open");
}

function fecharDrawer() {
  document.getElementById("inspectorDrawer").classList.remove("open");
  activeNodeId = null;
  destacarNode(null);
}

function trocarTab(tabId, btn) {
  document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
  document.querySelectorAll(".tab-pane").forEach(p => p.classList.remove("active"));
  btn.classList.add("active");
  document.getElementById(tabId).classList.add("active");
}

function preencherDrawer(nodeId) {
  const node = nodeMap.get(nodeId);
  if (!node) return;

  const layerCfg = LAYER_CONFIG[node.camada] || {};
  const badge = document.getElementById("dLayerBadge");
  badge.textContent = node.camada.toUpperCase();
  badge.style.color = layerCfg.color || "#38bdf8";
  badge.style.background = layerCfg.bg || "rgba(56, 189, 248, 0.1)";

  document.getElementById("dTitle").textContent = node.id;

  document.getElementById("dRows").textContent = (node.linhas || 0).toLocaleString('pt-BR');
  document.getElementById("dDuration").textContent = (node.duracao_s || 0).toFixed(4) + "s";

  const statusEl = document.getElementById("dStatus");
  statusEl.textContent = node.status || "SUCESSO";
  statusEl.style.color = node.status === "EXECUTANDO" ? "#38bdf8" : (node.status === "PENDENTE" ? "#64748b" : "#10b981");

  document.getElementById("dTimestamp").textContent = node.executado_em ? node.executado_em.replace('T', ' ').substring(0, 19) : "--";
  document.getElementById("dDescription").textContent = node.descricao || "";

  const upstreamNodes = EDGES.filter(e => e.destino === nodeId).map(e => e.origem);
  const upstreamEl = document.getElementById("dUpstream");
  if (upstreamNodes.length === 0) {
    upstreamEl.innerHTML = '<span style="font-size:11px;color:var(--text-subtle)">Nenhuma dependência (nó raiz)</span>';
  } else {
    upstreamEl.innerHTML = upstreamNodes.map(dep => 
      `<span class="dep-pill" onclick="selecionarNode('${dep}')">← ${dep}</span>`
    ).join('');
  }

  const downstreamNodes = EDGES.filter(e => e.origem === nodeId).map(e => e.destino);
  const downstreamEl = document.getElementById("dDownstream");
  if (downstreamNodes.length === 0) {
    downstreamEl.innerHTML = '<span style="font-size:11px;color:var(--text-subtle)">Nenhuma etapa dependente (nó folha)</span>';
  } else {
    downstreamEl.innerHTML = downstreamNodes.map(dest => 
      `<span class="dep-pill" onclick="selecionarNode('${dest}')">${dest} →</span>`
    ).join('');
  }

  const rawSql = (SQL_MAP[nodeId] || "-- Nenhuma query SQL registrada").trim();
  document.getElementById("dSqlCode").innerHTML = formatarSQL(rawSql);

  const sample = SAMPLES[nodeId];
  const previewContainer = document.getElementById("dPreviewTable");
  if (!sample || !sample.columns || sample.columns.length === 0) {
    previewContainer.innerHTML = '<div style="padding:16px;font-size:11px;color:var(--text-subtle)">Nenhum registro encontrado.</div>';
  } else {
    let html = '<table class="data-table"><thead><tr>';
    sample.columns.forEach(col => {
      html += `<th>${col}</th>`;
    });
    html += '</tr></thead><tbody>';
    sample.rows.forEach(row => {
      html += '<tr>';
      row.forEach(val => {
        html += `<td>${val !== null ? String(val) : '<span style="color:var(--text-subtle)">NULL</span>'}</td>`;
      });
      html += '</tr>';
    });
    html += '</tbody></table>';
    previewContainer.innerHTML = html;
  }
}

function formatarSQL(sql) {
  const keywords = ["SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "FULL", "OUTER", "ON", "GROUP", "BY", "ORDER", "DROP", "TABLE", "CREATE", "AS", "IF", "EXISTS", "CASE", "WHEN", "THEN", "ELSE", "END", "WITH", "OVER", "PARTITION", "AND", "OR", "NOT", "IN", "NULL", "COALESCE", "ROUND", "AVG", "SUM", "COUNT", "MAX", "MIN", "CAST", "INTEGER", "REAL", "TEXT", "RANK"];
  let escaped = sql
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

  keywords.forEach(kw => {
    const regex = new RegExp(`\\b(${kw})\\b`, 'gi');
    escaped = escaped.replace(regex, `<span class="kw">$1</span>`);
  });

  escaped = escaped.replace(/'([^']*)'/g, `<span class="str">'$1'</span>`);
  return escaped;
}

function copiarSQL() {
  if (!activeNodeId) return;
  const sql = (SQL_MAP[activeNodeId] || "").trim();
  navigator.clipboard.writeText(sql).then(() => {
    const btn = document.querySelector(".sql-copy-btn");
    btn.textContent = "Copiado!";
    setTimeout(() => btn.textContent = "Copiar SQL", 1500);
  });
}

// ─── Play: Executar Pipeline ─────────────────────────────────────
// Ordem topológica (Kahn) calculada a partir de NODES/EDGES
function calcularOrdemExecucao() {
  const grau = new Map(NODES.map(n => [n.id, 0]));
  EDGES.forEach(e => grau.set(e.destino, (grau.get(e.destino) || 0) + 1));
  const fila = NODES.filter(n => grau.get(n.id) === 0).map(n => n.id);
  const ordem = [];
  while (fila.length) {
    const id = fila.shift();
    ordem.push(id);
    EDGES.filter(e => e.origem === id).forEach(e => {
      grau.set(e.destino, grau.get(e.destino) - 1);
      if (grau.get(e.destino) === 0) fila.push(e.destino);
    });
  }
  return ordem;
}
let ORDEM_EXECUCAO = calcularOrdemExecucao();

async function executarPipelinePlay() {
  if (isPipelineRunning) return;
  isPipelineRunning = true;

  const playBtn = document.getElementById("btnPlayPipeline");
  const playText = document.getElementById("btnPlayText");
  const dot = document.getElementById("sbDot");
  const sbStatus = document.getElementById("sbPipelineStatus");

  playBtn.classList.add("running");
  playBtn.disabled = true;
  playBtn.innerHTML = `
    <svg class="spinner-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <circle cx="12" cy="12" r="10" stroke-opacity="0.25"></circle>
      <path d="M12 2a10 10 0 0 1 10 10" stroke-opacity="1"></path>
    </svg>
    <span>Executando...</span>
  `;

  dot.classList.add("running");
  sbStatus.textContent = "Status: Executando ordenação topológica...";

  // Abre console
  abrirTerminal();
  limparTerminal();
  adicionarLog("▶ Iniciando execução orquestrada do DAG (Kahn's Algorithm)...", "log-info");
  adicionarLog(`Ordem topológica resolvida: ${ORDEM_EXECUCAO.join(' → ')}`, "log-info");

  const falharExecucao = (msg) => {
    adicionarLog(`✗ ${msg}`, "log-error");
    playBtn.classList.remove("running");
    playBtn.disabled = false;
    playBtn.innerHTML = `
      <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
        <polygon points="5 3 19 12 5 21 5 3"></polygon>
      </svg>
      <span>Executar Pipeline</span>
    `;
    dot.classList.remove("running");
    dot.style.background = "#ef4444";
    sbStatus.textContent = "Status: Falha na execução";
    isPipelineRunning = false;
  };

  if (location.protocol === "file:") {
    return falharExecucao("Abra via servidor local para executar de verdade: python dag_pipeline.py --serve");
  }

  // Executa o pipeline real no backend Python
  adicionarLog("Executando pipeline no servidor (SQLite + Neo4j)...", "log-info");
  let dados;
  try {
    const resp = await fetch("/api/executar", { method: "POST" });
    dados = await resp.json();
  } catch (e) {
    return falharExecucao(`Servidor indisponível: ${e.message}. Inicie com: python dag_pipeline.py --serve`);
  }
  if (dados.log) dados.log.split(/\r?\n/).filter(l => l.trim()).forEach(l => adicionarLog(l, "log-info"));
  if (!dados.ok) {
    return falharExecucao(`Erro no pipeline: ${dados.erro}`);
  }

  NODES = dados.nodes;
  EDGES = dados.edges;
  RELATORIO = dados.relatorio;
  SAMPLES = dados.samples;
  atualizarNodeMap();
  ORDEM_EXECUCAO = calcularOrdemExecucao();

  // Reseta todos os nós para Pendente e anima com os resultados reais
  NODES.forEach(n => {
    n.status = "PENDENTE";
  });
  renderDAG();

  const startTime = performance.now();

  // Executa passo a passo animando
  for (let i = 0; i < ORDEM_EXECUCAO.length; i++) {
    const stepId = ORDEM_EXECUCAO[i];
    const node = nodeMap.get(stepId);
    if (!node) continue;

    // Atualiza nó para EXECUTANDO
    node.status = "EXECUTANDO";
    atualizarVisualDoNo(node);
    animarArestasEntrada(stepId, true);

    adicionarLog(`[${i + 1}/${ORDEM_EXECUCAO.length}] [${node.camada.toUpperCase()}] Executando query de ${node.label}...`, "log-running");

    // Delay para visibilidade da orquestração real
    await new Promise(r => setTimeout(r, 650));

    // Conclui nó com sucesso
    node.status = "SUCESSO";
    animarArestasEntrada(stepId, false);
    atualizarVisualDoNo(node);

    const durationMs = ((node.duracao_s || 0.003) * 1000).toFixed(1);
    adicionarLog(`  ↳ Concluído: ${node.linhas.toLocaleString('pt-BR')} linhas geradas em ${durationMs}ms`, "log-success");

    if (activeNodeId === stepId) {
      preencherDrawer(stepId);
    }
  }

  const totalElapsed = ((performance.now() - startTime) / 1000).toFixed(2);
  adicionarLog(`✓ Pipeline concluído com sucesso em ${totalElapsed}s! ${ORDEM_EXECUCAO.length}/${ORDEM_EXECUCAO.length} etapas validadas.`, "log-success");

  playBtn.classList.remove("running");
  playBtn.disabled = false;
  playBtn.innerHTML = `
    <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
      <polygon points="5 3 19 12 5 21 5 3"></polygon>
    </svg>
    <span>Executar Novamente</span>
  `;

  dot.classList.remove("running");
  dot.style.background = "#10b981";
  sbStatus.textContent = `Status: Concluído com sucesso (${totalElapsed}s)`;
  isPipelineRunning = false;
}

async function zerarPipeline(mostrarLog = true) {
  if (isPipelineRunning) return;

  // Reseta status de todos os nós para PENDENTE
  NODES.forEach(n => {
    n.status = "PENDENTE";
  });
  renderDAG();

  // Desativa animação de qualquer aresta
  d3.selectAll(".edge-path").classed("edge-running", false);

  // Reseta botão de Play
  const playBtn = document.getElementById("btnPlayPipeline");
  playBtn.classList.remove("running");
  playBtn.disabled = false;
  playBtn.innerHTML = `
    <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
      <polygon points="5 3 19 12 5 21 5 3"></polygon>
    </svg>
    <span>Executar Pipeline</span>
  `;

  // Reseta barra de status
  const dot = document.getElementById("sbDot");
  dot.classList.remove("running");
  dot.style.background = "#64748b";

  document.getElementById("sbPipelineStatus").textContent = "Status: Pipeline zerado · Pronto para execução";

  // Se o inspetor lateral estiver aberto, atualiza para refletir estado PENDENTE
  if (activeNodeId) {
    preencherDrawer(activeNodeId);
  }

  if (mostrarLog) {
    adicionarLog(`↺ Pipeline zerado com sucesso. Todas as ${NODES.length} etapas voltaram para PENDENTE.`, "log-info");
  }

  // Recria o banco de dados (tabelas brutas regeneradas; derivadas removidas)
  if (location.protocol === "file:") {
    if (mostrarLog) adicionarLog("⚠ Banco não recriado: abra via servidor local (python dag_pipeline.py --serve).", "log-error");
    return;
  }
  isPipelineRunning = true;
  playBtn.disabled = true;
  document.getElementById("btnResetPipeline").disabled = true;
  document.getElementById("sbPipelineStatus").textContent = "Status: Recriando banco de dados...";
  if (mostrarLog) { abrirTerminal(); adicionarLog("⟳ Recriando o banco de dados SQLite...", "log-running"); }
  try {
    const resp = await fetch("/api/zerar", { method: "POST" });
    const dados = await resp.json();
    if (!dados.ok) throw new Error(dados.erro || "falha desconhecida");
    NODES.forEach(n => { n.linhas = 0; n.duracao_s = 0; n.executado_em = ""; });
    NODES.filter(n => n.camada !== "raw").forEach(n => { SAMPLES[n.id] = { columns: [], rows: [] }; });
    renderDAG();
    if (activeNodeId) preencherDrawer(activeNodeId);
    adicionarLog("✓ Banco recriado: tabelas brutas regeneradas e tabelas derivadas removidas.", "log-success");
    document.getElementById("sbPipelineStatus").textContent = "Status: Banco recriado · Pronto para execução";
  } catch (e) {
    adicionarLog(`✗ Não foi possível recriar o banco: ${e.message}`, "log-error");
    document.getElementById("sbPipelineStatus").textContent = "Status: Falha ao recriar o banco";
  } finally {
    isPipelineRunning = false;
    playBtn.disabled = false;
    document.getElementById("btnResetPipeline").disabled = false;
  }
}

function atualizarVisualDoNo(node) {
  const g = d3.select(`.node-${node.id}`);
  if (g.empty()) return;

  const dotColor = node.status === "EXECUTANDO" ? "#38bdf8" : (node.status === "PENDENTE" ? "#475569" : "#10b981");
  g.select(".node-status-dot").attr("fill", dotColor);

  if (node.status === "EXECUTANDO") {
    g.select(".node-rect")
      .attr("stroke", "#38bdf8")
      .attr("stroke-width", 2.25);
  } else {
    g.select(".node-rect")
      .attr("stroke", LAYER_CONFIG[node.camada]?.border || "#334155")
      .attr("stroke-width", 1.25);
  }
}

function animarArestasEntrada(nodeId, ligar) {
  const edges = d3.selectAll(`.edge-path.to-${nodeId}`);
  if (ligar) {
    edges.classed("edge-running", true);
  } else {
    edges.classed("edge-running", false);
  }
}

// ─── Terminal / Logs ─────────────────────────────────────────────
function toggleTerminal() {
  const panel = document.getElementById("terminalPanel");
  panel.classList.toggle("open");
}

function abrirTerminal() {
  document.getElementById("terminalPanel").classList.add("open");
}

function limparTerminal() {
  document.getElementById("terminalLogs").innerHTML = "";
}

function adicionarLog(msg, classe = "log-info") {
  const logs = document.getElementById("terminalLogs");
  const time = new Date().toTimeString().split(" ")[0];
  const div = document.createElement("div");
  div.className = "log-line";
  div.innerHTML = `<span class="log-time">[${time}]</span> <span class="${classe}">${msg}</span>`;
  logs.appendChild(div);
  logs.scrollTop = logs.scrollHeight;
}

// ─── Modal do Relatório ──────────────────────────────────────────
function abrirModalRelatorio() {
  const tbody = document.getElementById("modalRelatorioBody");
  tbody.innerHTML = RELATORIO.map(r => `
    <tr>
      <td><strong style="color:var(--text-main)">#${r.ranking_risco}</strong></td>
      <td><strong>${r.zona}</strong></td>
      <td style="font-family:var(--font-code);font-weight:700">${parseFloat(r.score_risco).toFixed(2)}</td>
      <td><span class="risk-badge risk-${r.classificacao_risco}">${r.classificacao_risco}</span></td>
      <td>${r.total_incidentes}</td>
      <td>${r.incidentes_criticos}</td>
      <td>${r.total_alertas_sensor}</td>
      <td>${r.pct_rondas_irregulares}%</td>
      <td style="font-size:11px;color:var(--text-muted);max-width:280px;white-space:normal">${r.recomendacao}</td>
    </tr>
  `).join('');
  document.getElementById("modalRelatorio").classList.add("open");
}

function fecharModalRelatorio() {
  document.getElementById("modalRelatorio").classList.remove("open");
}

// ─── Filtros e Busca ─────────────────────────────────────────────
function filtrarCamada(layer, btn) {
  selectedLayer = layer;
  document.querySelectorAll(".chip-btn").forEach(b => b.classList.remove("active"));
  btn.classList.add("active");

  d3.selectAll(".node-group").style("opacity", d => {
    if (layer === 'all' || d.camada === layer) return 1;
    return 0.2;
  });
}

document.getElementById("nodeSearch").addEventListener("input", (e) => {
  const query = e.target.value.toLowerCase().trim();
  d3.selectAll(".node-group").style("opacity", d => {
    if (!query) return 1;
    const matches = d.id.toLowerCase().includes(query) || d.label.toLowerCase().includes(query);
    return matches ? 1 : 0.15;
  });
});

// ─── Controles de Zoom ───────────────────────────────────────────
function zoomIn() { svg.transition().duration(250).call(zoomBehavior.scaleBy, 1.3); }
function zoomOut() { svg.transition().duration(250).call(zoomBehavior.scaleBy, 0.7); }
function resetZoom() { svg.transition().duration(300).call(zoomBehavior.transform, d3.zoomIdentity); }

// Previne qualquer ação/zoom de duplo clique nativo do navegador
document.addEventListener("dblclick", (event) => {
  event.preventDefault();
}, { passive: false });

window.addEventListener("resize", () => renderDAG());
renderDAG();

adicionarLog(`Orquestrador de Pipeline carregado. ${NODES.length} etapas validadas.`, "log-info");

setTimeout(() => {
  selecionarNode("mart_zona_risco");
}, 200);
