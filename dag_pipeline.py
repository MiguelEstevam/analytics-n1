"""
DAG de Vigilância Patrimonial
==============================
Orquestra um pipeline de transformações SQL usando:
  - SQLite  → armazenamento e execução das queries
  - Neo4j   → armazenamento do DAG (nós + arestas + metadados de execução)
  - HTML    → visualização interativa do grafo

Etapas do pipeline:
  1. stg_eventos_camera      - staging de eventos de câmeras
  2. stg_leituras_sensor     - staging de leituras de sensores
  3. stg_rondas              - staging de rondas
  4. int_incidentes_brutos   - integração dos incidentes brutos (depende 1+2)
  5. int_alertas_sensor      - filtra alertas críticos dos sensores (depende 2)
  6. int_rondas_irregulares  - identifica rondas incompletas (depende 3)
  7. mart_zona_risco          - scoring de risco por zona (depende 4+5+6)
  8. mart_relatorio_final    - relatório consolidado de segurança (depende 7)
"""

import sqlite3
import time
import json
import os
import re
import sys
from datetime import datetime
from collections import defaultdict, deque

try:
    from neo4j import GraphDatabase
except ImportError:  # Neo4j é opcional: sem o driver usamos o fallback local
    GraphDatabase = None

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ─── Configurações ───────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "dados", "vigilancia.db")

# Credenciais do Neo4j: variáveis de ambiente têm prioridade; depois o arquivo
# local (ignorado pelo git). Nenhum segredo fica no código-fonte.
CREDS_FILE = os.path.join(os.path.dirname(__file__), "Blank Sandbox Sandbox Credentials.txt")
NEO4J_URI  = os.environ.get("NEO4J_URI", "")
NEO4J_USER = os.environ.get("NEO4J_USERNAME", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASSWORD", "")

if not (NEO4J_URI and NEO4J_PASS) and os.path.exists(CREDS_FILE):
    try:
        with open(CREDS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if "=" not in line:
                    continue
                k, v = line.strip().split("=", 1)
                k, v = k.strip(), v.strip()
                if k == "NEO4J_URI" and not NEO4J_URI:
                    NEO4J_URI = v
                elif k == "NEO4J_USERNAME":
                    NEO4J_USER = v
                elif k == "NEO4J_PASSWORD" and not NEO4J_PASS:
                    NEO4J_PASS = v
    except OSError as e:
        print(f"  ⚠️  Não foi possível ler o arquivo de credenciais: {e}")

# Acrescenta a porta padrão do Bolt quando a URI não a informa
if NEO4J_URI and not re.search(r":\d{1,5}$", NEO4J_URI):
    NEO4J_URI += ":7687"

# ─── Definição do DAG ────────────────────────────────────────────────────────
DAG_NODES = [
    {
        "id":          "stg_eventos_camera",
        "label":       "Staging: Eventos de Câmera",
        "camada":      "staging",
        "descricao":   "Carrega e padroniza os eventos brutos capturados pelas câmeras de segurança. "
                       "Filtra registros com confiança < 60% e normaliza timestamps.",
        "dependencias": []
    },
    {
        "id":          "stg_leituras_sensor",
        "label":       "Staging: Leituras de Sensor",
        "camada":      "staging",
        "descricao":   "Carrega as leituras brutas de sensores (PIR, magnético, temperatura, etc.). "
                       "Padroniza unidades e elimina leituras duplicadas.",
        "dependencias": []
    },
    {
        "id":          "stg_rondas",
        "label":       "Staging: Rondas de Segurança",
        "camada":      "staging",
        "descricao":   "Carrega o registro de rondas realizadas pelos vigilantes, "
                       "calcula duração em minutos e padroniza status.",
        "dependencias": []
    },
    {
        "id":          "int_incidentes_brutos",
        "label":       "Int: Incidentes Consolidados",
        "camada":      "intermediaria",
        "descricao":   "Consolida incidentes cruzando eventos de câmera e leituras de sensor. "
                       "Classifica gravidade e identifica zona de ocorrência.",
        "dependencias": ["stg_eventos_camera", "stg_leituras_sensor"]
    },
    {
        "id":          "int_alertas_sensor",
        "label":       "Int: Alertas Críticos de Sensor",
        "camada":      "intermediaria",
        "descricao":   "Filtra leituras de sensor com valor acima do limiar crítico (> 80). "
                       "Agrupa por zona e tipo de sensor para identificar padrões.",
        "dependencias": ["stg_leituras_sensor"]
    },
    {
        "id":          "int_rondas_irregulares",
        "label":       "Int: Rondas Irregulares",
        "camada":      "intermediaria",
        "descricao":   "Identifica rondas incompletas ou não realizadas e calcula a taxa de "
                       "irregularidade por funcionário e por zona.",
        "dependencias": ["stg_rondas"]
    },
    {
        "id":          "mart_zona_risco",
        "label":       "Mart: Score de Risco por Zona",
        "camada":      "mart",
        "descricao":   "Calcula um score composto de risco para cada zona patrimonial, "
                       "combinando: incidentes críticos, alertas de sensor e rondas irregulares.",
        "dependencias": ["int_incidentes_brutos", "int_alertas_sensor", "int_rondas_irregulares"]
    },
    {
        "id":          "mart_relatorio_final",
        "label":       "Mart: Relatório Final de Segurança",
        "camada":      "mart",
        "descricao":   "Relatório executivo consolidado com ranking de zonas por risco, "
                       "top incidentes do período e recomendações operacionais.",
        "dependencias": ["mart_zona_risco"]
    },
]

# ─── Transformações SQL ───────────────────────────────────────────────────────
TRANSFORMACOES_SQL = {

    "stg_eventos_camera": """
        DROP TABLE IF EXISTS stg_eventos_camera;
        CREATE TABLE stg_eventos_camera AS
        SELECT
            id,
            camera_id,
            zona,
            tipo_evento,
            timestamp,
            duracao_segundos,
            ROUND(confianca, 2)  AS confianca,
            funcionario_notificado,
            CASE
                WHEN tipo_evento IN ('MOVIMENTO_SUSPEITO','ACESSO_NEGADO','ALARME_PERIMETRO','ALARME_INCENDIO')
                THEN 1 ELSE 0
            END AS eh_alerta
        FROM eventos_camera
        WHERE confianca >= 0.60;
    """,

    "stg_leituras_sensor": """
        DROP TABLE IF EXISTS stg_leituras_sensor;
        CREATE TABLE stg_leituras_sensor AS
        SELECT
            id,
            sensor_id,
            tipo_sensor,
            zona,
            ROUND(valor, 2) AS valor,
            alerta,
            timestamp,
            CASE
                WHEN valor > 90 THEN 'CRITICO'
                WHEN valor > 80 THEN 'ALTO'
                WHEN valor > 60 THEN 'MEDIO'
                ELSE 'NORMAL'
            END AS nivel_risco
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY sensor_id, timestamp ORDER BY id) AS rn
            FROM leituras_sensor
        )
        WHERE rn = 1;
    """,

    "stg_rondas": """
        DROP TABLE IF EXISTS stg_rondas;
        CREATE TABLE stg_rondas AS
        SELECT
            r.id,
            r.funcionario_id,
            f.nome         AS funcionario_nome,
            f.cargo        AS funcionario_cargo,
            r.zona,
            r.inicio,
            r.fim,
            r.status,
            r.observacoes,
            CAST(
                (julianday(r.fim) - julianday(r.inicio)) * 24 * 60
            AS INTEGER)  AS duracao_minutos,
            CASE
                WHEN r.status IN ('INCOMPLETA','NAO_REALIZADA') THEN 1
                ELSE 0
            END          AS eh_irregular
        FROM rondas r
        JOIN funcionarios f ON r.funcionario_id = f.id;
    """,

    "int_incidentes_brutos": """
        DROP TABLE IF EXISTS int_incidentes_brutos;
        CREATE TABLE int_incidentes_brutos AS
        SELECT
            i.id,
            i.tipo,
            i.zona,
            i.data_hora,
            i.gravidade,
            i.status,
            i.origem,
            i.camera_id,
            i.sensor_id,
            i.funcionario_responsavel,
            f.nome AS responsavel_nome,
            CASE
                WHEN i.gravidade >= 4 THEN 'CRITICO'
                WHEN i.gravidade = 3  THEN 'ALTO'
                WHEN i.gravidade = 2  THEN 'MEDIO'
                ELSE 'BAIXO'
            END AS nivel_gravidade,
            COALESCE((
                SELECT MAX(ec.confianca)
                FROM stg_eventos_camera ec
                WHERE ec.camera_id = i.camera_id
            ), 0)                               AS confianca_camera,
            COALESCE((
                SELECT ls.nivel_risco
                FROM stg_leituras_sensor ls
                WHERE ls.sensor_id = i.sensor_id
                ORDER BY ls.valor DESC, ls.id
                LIMIT 1
            ), 'N/A')                           AS risco_sensor
        FROM incidentes i
        LEFT JOIN funcionarios f ON i.funcionario_responsavel = f.id;
    """,

    "int_alertas_sensor": """
        DROP TABLE IF EXISTS int_alertas_sensor;
        CREATE TABLE int_alertas_sensor AS
        SELECT
            zona,
            tipo_sensor,
            COUNT(*)                        AS total_leituras,
            SUM(alerta)                     AS total_alertas,
            ROUND(AVG(valor), 2)            AS media_valor,
            ROUND(MAX(valor), 2)            AS max_valor,
            SUM(CASE WHEN nivel_risco = 'CRITICO' THEN 1 ELSE 0 END) AS alertas_criticos,
            SUM(CASE WHEN nivel_risco = 'ALTO'    THEN 1 ELSE 0 END) AS alertas_altos,
            ROUND(
                100.0 * SUM(alerta) / COUNT(*), 2
            )                               AS pct_alerta
        FROM stg_leituras_sensor
        GROUP BY zona, tipo_sensor;
    """,

    "int_rondas_irregulares": """
        DROP TABLE IF EXISTS int_rondas_irregulares;
        CREATE TABLE int_rondas_irregulares AS
        SELECT
            zona,
            COUNT(*)                               AS total_rondas,
            SUM(eh_irregular)                      AS rondas_irregulares,
            ROUND(
                100.0 * SUM(eh_irregular) / COUNT(*), 2
            )                                      AS pct_irregular,
            ROUND(AVG(duracao_minutos), 1)          AS media_duracao_min,
            COUNT(DISTINCT funcionario_id)         AS vigilantes_envolvidos
        FROM stg_rondas
        GROUP BY zona;
    """,

    "mart_zona_risco": """
        DROP TABLE IF EXISTS mart_zona_risco;
        CREATE TABLE mart_zona_risco AS
        WITH incidentes_por_zona AS (
            SELECT
                zona,
                COUNT(*)                                                       AS total_incidentes,
                SUM(CASE WHEN nivel_gravidade = 'CRITICO' THEN 1 ELSE 0 END)  AS incidentes_criticos,
                SUM(CASE WHEN nivel_gravidade = 'ALTO'    THEN 1 ELSE 0 END)  AS incidentes_altos,
                ROUND(AVG(gravidade), 2)                                       AS gravidade_media
            FROM int_incidentes_brutos
            GROUP BY zona
        ),
        alertas_por_zona AS (
            SELECT
                zona,
                SUM(total_leituras)    AS total_leituras,
                SUM(total_alertas)     AS total_alertas_sensor,
                SUM(alertas_criticos)  AS alertas_criticos_sensor,
                ROUND(AVG(pct_alerta), 2) AS pct_alerta_medio
            FROM int_alertas_sensor
            GROUP BY zona
        ),
        rondas_por_zona AS (
            SELECT
                zona,
                total_rondas,
                rondas_irregulares,
                pct_irregular
            FROM int_rondas_irregulares
        ),
        zonas AS (
            SELECT zona FROM incidentes_por_zona
            UNION SELECT zona FROM alertas_por_zona
            UNION SELECT zona FROM rondas_por_zona
        ),
        base AS (
            SELECT
                z.zona                                     AS zona,
                COALESCE(i.total_incidentes, 0)            AS total_incidentes,
                COALESCE(i.incidentes_criticos, 0)         AS incidentes_criticos,
                COALESCE(a.total_alertas_sensor, 0)        AS total_alertas_sensor,
                COALESCE(a.alertas_criticos_sensor, 0)     AS alertas_criticos_sensor,
                COALESCE(r.rondas_irregulares, 0)          AS rondas_irregulares,
                COALESCE(r.pct_irregular, 0)               AS pct_rondas_irregulares,
                -- Score 0-100: pesos 40% incidentes criticos, 30% leituras criticas, 30% rondas irregulares
                ROUND(
                    0.4 * 100.0 * COALESCE(i.incidentes_criticos, 0) / NULLIF(i.total_incidentes, 0) +
                    0.3 * 100.0 * COALESCE(a.alertas_criticos_sensor, 0) / NULLIF(a.total_leituras, 0) +
                    0.3 * COALESCE(r.pct_irregular, 0)
                , 2)                                       AS score_risco
            FROM zonas z
            LEFT JOIN incidentes_por_zona i ON i.zona = z.zona
            LEFT JOIN alertas_por_zona    a ON a.zona = z.zona
            LEFT JOIN rondas_por_zona     r ON r.zona = z.zona
        ),
        faixas AS (
            SELECT *, NTILE(4) OVER (ORDER BY COALESCE(score_risco, 0) DESC) AS quartil
            FROM base
        )
        SELECT
            zona,
            total_incidentes,
            incidentes_criticos,
            total_alertas_sensor,
            alertas_criticos_sensor,
            rondas_irregulares,
            pct_rondas_irregulares,
            COALESCE(score_risco, 0)                       AS score_risco,
            CASE quartil
                WHEN 1 THEN 'CRITICO'
                WHEN 2 THEN 'ALTO'
                WHEN 3 THEN 'MEDIO'
                ELSE 'BAIXO'
            END                                            AS classificacao_risco
        FROM faixas;
    """,

    "mart_relatorio_final": """
        DROP TABLE IF EXISTS mart_relatorio_final;
        CREATE TABLE mart_relatorio_final AS
        SELECT
            z.zona,
            z.score_risco,
            z.classificacao_risco,
            z.total_incidentes,
            z.incidentes_criticos,
            z.total_alertas_sensor,
            z.alertas_criticos_sensor,
            z.rondas_irregulares,
            z.pct_rondas_irregulares,
            RANK() OVER (ORDER BY z.score_risco DESC) AS ranking_risco,
            CASE
                WHEN z.classificacao_risco = 'CRITICO' THEN
                    'Reforço imediato de vigilância. Verificação de câmeras e sensores. Auditoria de acessos.'
                WHEN z.classificacao_risco = 'ALTO' THEN
                    'Aumentar frequência de rondas. Revisar protocolos de resposta a incidentes.'
                WHEN z.classificacao_risco = 'MEDIO' THEN
                    'Manter monitoramento. Verificar calibração dos sensores.'
                ELSE
                    'Zona sob controle. Manter rotina operacional.'
            END AS recomendacao
        FROM mart_zona_risco z
        ORDER BY z.score_risco DESC;
    """
}


# ─── Motor do DAG ────────────────────────────────────────────────────────────
class DAGVigilancia:
    def __init__(self, nodes, transformacoes):
        self.nodes = {n["id"]: n for n in nodes}
        self.transformacoes = transformacoes
        self.resultados = {}   # id → {status, duracao, linhas, erro}

    def validar_dag(self):
        """Verifica que o grafo é acíclico (topological sort via Kahn's algorithm)."""
        in_degree = defaultdict(int)
        adj = defaultdict(list)
        # Lista (e não set) para que a ordem de execução seja determinística
        todos = list(self.nodes.keys())

        for nid, node in self.nodes.items():
            for dep in node["dependencias"]:
                if dep not in self.nodes:
                    raise ValueError(f"Dependência desconhecida '{dep}' em '{nid}'")
                adj[dep].append(nid)
                in_degree[nid] += 1

        fila = deque([n for n in todos if in_degree[n] == 0])
        ordem = []

        while fila:
            n = fila.popleft()
            ordem.append(n)
            for vizinho in adj[n]:
                in_degree[vizinho] -= 1
                if in_degree[vizinho] == 0:
                    fila.append(vizinho)

        if len(ordem) != len(todos):
            raise ValueError("⚠️  CICLO DETECTADO no DAG! Pipeline inválido.")

        return ordem

    def executar(self, conn):
        """Executa as transformações SQL na ordem topológica."""
        ordem = self.validar_dag()
        print(f"\n📋 Ordem de execução: {' → '.join(ordem)}\n")

        for node_id in ordem:
            node = self.nodes[node_id]
            print(f"  ⚙️  [{node['camada'].upper()}] {node['label']} ...", end=" ", flush=True)
            inicio = time.time()

            try:
                sql = self.transformacoes[node_id]
                # Executa bloco SQL (pode ter múltiplos statements)
                cursor = conn.cursor()
                cursor.executescript(sql)
                conn.commit()

                # Conta linhas resultantes
                tabela = node_id
                cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
                linhas = cursor.fetchone()[0]

                duracao = round(time.time() - inicio, 4)
                self.resultados[node_id] = {
                    "status": "SUCESSO",
                    "duracao_s": duracao,
                    "linhas": linhas,
                    "erro": None,
                    "executado_em": datetime.now().isoformat()
                }
                print(f"✅  {linhas} linhas  ({duracao}s)")

            except Exception as e:
                duracao = round(time.time() - inicio, 4)
                self.resultados[node_id] = {
                    "status": "ERRO",
                    "duracao_s": duracao,
                    "linhas": 0,
                    "erro": str(e),
                    "executado_em": datetime.now().isoformat()
                }
                print(f"❌  ERRO: {e}")
                raise

        return self.resultados


# ─── Integração Neo4j ─────────────────────────────────────────────────────────
class Neo4jDAGManager:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(
            uri,
            auth=(user, password),
            connection_timeout=5,
            max_connection_lifetime=30
        )

    def close(self):
        self.driver.close()

    def limpar_dag(self, session):
        session.run("MATCH (n:Etapa) DETACH DELETE n")

    def criar_dag(self, session, nodes, resultados):
        """Cria nós e arestas do DAG no Neo4j."""
        for node in nodes:
            res = resultados.get(node["id"], {})
            session.run("""
                MERGE (e:Etapa {id: $id})
                SET e.label        = $label,
                    e.camada       = $camada,
                    e.descricao    = $descricao,
                    e.status       = $status,
                    e.duracao_s    = $duracao,
                    e.linhas       = $linhas,
                    e.executado_em = $executado_em
            """, {
                "id":          node["id"],
                "label":       node["label"],
                "camada":      node["camada"],
                "descricao":   node["descricao"],
                "status":      res.get("status", "PENDENTE"),
                "duracao":     res.get("duracao_s", 0),
                "linhas":      res.get("linhas", 0),
                "executado_em": res.get("executado_em", "")
            })

        for node in nodes:
            for dep in node["dependencias"]:
                session.run("""
                    MATCH (origem:Etapa {id: $dep})
                    MATCH (destino:Etapa {id: $dest})
                    MERGE (origem)-[:ALIMENTA]->(destino)
                """, {"dep": dep, "dest": node["id"]})

    def exportar_para_html(self, session):
        """Retorna dados do DAG para geração do HTML."""
        result_nodes = session.run("MATCH (e:Etapa) RETURN e")
        result_edges = session.run("MATCH (a:Etapa)-[:ALIMENTA]->(b:Etapa) RETURN a.id AS origem, b.id AS destino")

        nodes = [dict(r["e"]) for r in result_nodes]
        edges = [{"origem": r["origem"], "destino": r["destino"]} for r in result_edges]
        return nodes, edges


# ─── Atualização do Visualizador HTML ─────────────────────────────────────────
def _json_para_js(obj):
    """JSON em uma única linha, seguro para embutir dentro de uma tag <script>."""
    return json.dumps(obj, ensure_ascii=False).replace("</", "<\\/")


def atualizar_html_visualizador(nodes, edges, relatorio, sql_dict, samples, html_path):
    """Atualiza os dados de execução diretamente dentro do arquivo dag_vigilancia.html.

    Cada variável (`let NOME = ...;`) ocupa exatamente uma linha no HTML, então a
    substituição é feita por linha inteira (não quebra com ';' dentro de strings).
    """
    if not os.path.exists(html_path):
        print(f"  ⚠️  HTML não encontrado: {html_path}")
        return

    try:
        with open(html_path, "r", encoding="utf-8", newline="") as f:
            content = f.read()

        valores = {
            "NODES": nodes,
            "EDGES": edges,
            "RELATORIO": relatorio,
            "SQL_MAP": sql_dict,
            "SAMPLES": samples,
        }
        for nome, valor in valores.items():
            novo = f"let {nome} = {_json_para_js(valor)};"
            padrao = re.compile(rf"^([ \t]*)let {nome} = [^\r\n]*;[ \t]*(?=\r?$)", re.MULTILINE)
            content, n = padrao.subn(lambda m: m.group(1) + novo, content, count=1)
            if n == 0:
                print(f"  ⚠️  Variável '{nome}' não encontrada no HTML.")

        with open(html_path, "w", encoding="utf-8", newline="") as f:
            f.write(content)
        print(f"  🌐 Visualizador HTML atualizado: {html_path}")
    except Exception as e:
        print(f"  ⚠️  Não foi possível sincronizar o HTML: {e}")


# ─── Ponto de entrada ──────────────────────────────────────────────────────────
def main():
    print("=" * 62)
    print("  🛡️  DAG – Pipeline de Vigilância Patrimonial")
    print("=" * 62)

    # 1. Conecta ao SQLite (sempre fechando a conexão, mesmo em caso de erro)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        return _executar_pipeline(conn)
    finally:
        conn.close()


def _executar_pipeline(conn):

    # 2. Instancia e executa o DAG
    dag = DAGVigilancia(DAG_NODES, TRANSFORMACOES_SQL)

    print("\n🚀 Iniciando execução do pipeline...\n")
    resultados_exec = dag.executar(conn)

    # 3. Lê o relatório final do SQLite
    cursor = conn.cursor()
    relatorio_sql = []
    try:
        cursor.execute("""
            SELECT zona, score_risco, classificacao_risco,
                   total_incidentes, incidentes_criticos,
                   total_alertas_sensor, alertas_criticos_sensor,
                   rondas_irregulares, pct_rondas_irregulares,
                   ranking_risco, recomendacao
            FROM mart_relatorio_final
            ORDER BY ranking_risco
        """)
        cols = [d[0] for d in cursor.description]
        relatorio_sql = [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as e:
        print(f"  ⚠️  Não foi possível ler o relatório: {e}")

    # 4. Publica o DAG no Neo4j
    neo4j_ok = False
    nodes_neo4j, edges_neo4j = [], []
    print("\n📡 Publicando DAG no Neo4j Sandbox...")
    manager = None
    try:
        if GraphDatabase is None:
            raise RuntimeError("driver 'neo4j' não instalado (pip install -r requirements.txt)")
        if not (NEO4J_URI and NEO4J_PASS):
            raise RuntimeError("credenciais não configuradas (NEO4J_URI / NEO4J_PASSWORD)")
        manager = Neo4jDAGManager(NEO4J_URI, NEO4J_USER, NEO4J_PASS)
        manager.driver.verify_connectivity()
        with manager.driver.session() as session:
            manager.limpar_dag(session)
            manager.criar_dag(session, DAG_NODES, resultados_exec)
            nodes_neo4j, edges_neo4j = manager.exportar_para_html(session)
        neo4j_ok = True
        print("  ✅ DAG publicado com sucesso no Neo4j!")
        print("     → Abra o Neo4j Browser e execute:")
        print("       MATCH (e:Etapa) OPTIONAL MATCH (e)-[r:ALIMENTA]->(b) RETURN e,r,b")
    except Exception as e:
        print(f"  ⚠️  Neo4j indisponível ({e}). Usando dados locais para o HTML.")
    finally:
        if manager is not None:
            manager.close()

    # Se Neo4j falhou, usa os dados locais
    if not neo4j_ok:
        nodes_neo4j = [
            {**n, **resultados_exec.get(n["id"], {"status": "PENDENTE", "duracao_s": 0, "linhas": 0, "executado_em": ""})}
            for n in DAG_NODES
        ]
        edges_neo4j = [
            {"origem": dep, "destino": n["id"]}
            for n in DAG_NODES for dep in n["dependencias"]
        ]

    # Amostra dados para o visualizador (preview das tabelas SQLite)
    samples_dict = {}
    for n in DAG_NODES:
        tabela = n["id"]
        try:
            cursor.execute(f"SELECT * FROM {tabela} LIMIT 5")
            cols = [d[0] for d in cursor.description]
            rows = cursor.fetchall()
            samples_dict[tabela] = {"columns": cols, "rows": [list(r) for r in rows]}
        except Exception:
            samples_dict[tabela] = {"columns": [], "rows": []}

    # 5. Sincroniza dados da execução no HTML
    print("\n🌐 Sincronizando visualização HTML...")
    html_path = os.path.join(os.path.dirname(__file__), "dag_vigilancia.html")
    atualizar_html_visualizador(
        nodes_neo4j,
        edges_neo4j,
        relatorio_sql,
        TRANSFORMACOES_SQL,
        samples_dict,
        html_path
    )

    # 6. Sumário final
    print("\n" + "=" * 62)
    print("  ✅ Pipeline concluído com sucesso!\n")
    print("  📁 Arquivos gerados:")
    print(f"     • {DB_PATH}")
    print(f"     • {html_path}")
    if neo4j_ok:
        print(f"\n  🔗 Neo4j Sandbox: {NEO4J_URI}")
        print(f"     Nós Etapa: {len(nodes_neo4j)} | Arestas ALIMENTA: {len(edges_neo4j)}")
    print("\n  🏢 Top 3 Zonas de Risco:")
    for r in relatorio_sql[:3]:
        print(f"     #{r['ranking_risco']} {r['zona']:30s} → {r['classificacao_risco']} (score: {r['score_risco']})")
    print("=" * 62)

    return {
        "nodes": nodes_neo4j,
        "edges": edges_neo4j,
        "relatorio": relatorio_sql,
        "samples": samples_dict,
        "neo4j": neo4j_ok,
    }


# ─── Servidor local (permite executar o pipeline pelo HTML) ───────────────────
def servir(porta=8000):
    """Serve o HTML e expõe POST /api/executar para rodar o pipeline real."""
    import io
    import threading
    import traceback
    import webbrowser
    from contextlib import redirect_stdout
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    base = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base, "dag_vigilancia.html")
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def _enviar(self, codigo, corpo, tipo):
            self.send_response(codigo)
            self.send_header("Content-Type", tipo)
            self.send_header("Content-Length", str(len(corpo)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(corpo)

        def do_GET(self):
            if self.path in ("/", "/index.html", "/dag_vigilancia.html"):
                with open(html_path, "rb") as f:
                    self._enviar(200, f.read(), "text/html; charset=utf-8")
            else:
                self._enviar(404, b"Not found", "text/plain")

        def do_POST(self):
            if self.path != "/api/executar":
                return self._enviar(404, b"Not found", "text/plain")
            buf = io.StringIO()
            with lock:
                try:
                    with redirect_stdout(buf):
                        dados = main()
                    resp = {"ok": True, "log": buf.getvalue(), **dados}
                    codigo = 200
                except Exception as e:
                    resp = {"ok": False, "erro": str(e), "log": buf.getvalue(),
                            "trace": traceback.format_exc()}
                    codigo = 500
            self._enviar(codigo, json.dumps(resp, ensure_ascii=False, default=str).encode("utf-8"),
                         "application/json; charset=utf-8")

        def log_message(self, *args):
            pass

    servidor = ThreadingHTTPServer(("127.0.0.1", porta), Handler)
    url = f"http://127.0.0.1:{porta}/"
    print(f"🌐 Servidor em {url}  (Ctrl+C para encerrar)")
    webbrowser.open(url)
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        servidor.server_close()


if __name__ == "__main__":
    if "--serve" in sys.argv:
        servir()
    else:
        main()
