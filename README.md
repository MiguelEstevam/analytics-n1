# 🛡️ DAG – Pipeline de Vigilância Patrimonial

> **N1-1 – Orquestrando Transformações SQL**  
> Pipeline de engenharia de dados modelado como um Grafo Direcionado Acíclico (DAG), aplicado ao domínio de **Vigilância Patrimonial**.

---

## 📐 Arquitetura do Projeto

```text
analytics-n1/
├── dados/
│   ├── gerar_dados.py        ← Popula o banco com eventos brutos simulados (reproduzíveis, seed fixa)
│   └── vigilancia.db         ← SQLite gerado localmente (ignorado pelo git)
├── dag_pipeline.py          ← Motor do DAG: Algoritmo de Kahn, execução SQL e integração Neo4j
├── dag_vigilancia.html       ← Estrutura (markup) da interface web
├── static/
│   ├── style.css             ← Estilos da interface
│   ├── app.js                ← Lógica da interface (D3, execução, inspetor, zerar)
│   └── dados.js              ← Dados da última execução (atualizado automaticamente pelo pipeline)
├── requirements.txt          ← Dependências Python (neo4j)
├── README.md                 ← Documentação completa da avaliação
```

---

## 🔗 O Grafo de Dependências (DAG)

O pipeline parte de **5 tabelas brutas (Raw)** e é composto por **8 etapas de transformação** organizadas em **4 camadas lógicas**:

```text
[1. Raw]              [2. Staging]           [3. Intermediária]          [4. Data Marts]
─────────────────────────────────────────────────────────────────────────────────────────
eventos_camera  ───→ stg_eventos_camera ──┐
                                          ├──→ int_incidentes_brutos ──┐
leituras_sensor ───→ stg_leituras_sensor ─┤                            │
                                          └──→ int_alertas_sensor ─────┼──→ mart_zona_risco → mart_relatorio_final
rondas ──────────┐                                                     │
funcionarios ────┴─→ stg_rondas ─────────────→ int_rondas_irregulares ─┘
incidentes ──────────────────────────────────→ int_incidentes_brutos
funcionarios ────────────────────────────────→ int_incidentes_brutos
```

### 🗄️ Tabelas Brutas (Camada Raw)

São as tabelas de origem criadas por `dados/gerar_dados.py` no SQLite. Não possuem SQL de transformação: no DAG são nós raiz (sem dependências) e o pipeline apenas conta suas linhas.

| Tabela | Linhas | Colunas principais | O que representa |
|---|---|---|---|
| `eventos_camera` | 500 | `id`, `camera_id`, `zona`, `tipo_evento`, `timestamp`, `duracao_segundos`, `confianca`, `funcionario_notificado` | Eventos detectados pelas câmeras (movimento suspeito, acesso negado, alarmes), com a confiança da detecção. |
| `leituras_sensor` | 800 | `id`, `sensor_id`, `tipo_sensor`, `zona`, `valor`, `alerta`, `timestamp` | Leituras de sensores (PIR, magnético, fumaça, temperatura etc.); pode conter duplicatas. |
| `rondas` | 200 | `id`, `funcionario_id`, `zona`, `inicio`, `fim`, `status`, `observacoes` | Rondas realizadas (ou não) pelos vigilantes em cada zona. |
| `funcionarios` | 7 | `id`, `nome`, `cargo`, `ativo` | Cadastro dos vigilantes/funcionários; usado para enriquecer rondas e incidentes. |
| `incidentes` | 150 | `id`, `tipo`, `zona`, `data_hora`, `gravidade`, `status`, `origem`, `camera_id`, `sensor_id`, `funcionario_responsavel` | Incidentes registrados, com gravidade (1–5) e referência à câmera/sensor de origem. |

### 📋 Nós e suas Responsabilidades

| Nó | Camada | Tipo | Responsabilidade |
|---|---|---|---|
| `stg_eventos_camera` | Staging | View/Table | Padroniza eventos das 20 câmeras, filtra confiança $\ge 60\%$, normaliza timestamps e classifica alertas. |
| `stg_leituras_sensor` | Staging | View/Table | Deduplica leituras por sensor/timestamp (`ROW_NUMBER`) e categoriza faixas de risco (CRÍTICO, ALTO, MÉDIO, NORMAL). |
| `stg_rondas` | Staging | View/Table | Une rondas com funcionários via `JOIN`, calcula duração real em minutos e sinaliza rondas incompletas ou não realizadas. |
| `int_incidentes_brutos` | Intermediária | Join/Enrich | Consolida incidentes cruzando metadados de câmeras e sensores relacionados, atribuindo nível de gravidade. |
| `int_alertas_sensor` | Intermediária | Aggregation | Agrega leituras por zona e tipo de sensor (PIR, Magnético, Fumaça, etc.), calculando percentual e contagem de picos críticos. |
| `int_rondas_irregulares` | Intermediária | Aggregation | Calcula o percentual de não-conformidade e abandono de rondas por setor patrimonial. |
| `mart_zona_risco` | Mart | Scoring | Une as três frentes analíticas via `CTEs` e calcula o **Score Composto de Risco** ponderado por zona. |
| `mart_relatorio_final` | Mart | Executive | Ranqueia as zonas por criticidade (`RANK() OVER`) e define recomendações operacionais preventivas. |

### ⛓️ Arestas (Linhagem de Dependências)

| Nó Origem (Upstream) | Nó Destino (Downstream) | Justificativa |
|---|---|---|
| `eventos_camera` | `stg_eventos_camera` | Fonte bruta dos eventos de câmera a padronizar. |
| `leituras_sensor` | `stg_leituras_sensor` | Fonte bruta das leituras a deduplicar e classificar. |
| `rondas` | `stg_rondas` | Fonte bruta das rondas a enriquecer e medir. |
| `funcionarios` | `stg_rondas` | Fornece nome e cargo do vigilante da ronda (`JOIN`). |
| `incidentes` | `int_incidentes_brutos` | Fonte bruta dos incidentes a consolidar. |
| `funcionarios` | `int_incidentes_brutos` | Fornece o nome do responsável pelo incidente (`LEFT JOIN`). |
| `stg_eventos_camera` | `int_incidentes_brutos` | Fornece confiança e validação visual para incidentes. |
| `stg_leituras_sensor` | `int_incidentes_brutos` | Cruza leituras de sensores no momento do incidente. |
| `stg_leituras_sensor` | `int_alertas_sensor` | Base para cálculo de médias, máximas e percentuais de alerta por zona. |
| `stg_rondas` | `int_rondas_irregulares` | Fornece status e duração das rondas para mensurar falhas de patrulha. |
| `int_incidentes_brutos` | `mart_zona_risco` | Alimenta a quantidade e gravidade de incidentes por zona. |
| `int_alertas_sensor` | `mart_zona_risco` | Alimenta o volume de alertas críticos de sensores por zona. |
| `int_rondas_irregulares` | `mart_zona_risco` | Alimenta a taxa de irregularidade das rondas por zona. |
| `mart_zona_risco` | `mart_relatorio_final` | Alimenta o score consolidado para ranqueamento e prescrição de ações. |

---

## ⚙️ Garantia de Ordem e Acíclicidade (Kahn's Algorithm)

Para garantir que o fluxo seja estritamente um **Grafo Direcionado Acíclico (DAG)** e que nenhuma etapa execute antes de suas dependências:
1. É construído o grafo de adjacências e calculado o grau de entrada (*in-degree*) de cada nó.
2. Nós sem dependências (*in-degree* = 0, camada *Raw*) entram na fila de execução.
3. À medida que um nó é processado, o grau de dependência dos seus vizinhos é decrementado.
4. Se o número de etapas ordenadas for menor que o total de nós, o algoritmo interrompe imediatamente com `ValueError("⚠️ CICLO DETECTADO")`.

A ordem de execução é determinística (segue a ordem de declaração dos nós) e validada:
$$\text{eventos\_camera} \rightarrow \text{leituras\_sensor} \rightarrow \text{rondas} \rightarrow \text{funcionarios} \rightarrow \text{incidentes} \rightarrow \text{stg\_eventos\_camera} \rightarrow \text{stg\_leituras\_sensor} \rightarrow \text{stg\_rondas} \rightarrow \text{int\_incidentes\_brutos} \rightarrow \text{int\_alertas\_sensor} \rightarrow \text{int\_rondas\_irregulares} \rightarrow \text{mart\_zona\_risco} \rightarrow \text{mart\_relatorio\_final}$$

---

## 📊 Regra de Negócio: Score Composto de Risco

O score é uma média ponderada de **taxas (0–100)**, o que torna as dimensões comparáveis entre si:

$$\text{Score} = 0.4 \times \%\text{Incidentes Críticos} + 0.3 \times \%\text{Leituras Críticas de Sensor} + 0.3 \times \%\text{Rondas Irregulares}$$

A classificação é relativa, por **quartil do score** (`NTILE(4)`) entre as zonas:

| Quartil | Classificação | Diretriz Operacional Prescrita |
|---|---|---|
| 1º (maiores scores) | 🔴 **CRÍTICO** | Reforço imediato de vigilância. Auditoria de câmeras e sensores. |
| 2º | 🟠 **ALTO** | Aumentar frequência de patrulha e revisar protocolos de resposta. |
| 3º | 🟡 **MÉDIO** | Manter monitoramento contínuo e verificar calibração dos sensores. |
| 4º | 🟢 **BAIXO** | Zona sob controle. Manter rotina padrão. |

---

## 🗃️ Stack Tecnológica

| Componente | Tecnologia | Papel no Projeto |
|---|---|---|
| Transformações SQL | **SQLite 3 (WAL mode)** | Armazena as tabelas e executa as queries SQL com CTEs, Window Functions e Joins. |
| Catálogo & Linhagem | **Neo4j** | Persiste a topologia do DAG com nós `(:Etapa)` e arestas `[:ALIMENTA]`, com tratamento de timeout/fallback. |
| Visualizador Interativo | **HTML5 + CSS + D3.js v7** | Interface web com layout determinístico Left-to-Right, simulação de execução e inspeção de dados. |

---

## 🌐 Interface Web Interativa (`dag_vigilancia.html`)

A visualização HTML conta com recursos para inspeção e apresentação da orquestração:
- **Layout Determinístico Left-to-Right**: 4 colunas lógicas por camada (Raw, Staging, Intermediária, Marts) com conexões suaves em curvas Bézier. As tabelas brutas aparecem em âmbar na primeira coluna e têm filtro próprio (`Raw`).
- **Botão `▶ Executar Pipeline`**: Executa o pipeline real no servidor local (`python dag_pipeline.py --serve`) e anima nós e arestas com os resultados reais, transmitindo o log para o console integrado.
- **Botão `↺ Zerar`**: Reseta o status de todas as etapas para `PENDENTE`, permitindo novas demonstrações.
- **Inspetor Lateral (Drawer)**:
  - **Aba Visão Geral**: Métricas de linhas, tempo de execução, descrição e links de linhagem upstream/downstream.
  - **Aba Transformação SQL**: Query SQL exata executada naquela etapa, formatada e com realce de sintaxe.
  - **Aba Amostra (SQLite)**: Tabela com as colunas reais e os primeiros 5 registros gerados no banco.
- **Modal Relatório Executivo**: Tabela consolidada com o ranking de risco de todas as zonas patrimoniais.
- **Navegação Estável**: O zoom é controlado exclusivamente pelos botões `+` e `-` (duplo clique seleciona o nó sem saltos de tela).

---

## 🚀 Como Executar

### 0. Instalar dependências
```bash
pip install -r requirements.txt
```
Para publicar no Neo4j (opcional), defina `NEO4J_URI`, `NEO4J_USERNAME` e `NEO4J_PASSWORD` como variáveis de ambiente **ou** mantenha o arquivo `Blank Sandbox Sandbox Credentials.txt` (ignorado pelo git) na raiz. Sem credenciais, o pipeline segue normalmente usando dados locais.

### 1. Gerar a Base de Dados
Cria as tabelas brutas no SQLite e popula eventos simulados de vigilância patrimonial:
```bash
python dados/gerar_dados.py
```

### 2. Executar o Pipeline do DAG
Valida o acíclico, executa todas as transformações SQL na ordem topológica, sincroniza o Neo4j e atualiza o visualizador HTML:
```bash
python dag_pipeline.py
```

### 3. Abrir a Visualização
Para apenas visualizar, abra [dag_vigilancia.html](dag_vigilancia.html) no navegador. Para **executar o pipeline pelo botão do HTML**, use o servidor local:
```bash
python dag_pipeline.py --serve
```

---

## 🔍 Consultas Cypher (Neo4j)

Caso deseje visualizar o DAG no Neo4j Browser (`https://sandbox.neo4j.com`):

```cypher
// 1. Visualizar o grafo completo do pipeline com status e métricas
MATCH (e:Etapa) OPTIONAL MATCH (e)-[r:ALIMENTA]->(b:Etapa)
RETURN e, r, b;

// 2. Consultar quem alimenta diretamente a etapa de Score de Risco
MATCH (dep:Etapa)-[:ALIMENTA]->(e:Etapa {id: 'mart_zona_risco'})
RETURN dep.label AS depende_de, dep.camada AS camada, dep.linhas AS linhas_geradas;

// 3. Exibir o caminho crítico completo (da raiz até a entrega final)
MATCH path = (inicio:Etapa)-[:ALIMENTA*]->(fim:Etapa)
WHERE NOT ()-[:ALIMENTA]->(inicio) AND NOT (fim)-[:ALIMENTA]->()
RETURN path;
```
