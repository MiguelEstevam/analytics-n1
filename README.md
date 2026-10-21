# 🛡️ DAG – Pipeline de Vigilância Patrimonial

> **N1-1 – Orquestrando Transformações SQL**  
> Pipeline de engenharia de dados modelado como um Grafo Direcionado Acíclico (DAG), aplicado ao domínio de **Vigilância Patrimonial**.

---

## 📐 Arquitetura do Projeto

```text
analytics-n1/
├── dados/
│   ├── gerar_dados.py        ← Popula o banco com eventos brutos simulados (câmeras, sensores, rondas)
│   └── vigilancia.db         ← Banco de dados SQLite contendo tabelas brutas e derivadas
├── dag_pipeline.py           ← Motor do DAG: Algoritmo de Kahn, execução SQL e integração Neo4j
├── dag_vigilancia.html       ← Interface web interativa (DAG visual, execução animada, SQL e preview)
├── README.md                 ← Documentação completa da avaliação
```

---

## 🔗 O Grafo de Dependências (DAG)

O pipeline é composto por **8 etapas de transformação** organizadas em **3 camadas lógicas**:

```text
[1. Staging]                      [2. Camada Intermediária]        [3. Data Marts]
─────────────────────────────────────────────────────────────────────────────────
stg_eventos_camera  ─────────┐
                              ├──→ int_incidentes_brutos ──┐
stg_leituras_sensor ─────────┼──→ int_alertas_sensor ─────┼──→ mart_zona_risco → mart_relatorio_final
                              │                            │
stg_rondas ──────────────────┼────────────────────────────┘
                              └──→ int_rondas_irregulares ─┘
```

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
2. Nós sem dependências (*in-degree* = 0, camada *Staging*) entram na fila de execução.
3. À medida que um nó é processado, o grau de dependência dos seus vizinhos é decrementado.
4. Se o número de etapas ordenadas for menor que o total de nós, o algoritmo interrompe imediatamente com `ValueError("⚠️ CICLO DETECTADO")`.

A ordem de execução calculada e validada é:
$$\text{stg\_leituras\_sensor} \rightarrow \text{stg\_eventos\_camera} \rightarrow \text{stg\_rondas} \rightarrow \text{int\_alertas\_sensor} \rightarrow \text{int\_incidentes\_brutos} \rightarrow \text{int\_rondas\_irregulares} \rightarrow \text{mart\_zona\_risco} \rightarrow \text{mart\_relatorio\_final}$$

---

## 📊 Regra de Negócio: Score Composto de Risco

O cálculo da camada final combina pesos para diferentes dimensões de segurança:

$$\text{Score de Risco} = (\text{Incidentes Críticos} \times 3.0) + (\text{Alertas Críticos de Sensor} \times 2.0) + (\%\text{ Rondas Irregulares} \times 0.5)$$

| Faixa de Score | Classificação | Diretriz Operacional Prescrita |
|---|---|---|
| $\ge 30$ | 🔴 **CRÍTICO** | Reforço imediato de vigilância. Auditoria de câmeras e sensores. |
| $\ge 20$ | 🟠 **ALTO** | Aumentar frequência de patrulha e revisar protocolos de resposta. |
| $\ge 10$ | 🟡 **MÉDIO** | Manter monitoramento contínuo e verificar calibração dos sensores. |
| $< 10$ | 🟢 **BAIXO** | Zona sob controle. Manter rotina padrão. |

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
- **Layout Determinístico Left-to-Right**: 3 colunas lógicas por camada com conexões suaves em curvas Bézier.
- **Botão `▶ Executar Pipeline`**: Executa passo a passo o grafo animando nós ativos e fluxos nas arestas, transmitindo logs em tempo real para o console integrado.
- **Botão `↺ Zerar`**: Reseta o status de todas as etapas para `PENDENTE`, permitindo novas demonstrações.
- **Inspetor Lateral (Drawer)**:
  - **Aba Visão Geral**: Métricas de linhas, tempo de execução, descrição e links de linhagem upstream/downstream.
  - **Aba Transformação SQL**: Query SQL exata executada naquela etapa, formatada e com realce de sintaxe.
  - **Aba Amostra (SQLite)**: Tabela com as colunas reais e os primeiros 5 registros gerados no banco.
- **Modal Relatório Executivo**: Tabela consolidada com o ranking de risco de todas as zonas patrimoniais.
- **Navegação Estável**: O zoom é controlado exclusivamente pelos botões `+` e `-` (duplo clique seleciona o nó sem saltos de tela).

---

## 🚀 Como Executar

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
Basta abrir o arquivo [dag_vigilancia.html](file:///c:/Users/miguel.lopes/Documents/analytics-n1/dag_vigilancia.html) em qualquer navegador web.

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
