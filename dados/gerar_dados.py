"""
Gerador de dados sintéticos para o pipeline de Vigilância Patrimonial.
Cria o banco SQLite com dados brutos simulando eventos reais de segurança.
"""

import sqlite3
import random
from datetime import datetime, timedelta
import os
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = os.path.join(os.path.dirname(__file__), "vigilancia.db")

random.seed(42)

ZONAS = ["Entrada Principal", "Estacionamento", "Almoxarifado", "Sala de Servidores", "Refeitório", "Recepção", "Perímetro Externo"]
CAMERAS = [f"CAM-{str(i).zfill(3)}" for i in range(1, 21)]
SENSORES = [f"SEN-{str(i).zfill(3)}" for i in range(1, 16)]
FUNCIONARIOS = [
    ("F001", "Carlos Mendes", "Supervisor"),
    ("F002", "Ana Souza", "Vigilante"),
    ("F003", "João Lima", "Vigilante"),
    ("F004", "Maria Costa", "Coordenadora"),
    ("F005", "Roberto Silva", "Vigilante"),
    ("F006", "Patrícia Alves", "Vigilante"),
    ("F007", "Fernando Rocha", "Técnico"),
]
TIPOS_EVENTO = ["MOVIMENTO_SUSPEITO", "ACESSO_NEGADO", "ALARME_PERIMETRO", "FALHA_CAMERA", "RONDA_INCOMPLETA", "ACESSO_AUTORIZADO", "ALARME_INCENDIO"]
TIPOS_SENSOR = ["PIR", "MAGNETICO", "TEMPERATURA", "FUMACA", "VIBRAÇÃO"]
STATUS_INCIDENTE = ["ABERTO", "EM_ATENDIMENTO", "RESOLVIDO", "FALSO_POSITIVO"]

# Data-base fixa: com random.seed(42) isso torna os dados 100% reproduzíveis
DATA_BASE = datetime(2026, 1, 1)

def gerar_periodo(dias=30):
    return DATA_BASE - timedelta(days=dias)

def conn_db():
    return sqlite3.connect(DB_PATH)

def criar_tabelas(conn):
    cursor = conn.cursor()

    cursor.executescript("""
        DROP TABLE IF EXISTS eventos_camera;
        DROP TABLE IF EXISTS leituras_sensor;
        DROP TABLE IF EXISTS rondas;
        DROP TABLE IF EXISTS funcionarios;
        DROP TABLE IF EXISTS incidentes;

        CREATE TABLE funcionarios (
            id TEXT PRIMARY KEY,
            nome TEXT NOT NULL,
            cargo TEXT NOT NULL,
            ativo INTEGER DEFAULT 1
        );

        CREATE TABLE eventos_camera (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            camera_id TEXT NOT NULL,
            zona TEXT NOT NULL,
            tipo_evento TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            duracao_segundos INTEGER,
            confianca REAL,
            funcionario_notificado TEXT
        );

        CREATE TABLE leituras_sensor (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id TEXT NOT NULL,
            tipo_sensor TEXT NOT NULL,
            zona TEXT NOT NULL,
            valor REAL NOT NULL,
            alerta INTEGER DEFAULT 0,
            timestamp TEXT NOT NULL
        );

        CREATE TABLE rondas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            funcionario_id TEXT NOT NULL,
            zona TEXT NOT NULL,
            inicio TEXT NOT NULL,
            fim TEXT,
            status TEXT DEFAULT 'CONCLUIDA',
            observacoes TEXT,
            FOREIGN KEY (funcionario_id) REFERENCES funcionarios(id)
        );

        CREATE TABLE incidentes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo TEXT NOT NULL,
            zona TEXT NOT NULL,
            data_hora TEXT NOT NULL,
            gravidade INTEGER NOT NULL,
            status TEXT DEFAULT 'ABERTO',
            origem TEXT NOT NULL,
            camera_id TEXT,
            sensor_id TEXT,
            funcionario_responsavel TEXT
        );
    """)
    conn.commit()

def popular_funcionarios(conn):
    cursor = conn.cursor()
    cursor.executemany(
        "INSERT INTO funcionarios (id, nome, cargo, ativo) VALUES (?, ?, ?, 1)",
        FUNCIONARIOS
    )
    conn.commit()

def popular_eventos_camera(conn, n=500):
    cursor = conn.cursor()
    inicio = gerar_periodo(30)
    registros = []
    for _ in range(n):
        ts = inicio + timedelta(
            days=random.randint(0, 29),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        camera = random.choice(CAMERAS)
        zona = random.choice(ZONAS)
        tipo = random.choice(TIPOS_EVENTO)
        duracao = random.randint(2, 120)
        confianca = round(random.uniform(0.40, 0.99), 2)
        funcionario = random.choice(FUNCIONARIOS)[0] if random.random() > 0.4 else None

        registros.append((camera, zona, tipo, ts.strftime("%Y-%m-%d %H:%M:%S"), duracao, confianca, funcionario))

    cursor.executemany(
        "INSERT INTO eventos_camera (camera_id, zona, tipo_evento, timestamp, duracao_segundos, confianca, funcionario_notificado) VALUES (?,?,?,?,?,?,?)",
        registros
    )
    conn.commit()

def popular_leituras_sensor(conn, n=800):
    cursor = conn.cursor()
    inicio = gerar_periodo(30)
    registros = []
    for _ in range(n):
        ts = inicio + timedelta(
            days=random.randint(0, 29),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        sensor = random.choice(SENSORES)
        tipo = random.choice(TIPOS_SENSOR)
        zona = random.choice(ZONAS)
        valor = round(random.uniform(0, 100), 2)
        alerta = 1 if valor > 80 else 0

        registros.append((sensor, tipo, zona, valor, alerta, ts.strftime("%Y-%m-%d %H:%M:%S")))

    cursor.executemany(
        "INSERT INTO leituras_sensor (sensor_id, tipo_sensor, zona, valor, alerta, timestamp) VALUES (?,?,?,?,?,?)",
        registros
    )
    conn.commit()

def popular_rondas(conn, n=200):
    cursor = conn.cursor()
    inicio = gerar_periodo(30)
    registros = []
    vigilantes = [f[0] for f in FUNCIONARIOS if f[2] == "Vigilante"]
    status_opcoes = ["CONCLUIDA", "CONCLUIDA", "CONCLUIDA", "INCOMPLETA", "NAO_REALIZADA"]

    for _ in range(n):
        ts_inicio = inicio + timedelta(
            days=random.randint(0, 29),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 50)
        )
        duracao = timedelta(minutes=random.randint(10, 90))
        ts_fim = ts_inicio + duracao
        funcionario = random.choice(vigilantes)
        zona = random.choice(ZONAS)
        status = random.choice(status_opcoes)
        obs = "Nenhuma ocorrência" if status == "CONCLUIDA" else "Ronda não finalizada no prazo"
        # Ronda não realizada não tem horário de término (duração fica NULL no pipeline)
        fim_str = None if status == "NAO_REALIZADA" else ts_fim.strftime("%Y-%m-%d %H:%M:%S")

        registros.append((
            funcionario, zona,
            ts_inicio.strftime("%Y-%m-%d %H:%M:%S"),
            fim_str,
            status, obs
        ))

    cursor.executemany(
        "INSERT INTO rondas (funcionario_id, zona, inicio, fim, status, observacoes) VALUES (?,?,?,?,?,?)",
        registros
    )
    conn.commit()

def popular_incidentes(conn, n=150):
    cursor = conn.cursor()
    inicio = gerar_periodo(30)
    registros = []
    origens = ["CAMERA", "SENSOR", "FUNCIONARIO"]

    for _ in range(n):
        ts = inicio + timedelta(
            days=random.randint(0, 29),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        tipo = random.choice(TIPOS_EVENTO)
        zona = random.choice(ZONAS)
        gravidade = random.randint(1, 5)
        status = random.choice(STATUS_INCIDENTE)
        origem = random.choice(origens)
        camera_id = random.choice(CAMERAS) if origem == "CAMERA" else None
        sensor_id = random.choice(SENSORES) if origem == "SENSOR" else None
        resp = random.choice(FUNCIONARIOS)[0]

        registros.append((tipo, zona, ts.strftime("%Y-%m-%d %H:%M:%S"), gravidade, status, origem, camera_id, sensor_id, resp))

    cursor.executemany(
        "INSERT INTO incidentes (tipo, zona, data_hora, gravidade, status, origem, camera_id, sensor_id, funcionario_responsavel) VALUES (?,?,?,?,?,?,?,?,?)",
        registros
    )
    conn.commit()

def main():
    print("🔧 Criando banco de dados de Vigilância Patrimonial...")
    conn = conn_db()
    criar_tabelas(conn)
    popular_funcionarios(conn)
    popular_eventos_camera(conn)
    popular_leituras_sensor(conn)
    popular_rondas(conn)
    popular_incidentes(conn)
    conn.close()
    print(f"✅ Banco criado com sucesso: {DB_PATH}")

if __name__ == "__main__":
    main()
