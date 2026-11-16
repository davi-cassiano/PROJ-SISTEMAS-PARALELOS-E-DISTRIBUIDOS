import json
import os
import re
import time
from datetime import datetime

import msgpack
import zmq

NOME = os.environ.get("NOME", "servidor-py")
BROKER = os.environ.get("BROKER", "tcp://broker:5556")

# cada servidor tem a sua propria pasta de dados
PASTA = os.path.join("dados", NOME)
os.makedirs(PASTA, exist_ok=True)

NOME_USUARIO_VALIDO = re.compile(r"^[A-Za-z0-9_-]{2,30}$")
NOME_CANAL_VALIDO = re.compile(r"^[a-z0-9_-]{2,30}$")


def hora_formatada(ts):
    return datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M:%S.%f")[:-3]


def mostra(direcao, msg):
    # mostra a mensagem inteira numa linha so
    partes = []
    for chave, valor in msg.items():
        if chave == "timestamp":
            valor = hora_formatada(valor)
        partes.append(f"{chave}={valor}")
    print(f"[{NOME}] {direcao}: " + " | ".join(partes))


# persistencia (um json por linha)

def salva(arquivo, registro):
    with open(os.path.join(PASTA, arquivo), "a", encoding="utf-8") as f:
        f.write(json.dumps(registro, ensure_ascii=False) + "\n")


def carrega(arquivo):
    caminho = os.path.join(PASTA, arquivo)
    if not os.path.exists(caminho):
        return []
    with open(caminho, encoding="utf-8") as f:
        return [json.loads(linha) for linha in f if linha.strip()]


canais = [c["canal"] for c in carrega("canais.jsonl")]
print(f"[{NOME}] canais carregados do disco: {canais}")


# tratamento das requisicoes

def faz_login(dados):
    usuario = dados.get("usuario", "")
    if not NOME_USUARIO_VALIDO.match(usuario):
        return "erro", {"erro": "nome de usuario invalido (2 a 30 letras, numeros, _ ou -)"}
    salva("logins.jsonl", {"usuario": usuario, "timestamp": time.time(), "servidor": NOME})
    return "ok", {"usuario": usuario}


def cria_canal(dados):
    canal = dados.get("canal", "")
    if not NOME_CANAL_VALIDO.match(canal):
        return "erro", {"erro": "nome de canal invalido (2 a 30 letras minusculas, numeros, _ ou -)"}
    if canal in canais:
        return "erro", {"erro": f"canal {canal} ja existe"}
    canais.append(canal)
    salva("canais.jsonl", {"canal": canal, "criado_por": dados.get("usuario", ""),
                           "timestamp": time.time(), "servidor": NOME})
    return "ok", {"canal": canal}


def lista_canais(dados):
    return "ok", {"canais": canais}


acoes = {
    "login": faz_login,
    "canal": cria_canal,
    "listar": lista_canais,
}


def trata(msg):
    tipo = msg.get("tipo")
    acao = acoes.get(tipo)
    if acao is None:
        status, dados = "erro", {"erro": f"tipo de mensagem desconhecido: {tipo}"}
    else:
        status, dados = acao(msg.get("dados", {}))
    return {"tipo": tipo, "status": status, "timestamp": time.time(), "dados": dados}


context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect(BROKER)

print(f"[{NOME}] servidor pronto, conectado no broker {BROKER}")

while True:
    msg = msgpack.unpackb(socket.recv())
    mostra("RECEBEU", msg)

    resposta = trata(msg)

    mostra("ENVIOU", resposta)
    socket.send(msgpack.packb(resposta))
