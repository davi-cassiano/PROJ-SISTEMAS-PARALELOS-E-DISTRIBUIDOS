import os
import random
import time
from datetime import datetime

import msgpack
import zmq

NOME = os.environ.get("NOME", "bot-py")
BROKER = os.environ.get("BROKER", "tcp://broker:5555")

# nomes que o bot usa quando precisa criar um canal
NOMES_CANAIS = ["geral", "games", "musica", "filmes", "esportes",
                "noticias", "tecnologia", "memes", "estudos", "aleatorio"]


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


context = zmq.Context()
socket = None


def conecta():
    # cria o socket de novo (o REQ trava se ficar sem resposta)
    global socket
    if socket is not None:
        socket.close(linger=0)
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, 5000)
    socket.connect(BROKER)


def requisicao(tipo, dados):
    # se nao tiver resposta em 5s recria o socket e manda de novo
    while True:
        msg = {"tipo": tipo, "timestamp": time.time(), "dados": dados}
        mostra("ENVIOU", msg)
        socket.send(msgpack.packb(msg))
        try:
            resposta = msgpack.unpackb(socket.recv())
        except zmq.Again:
            print(f"[{NOME}] sem resposta do servidor, tentando de novo")
            conecta()
            continue
        mostra("RECEBEU", resposta)
        return resposta


def login():
    # repete ate o servidor aceitar o login
    while True:
        resposta = requisicao("login", {"usuario": NOME})
        if resposta["status"] == "ok":
            print(f"[{NOME}] login feito")
            return
        print(f"[{NOME}] erro no login: {resposta['dados'].get('erro')}, tentando de novo")
        time.sleep(1)


def lista_canais():
    resposta = requisicao("listar", {})
    return resposta["dados"].get("canais", [])


def cria_canal(canal):
    resposta = requisicao("canal", {"canal": canal, "usuario": NOME})
    if resposta["status"] != "ok":
        print(f"[{NOME}] nao criou o canal {canal}: {resposta['dados'].get('erro')}")


conecta()
time.sleep(2)  # espera o broker e os servidores subirem

login()

while True:
    canais = lista_canais()
    # se tiver menos que 5 canais o bot cria um novo
    if len(canais) < 5:
        opcoes = [c for c in NOMES_CANAIS if c not in canais]
        cria_canal(random.choice(opcoes))
    time.sleep(3)
