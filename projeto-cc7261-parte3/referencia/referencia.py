import time
from datetime import datetime

import msgpack
import zmq

NOME = "referencia"

# se o servidor ficar esse tempo sem mandar heartbeat ele sai da lista
TEMPO_LIMITE = 30

ranks = {}           # nome -> rank (nao apaga, assim o nome nunca repete)
ultimo_contato = {}  # nome -> hora do ultimo contato (so os disponiveis)
proximo_rank = 1

relogio = 0  # relogio logico


def incrementa():
    global relogio
    relogio += 1
    return relogio


def atualiza(recebido):
    global relogio
    relogio = max(relogio, recebido)


def hora_formatada(ts):
    return datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M:%S.%f")[:-3]


def mostra(direcao, msg):
    partes = []
    for chave, valor in msg.items():
        if chave == "timestamp":
            valor = hora_formatada(valor)
        partes.append(f"{chave}={valor}")
    print(f"[{NOME}] {direcao}: " + " | ".join(partes))


def registra(nome):
    # da um rank pro servidor (se ja tinha, continua o mesmo)
    global proximo_rank
    if nome not in ranks:
        ranks[nome] = proximo_rank
        proximo_rank += 1
    if nome not in ultimo_contato:
        print(f"[{NOME}] servidor {nome} entrou na lista com rank {ranks[nome]}")
    ultimo_contato[nome] = time.time()
    return ranks[nome]


def lista_disponiveis():
    lista = [{"nome": nome, "rank": ranks[nome]} for nome in ultimo_contato]
    return sorted(lista, key=lambda s: s["rank"])


def remove_parados():
    agora = time.time()
    for nome in list(ultimo_contato):
        if agora - ultimo_contato[nome] > TEMPO_LIMITE:
            del ultimo_contato[nome]
            print(f"[{NOME}] servidor {nome} removido da lista (sem heartbeat ha mais de {TEMPO_LIMITE}s)")


def trata(msg):
    tipo = msg.get("tipo")
    dados = msg.get("dados", {})
    status = "ok"

    if tipo == "rank":
        resposta = {"rank": registra(dados.get("nome", ""))}
    elif tipo == "list":
        resposta = {"servidores": lista_disponiveis()}
    elif tipo == "heartbeat":
        registra(dados.get("nome", ""))
        # aproveita o heartbeat pra mandar a hora certa
        resposta = {"hora": time.time()}
    else:
        status = "erro"
        resposta = {"erro": f"tipo de mensagem desconhecido: {tipo}"}

    return {"tipo": tipo, "status": status, "timestamp": time.time(),
            "relogio": incrementa(), "dados": resposta}


context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5559")

print(f"[{NOME}] servico de referencia rodando na porta 5559")

while True:
    # espera no maximo 1s pra conseguir verificar quem parou de mandar heartbeat
    if socket.poll(1000):
        msg = msgpack.unpackb(socket.recv())
        atualiza(msg.get("relogio", 0))
        mostra("RECEBEU", msg)

        resposta = trata(msg)

        mostra("ENVIOU", resposta)
        socket.send(msgpack.packb(resposta))

    remove_parados()
