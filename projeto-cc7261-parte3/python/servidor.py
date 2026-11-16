import json
import os
import re
import time
from datetime import datetime

import msgpack
import zmq

NOME = os.environ.get("NOME", "servidor-py")
BROKER = os.environ.get("BROKER", "tcp://broker:5556")
PROXY = os.environ.get("PROXY", "tcp://proxy:5557")
REFERENCIA = os.environ.get("REFERENCIA", "tcp://referencia:5559")

# cada servidor tem a sua propria pasta de dados
PASTA = os.path.join("dados", NOME)
os.makedirs(PASTA, exist_ok=True)

NOME_USUARIO_VALIDO = re.compile(r"^[A-Za-z0-9_-]{2,30}$")
NOME_CANAL_VALIDO = re.compile(r"^[a-z0-9_-]{2,30}$")


# relogios

relogio = 0     # relogio logico
desvio = 0.0    # diferenca entre o relogio fisico do servidor e a hora certa


def incrementa():
    # incrementa antes de enviar uma mensagem
    global relogio
    relogio += 1
    return relogio


def atualiza(recebido):
    # quando recebe fica com o maior valor
    global relogio
    relogio = max(relogio, recebido)


def hora():
    # hora do servidor ja com o ajuste
    return time.time() + desvio


def ajusta_hora(hora_certa):
    global desvio
    antes = hora()
    desvio = hora_certa - time.time()
    print(f"[{NOME}] relogio fisico ajustado: {hora_formatada(antes)} -> {hora_formatada(hora_certa)} "
          f"(diferenca de {(hora_certa - antes) * 1000:.1f} ms)")


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


# tratamento das requisicoes dos clientes

def faz_login(msg):
    dados = msg["dados"]
    usuario = dados.get("usuario", "")
    if not NOME_USUARIO_VALIDO.match(usuario):
        return "erro", {"erro": "nome de usuario invalido (2 a 30 letras, numeros, _ ou -)"}
    salva("logins.jsonl", {"usuario": usuario, "timestamp": hora(),
                           "relogio": msg["relogio"], "servidor": NOME})
    return "ok", {"usuario": usuario}


def cria_canal(msg):
    dados = msg["dados"]
    canal = dados.get("canal", "")
    if not NOME_CANAL_VALIDO.match(canal):
        return "erro", {"erro": "nome de canal invalido (2 a 30 letras minusculas, numeros, _ ou -)"}
    if canal in canais:
        return "erro", {"erro": f"canal {canal} ja existe"}
    canais.append(canal)
    salva("canais.jsonl", {"canal": canal, "criado_por": dados.get("usuario", ""),
                           "timestamp": hora(), "relogio": msg["relogio"], "servidor": NOME})
    return "ok", {"canal": canal}


def lista_canais(msg):
    return "ok", {"canais": canais}


def publica(msg):
    dados = msg["dados"]
    canal = dados.get("canal", "")
    mensagem = dados.get("mensagem", "")
    usuario = dados.get("usuario", "")
    if not NOME_CANAL_VALIDO.match(canal):
        return "erro", {"erro": "nome de canal invalido"}
    if not mensagem:
        return "erro", {"erro": "mensagem vazia"}

    # publica no canal usando o nome do canal como topico
    publicacao = {"tipo": "publicacao", "timestamp": hora(), "relogio": incrementa(),
                  "dados": {"canal": canal, "usuario": usuario, "mensagem": mensagem}}
    pub.send_multipart([canal.encode(), msgpack.packb(publicacao)])
    mostra(f"PUBLICOU no canal {canal}", publicacao)

    salva("mensagens.jsonl", {"canal": canal, "usuario": usuario, "mensagem": mensagem,
                              "timestamp": publicacao["timestamp"], "relogio": msg["relogio"],
                              "servidor": NOME})
    return "ok", {"canal": canal}


acoes = {
    "login": faz_login,
    "canal": cria_canal,
    "listar": lista_canais,
    "publicar": publica,
}


def trata(msg):
    tipo = msg.get("tipo")
    acao = acoes.get(tipo)
    if acao is None:
        status, dados = "erro", {"erro": f"tipo de mensagem desconhecido: {tipo}"}
    else:
        status, dados = acao(msg)
    return {"tipo": tipo, "status": status, "timestamp": hora(), "relogio": incrementa(), "dados": dados}


# comunicacao com o servico de referencia

context = zmq.Context()


def pede_referencia(tipo, dados, insiste=True):
    # usa um socket novo em cada pedido pra nao travar se a referencia nao responder
    while True:
        req = context.socket(zmq.REQ)
        req.setsockopt(zmq.RCVTIMEO, 3000)
        req.setsockopt(zmq.LINGER, 0)
        req.connect(REFERENCIA)

        msg = {"tipo": tipo, "timestamp": hora(), "relogio": incrementa(), "dados": dados}
        mostra("ENVIOU para referencia", msg)
        req.send(msgpack.packb(msg))
        try:
            resposta = msgpack.unpackb(req.recv())
        except zmq.Again:
            req.close()
            print(f"[{NOME}] referencia nao respondeu")
            if not insiste:
                return None
            time.sleep(1)
            continue

        req.close()
        atualiza(resposta["relogio"])
        mostra("RECEBEU da referencia", resposta)
        return resposta


def heartbeat():
    # avisa a referencia que ainda ta funcionando e aproveita pra pegar a hora certa
    enviado = time.time()
    resposta = pede_referencia("heartbeat", {"nome": NOME}, insiste=False)
    if resposta is None:
        return
    recebido = time.time()
    # soma metade do tempo de ida e volta da mensagem
    ajusta_hora(resposta["dados"]["hora"] + (recebido - enviado) / 2)


# inicio

socket = context.socket(zmq.REP)
socket.connect(BROKER)

# socket pra publicar nos canais (passa pelo proxy)
pub = context.socket(zmq.PUB)
pub.connect(PROXY)

resposta = pede_referencia("rank", {"nome": NOME})
meu_rank = resposta["dados"]["rank"]
print(f"[{NOME}] meu rank e {meu_rank}")

resposta = pede_referencia("list", {})
print(f"[{NOME}] servidores disponiveis: {resposta['dados']['servidores']}")

print(f"[{NOME}] servidor pronto, conectado no broker {BROKER} e no proxy {PROXY}")

mensagens_recebidas = 0

while True:
    msg = msgpack.unpackb(socket.recv())
    atualiza(msg.get("relogio", 0))
    mostra("RECEBEU", msg)

    resposta = trata(msg)

    mostra("ENVIOU", resposta)
    socket.send(msgpack.packb(resposta))

    # a cada 10 mensagens de clientes manda o heartbeat
    mensagens_recebidas += 1
    if mensagens_recebidas % 10 == 0:
        heartbeat()
