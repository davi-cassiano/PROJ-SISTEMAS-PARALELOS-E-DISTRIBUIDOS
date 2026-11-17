import os
import random
import time
from datetime import datetime

import msgpack
import zmq

NOME = os.environ.get("NOME", "bot-py")
BROKER = os.environ.get("BROKER", "tcp://broker:5555")
PROXY = os.environ.get("PROXY", "tcp://proxy:5558")

# nomes que o bot usa quando precisa criar um canal
NOMES_CANAIS = ["geral", "games", "musica", "filmes", "esportes",
                "noticias", "tecnologia", "memes", "estudos", "aleatorio"]

# frases que o bot sorteia pra mandar nos canais
FRASES = ["oi pessoal", "alguem ai?", "bom dia", "boa noite", "que dia hein",
          "alguem viu o jogo ontem?", "to estudando sistemas distribuidos",
          "zeromq e muito bom", "kkkkkk", "partiu almoco", "ta chovendo aqui",
          "alguem recomenda uma serie?", "o docker ta funcionando", "teste 123"]


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


def publica(canal, mensagem):
    resposta = requisicao("publicar", {"canal": canal, "usuario": NOME, "mensagem": mensagem})
    if resposta["status"] != "ok":
        print(f"[{NOME}] erro ao publicar no canal {canal}: {resposta['dados'].get('erro')}")


# inscricao nos canais (pub/sub)

sub = context.socket(zmq.SUB)
sub.connect(PROXY)
inscritos = []


def inscreve(canal):
    sub.setsockopt_string(zmq.SUBSCRIBE, canal)
    inscritos.append(canal)
    print(f"[{NOME}] se inscreveu no canal {canal} (inscrito em {inscritos})")


def recebe_publicacoes(segundos):
    # fica esperando publicacoes dos canais durante um tempo
    fim = time.time() + segundos
    while True:
        falta = fim - time.time()
        if falta <= 0:
            return
        if sub.poll(int(falta * 1000)):
            topico, conteudo = sub.recv_multipart()
            recebido = time.time()
            canal = topico.decode()
            # o filtro do zmq e por prefixo, entao confere se e o canal certo
            if canal not in inscritos:
                continue
            msg = msgpack.unpackb(conteudo)
            dados = msg["dados"]
            print(f"[{NOME}] RECEBEU PUBLICACAO: canal={canal} | usuario={dados['usuario']} | "
                  f"mensagem={dados['mensagem']} | enviada={hora_formatada(msg['timestamp'])} | "
                  f"recebida={hora_formatada(recebido)}")


conecta()
time.sleep(2)  # espera o broker e os servidores subirem

login()

while True:
    canais = lista_canais()

    # se tiver menos que 5 canais o bot cria um novo
    if len(canais) < 5:
        opcoes = [c for c in NOMES_CANAIS if c not in canais]
        cria_canal(random.choice(opcoes))
        canais = lista_canais()

    # se estiver inscrito em menos de 3 canais se inscreve em mais um
    if len(inscritos) < 3:
        opcoes = [c for c in canais if c not in inscritos]
        if opcoes:
            inscreve(random.choice(opcoes))

    if not canais:
        recebe_publicacoes(1)
        continue

    # escolhe um canal e manda 10 mensagens, uma por segundo
    canal = random.choice(canais)
    for i in range(10):
        publica(canal, random.choice(FRASES))
        recebe_publicacoes(1)
