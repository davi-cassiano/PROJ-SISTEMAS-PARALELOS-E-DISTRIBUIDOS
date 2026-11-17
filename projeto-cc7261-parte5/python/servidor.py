import json
import os
import re
import threading
import time
import uuid
from datetime import datetime

import msgpack
import zmq

NOME = os.environ.get("NOME", "servidor-py")
BROKER = os.environ.get("BROKER", "tcp://broker:5556")
PROXY = os.environ.get("PROXY", "tcp://proxy:5557")          # pra publicar
PROXY_SUB = os.environ.get("PROXY_SUB", "tcp://proxy:5558")  # pra receber o topico servers
REFERENCIA = os.environ.get("REFERENCIA", "tcp://referencia:5559")

# porta que cada servidor abre pra falar com os outros servidores (eleicao e relogio)
PORTA_SERVIDORES = 5560
ENDERECO = os.environ.get("ENDERECO", f"tcp://*:{PORTA_SERVIDORES}")

# cada servidor tem a sua propria pasta de dados
PASTA = os.path.join("dados", NOME)
os.makedirs(PASTA, exist_ok=True)

NOME_USUARIO_VALIDO = re.compile(r"^[A-Za-z0-9_-]{2,30}$")
NOME_CANAL_VALIDO = re.compile(r"^[a-z0-9_-]{2,30}$")
TOPICO_SERVIDORES = "servers"
TOPICO_REPLICACAO = "replicacao"
NOMES_RESERVADOS = [TOPICO_SERVIDORES, TOPICO_REPLICACAO]

# trava pra thread que atende os outros servidores nao mexer junto nas variaveis
trava = threading.Lock()


# relogios

relogio = 0     # relogio logico
desvio = 0.0    # diferenca entre o relogio fisico do servidor e a hora certa


def incrementa():
    # incrementa antes de enviar uma mensagem
    global relogio
    with trava:
        relogio += 1
        return relogio


def atualiza(recebido):
    # quando recebe fica com o maior valor
    global relogio
    with trava:
        relogio = max(relogio, recebido)


def hora():
    # hora do servidor ja com o ajuste
    return time.time() + desvio


def ajusta_hora(ajuste):
    # soma o ajuste (em segundos) no relogio fisico do servidor
    global desvio
    with trava:
        antes = time.time() + desvio
        desvio += ajuste
    print(f"[{NOME}] relogio fisico ajustado: {hora_formatada(antes)} -> {hora_formatada(antes + ajuste)} "
          f"(ajuste de {ajuste * 1000:.1f} ms)")


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


# dados que ficam em memoria (e no disco)
dados_servidor = {
    "logins": carrega("logins.jsonl"),
    "canais": carrega("canais.jsonl"),
    "mensagens": carrega("mensagens.jsonl"),
}
canais = [c["canal"] for c in dados_servidor["canais"]]

# ids de tudo que ja foi guardado, pra nao guardar a mesma coisa duas vezes
ids = set()
for lista in dados_servidor.values():
    for registro in lista:
        ids.add(registro.get("id"))

print(f"[{NOME}] carregados do disco: {len(dados_servidor['logins'])} logins, "
      f"{len(canais)} canais, {len(dados_servidor['mensagens'])} mensagens")


def guarda(categoria, registro):
    # guarda o registro em memoria e no disco, se ainda nao tiver
    with trava:
        if registro["id"] in ids:
            return False
        if categoria == "canais" and registro["canal"] in canais:
            # o mesmo canal foi criado em dois servidores ao mesmo tempo, fica so o primeiro
            return False
        ids.add(registro["id"])
        dados_servidor[categoria].append(registro)
        if categoria == "canais":
            canais.append(registro["canal"])
        salva(f"{categoria}.jsonl", registro)
        return True


def novo_id():
    return f"{NOME}-{uuid.uuid4().hex[:12]}"


def replica(categoria, registro):
    # manda a operacao pros outros servidores pelo topico de replicacao
    msg = {"tipo": "replicacao", "timestamp": hora(), "relogio": incrementa(),
           "dados": {"origem": NOME, "categoria": categoria, "registro": registro}}
    pub.send_multipart([TOPICO_REPLICACAO.encode(), msgpack.packb(msg)])
    mostra(f"PUBLICOU no topico {TOPICO_REPLICACAO}", msg)


def recebe_replicacao(msg):
    dados = msg["dados"]
    if dados["origem"] == NOME:
        return
    if guarda(dados["categoria"], dados["registro"]):
        print(f"[{NOME}] replicou {dados['categoria']} vindo do {dados['origem']}: {dados['registro']}")


# tratamento das requisicoes dos clientes

def faz_login(msg):
    dados = msg["dados"]
    usuario = dados.get("usuario", "")
    if not NOME_USUARIO_VALIDO.match(usuario):
        return "erro", {"erro": "nome de usuario invalido (2 a 30 letras, numeros, _ ou -)"}
    registro = {"id": novo_id(), "usuario": usuario, "timestamp": hora(),
                "relogio": msg["relogio"], "servidor": NOME}
    guarda("logins", registro)
    replica("logins", registro)
    return "ok", {"usuario": usuario}


def cria_canal(msg):
    dados = msg["dados"]
    canal = dados.get("canal", "")
    if not NOME_CANAL_VALIDO.match(canal):
        return "erro", {"erro": "nome de canal invalido (2 a 30 letras minusculas, numeros, _ ou -)"}
    if canal in NOMES_RESERVADOS:
        return "erro", {"erro": f"o nome {canal} e reservado pros servidores"}
    if canal in canais:
        return "erro", {"erro": f"canal {canal} ja existe"}
    registro = {"id": novo_id(), "canal": canal, "criado_por": dados.get("usuario", ""),
                "timestamp": hora(), "relogio": msg["relogio"], "servidor": NOME}
    guarda("canais", registro)
    replica("canais", registro)
    return "ok", {"canal": canal}


def lista_canais(msg):
    return "ok", {"canais": canais}


def publica(msg):
    dados = msg["dados"]
    canal = dados.get("canal", "")
    mensagem = dados.get("mensagem", "")
    usuario = dados.get("usuario", "")
    if not NOME_CANAL_VALIDO.match(canal) or canal in NOMES_RESERVADOS:
        return "erro", {"erro": "nome de canal invalido"}
    if not mensagem:
        return "erro", {"erro": "mensagem vazia"}

    # publica no canal usando o nome do canal como topico
    publicacao = {"tipo": "publicacao", "timestamp": hora(), "relogio": incrementa(),
                  "dados": {"canal": canal, "usuario": usuario, "mensagem": mensagem}}
    pub.send_multipart([canal.encode(), msgpack.packb(publicacao)])
    mostra(f"PUBLICOU no canal {canal}", publicacao)

    registro = {"id": novo_id(), "canal": canal, "usuario": usuario, "mensagem": mensagem,
                "timestamp": publicacao["timestamp"], "relogio": msg["relogio"], "servidor": NOME}
    guarda("mensagens", registro)
    replica("mensagens", registro)
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


# pedidos pra referencia e pros outros servidores

context = zmq.Context()


def pede(destino, endereco, tipo, dados, espera):
    # usa um socket novo em cada pedido, se nao responder no tempo volta None
    req = context.socket(zmq.REQ)
    req.setsockopt(zmq.RCVTIMEO, espera)
    req.setsockopt(zmq.LINGER, 0)
    try:
        req.connect(endereco)
    except zmq.ZMQError:
        req.close()
        print(f"[{NOME}] nao consegui conectar em {destino}")
        return None

    msg = {"tipo": tipo, "timestamp": hora(), "relogio": incrementa(), "dados": dados}
    mostra(f"ENVIOU para {destino}", msg)
    req.send(msgpack.packb(msg))
    try:
        resposta = msgpack.unpackb(req.recv())
    except zmq.Again:
        req.close()
        print(f"[{NOME}] {destino} nao respondeu")
        return None

    req.close()
    atualiza(resposta["relogio"])
    mostra(f"RECEBEU de {destino}", resposta)
    return resposta


def pede_referencia(tipo, dados, insiste=True):
    while True:
        resposta = pede("referencia", REFERENCIA, tipo, dados, 3000)
        if resposta is not None or not insiste:
            return resposta
        time.sleep(1)


def pede_servidor(nome, tipo, dados, espera=1000):
    return pede(nome, f"tcp://{nome}:{PORTA_SERVIDORES}", tipo, dados, espera)


def lista_servidores():
    resposta = pede_referencia("list", {}, insiste=False)
    if resposta is None:
        return []
    return resposta["dados"]["servidores"]


def heartbeat():
    # avisa a referencia que ainda ta funcionando
    pede_referencia("heartbeat", {"nome": NOME}, insiste=False)


def sincroniza_dados(servidores):
    # pede todos os dados pra um dos outros servidores e guarda o que eu nao tenho
    for s in servidores:
        if s["nome"] == NOME:
            continue
        resposta = pede_servidor(s["nome"], "sincronizar", {"nome": NOME}, espera=5000)
        if resposta is None or resposta["status"] != "ok":
            continue
        novos = 0
        for categoria, registros in resposta["dados"].items():
            for registro in registros:
                if guarda(categoria, registro):
                    novos += 1
        print(f"[{NOME}] sincronizei com o {s['nome']}: {novos} registros novos")
        return
    print(f"[{NOME}] nenhum outro servidor pra sincronizar os dados")


# eleicao (algoritmo do valentao)

coordenador = None
precisa_eleicao = False
esperando_anuncio = None  # hora que comecou a esperar o anuncio do novo coordenador


def eleicao():
    global esperando_anuncio
    print(f"[{NOME}] comecando eleicao (meu rank e {meu_rank})")
    maiores = [s for s in lista_servidores() if s["rank"] > meu_rank]

    # pede eleicao pra todos com rank maior que o meu
    alguem_respondeu = False
    for s in maiores:
        resposta = pede_servidor(s["nome"], "eleicao", {"nome": NOME, "rank": meu_rank})
        if resposta is not None and resposta["status"] == "ok":
            alguem_respondeu = True

    if alguem_respondeu:
        # algum maior ta vivo, agora ele que continua a eleicao
        print(f"[{NOME}] servidor com rank maior respondeu, esperando o anuncio do coordenador")
        esperando_anuncio = time.time()
    else:
        # ninguem maior respondeu, entao eu sou o coordenador
        anuncia_coordenador()


def anuncia_coordenador():
    global coordenador, esperando_anuncio
    coordenador = NOME
    esperando_anuncio = None
    msg = {"tipo": "coordenador", "timestamp": hora(), "relogio": incrementa(),
           "dados": {"coordenador": NOME}}
    pub.send_multipart([TOPICO_SERVIDORES.encode(), msgpack.packb(msg)])
    mostra(f"PUBLICOU no topico {TOPICO_SERVIDORES}", msg)
    print(f"[{NOME}] agora eu sou o coordenador")


def recebe_anuncio(msg):
    global coordenador, esperando_anuncio
    coordenador = msg["dados"]["coordenador"]
    esperando_anuncio = None
    print(f"[{NOME}] coordenador agora e {coordenador}")


# sincronizacao do relogio (algoritmo de Berkeley)

def sincroniza_relogio():
    global coordenador
    if coordenador == NOME:
        print(f"[{NOME}] sou o coordenador, nao preciso pedir a hora")
        return
    if coordenador is None:
        eleicao()
        return

    enviado = time.time()
    resposta = pede_servidor(coordenador, "relogio", {"nome": NOME}, espera=8000)
    if resposta is None or resposta["status"] != "ok":
        print(f"[{NOME}] coordenador {coordenador} nao respondeu, vou pedir eleicao")
        coordenador = None
        eleicao()
        return

    dados = resposta["dados"]
    if "ajuste" in dados:
        ajusta_hora(dados["ajuste"])
    else:
        # o coordenador nao conseguiu a minha hora, entao usa a hora dele + metade do tempo de ida e volta
        ajusta_hora(dados["hora"] + (time.time() - enviado) / 2 - hora())


def berkeley(msg):
    # sou o coordenador: pergunto a hora de todos os servidores, calculo a diferenca
    # de cada um pro meu relogio e tiro a media dessas diferencas
    quem_pediu = msg["dados"]["nome"]
    diferencas = {NOME: 0.0}

    nomes = [quem_pediu] + [s["nome"] for s in lista_servidores()]
    for nome in nomes:
        if nome in diferencas:
            continue
        enviado = time.time()
        resposta = pede_servidor(nome, "hora", {"nome": NOME})
        if resposta is not None:
            # hora do outro servidor + metade do tempo de ida e volta - a minha hora agora
            diferencas[nome] = resposta["dados"]["hora"] + (time.time() - enviado) / 2 - hora()

    media = sum(diferencas.values()) / len(diferencas)
    print(f"[{NOME}] berkeley: diferencas pro meu relogio (ms) = "
          f"{ {n: round(d * 1000, 1) for n, d in diferencas.items()} } -> media = {media * 1000:.1f} ms")

    # cada um tem que somar (media - diferenca dele) no relogio
    ajusta_hora(media)
    dados = {"hora": hora()}
    if quem_pediu in diferencas:
        dados["ajuste"] = media - diferencas[quem_pediu]
    return dados


# atende os pedidos dos outros servidores (roda numa thread separada)

def atende_servidores():
    global precisa_eleicao
    rep = context.socket(zmq.REP)
    rep.bind(ENDERECO)
    while True:
        msg = msgpack.unpackb(rep.recv())
        atualiza(msg.get("relogio", 0))
        mostra("RECEBEU de servidor", msg)
        tipo = msg.get("tipo")
        status = "ok"

        if tipo == "eleicao":
            # responde OK e comeca uma eleicao tambem
            dados = {}
            precisa_eleicao = True
        elif tipo == "hora":
            dados = {"hora": hora()}
        elif tipo == "sincronizar":
            # manda tudo o que eu tenho pro servidor que acabou de entrar
            with trava:
                dados = {categoria: list(lista) for categoria, lista in dados_servidor.items()}
        elif tipo == "relogio":
            if coordenador == NOME:
                dados = berkeley(msg)
            else:
                status, dados = "erro", {"erro": "nao sou o coordenador"}
        else:
            status, dados = "erro", {"erro": f"tipo de mensagem desconhecido: {tipo}"}

        resposta = {"tipo": tipo, "status": status, "timestamp": hora(),
                    "relogio": incrementa(), "dados": dados}
        mostra("ENVIOU para servidor", resposta)
        rep.send(msgpack.packb(resposta))


# inicio

socket = context.socket(zmq.REP)
socket.connect(BROKER)

# socket pra publicar nos canais e no topico servers (passa pelo proxy)
pub = context.socket(zmq.PUB)
pub.connect(PROXY)

# socket pra saber quem e o novo coordenador
sub = context.socket(zmq.SUB)
sub.connect(PROXY_SUB)
sub.setsockopt_string(zmq.SUBSCRIBE, TOPICO_SERVIDORES)
sub.setsockopt_string(zmq.SUBSCRIBE, TOPICO_REPLICACAO)

resposta = pede_referencia("rank", {"nome": NOME})
meu_rank = resposta["dados"]["rank"]
print(f"[{NOME}] meu rank e {meu_rank}")

resposta = pede_referencia("list", {})
print(f"[{NOME}] servidores disponiveis: {resposta['dados']['servidores']}")

# pega os dados que os outros servidores ja tem (caso eu tenha entrado depois ou ficado fora)
sincroniza_dados(resposta["dados"]["servidores"])

threading.Thread(target=atende_servidores, daemon=True).start()
time.sleep(1)  # da um tempo pro sub conectar no proxy

# quando entra ja faz uma eleicao pra descobrir quem e o coordenador
eleicao()

print(f"[{NOME}] servidor pronto, conectado no broker {BROKER} e no proxy {PROXY}")

poller = zmq.Poller()
poller.register(socket, zmq.POLLIN)
poller.register(sub, zmq.POLLIN)

mensagens_recebidas = 0

while True:
    eventos = dict(poller.poll(500))

    if sub in eventos:
        topico, conteudo = sub.recv_multipart()
        topico = topico.decode()
        if topico in NOMES_RESERVADOS:
            msg = msgpack.unpackb(conteudo)
            atualiza(msg["relogio"])
            mostra(f"RECEBEU do topico {topico}", msg)
            if topico == TOPICO_SERVIDORES:
                recebe_anuncio(msg)
            else:
                recebe_replicacao(msg)

    if socket in eventos:
        msg = msgpack.unpackb(socket.recv())
        atualiza(msg.get("relogio", 0))
        mostra("RECEBEU", msg)

        resposta = trata(msg)

        mostra("ENVIOU", resposta)
        socket.send(msgpack.packb(resposta))

        mensagens_recebidas += 1
        # a cada 10 mensagens de clientes manda o heartbeat
        if mensagens_recebidas % 10 == 0:
            heartbeat()
        # a cada 15 mensagens sincroniza o relogio com o coordenador
        if mensagens_recebidas % 15 == 0:
            sincroniza_relogio()

    # outro servidor pediu eleicao
    if precisa_eleicao:
        precisa_eleicao = False
        eleicao()

    # se ninguem anunciou o coordenador depois de um tempo, faz a eleicao de novo
    if esperando_anuncio is not None and time.time() - esperando_anuncio > 10:
        print(f"[{NOME}] ninguem anunciou o coordenador, fazendo eleicao de novo")
        eleicao()
