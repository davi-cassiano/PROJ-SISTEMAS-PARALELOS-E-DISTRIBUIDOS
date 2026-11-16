# Projeto CC7261 - Sistema de troca de mensagens instantâneas

Projeto da disciplina de Sistemas Distribuídos. Fizemos uma versão simples de um BBS/IRC, onde os usuários são bots que fazem login, criam canais, publicam mensagens nos canais e recebem as mensagens dos canais em que estão inscritos. Tudo roda em containers com Docker Compose e a troca de mensagens é feita com ZeroMQ.

Fizemos o projeto em dupla, então usamos 2 linguagens: Python e JavaScript (Node.js). Cada linguagem tem o seu cliente (bot) e o seu servidor:

- Python: `python/cliente.py` e `python/servidor.py`
- JavaScript: `javascript/cliente.js` e `javascript/servidor.js`

Os bots em Python conversam com os servidores em JavaScript e o contrário também, porque todos usam o mesmo formato de mensagem.

## Como rodar

```
docker compose up --build
```

Os bots funcionam sozinhos, não tem nenhuma entrada do usuário. Para parar é só dar `Ctrl+C` ou `docker compose down`.

Os dados dos servidores ficam salvos na pasta `dados/`, com uma pasta para cada servidor. Se quiser começar do zero, é só apagar ela.

## Containers

| Serviço | Linguagem | Função | Porta |
|---|---|---|---|
| broker | Python | recebe as requisições dos bots (ROUTER) e repassa para os servidores (DEALER) em round-robin | 5555 e 5556 |
| proxy | Python | proxy do pub/sub, os servidores publicam no XSUB e os bots se inscrevem no XPUB | 5557 e 5558 |
| servidor-py-1 e servidor-py-2 | Python | servidores | - |
| servidor-js-1 e servidor-js-2 | JavaScript | servidores | - |
| bot-py-1 e bot-py-2 | Python | bots | - |
| bot-js-1 e bot-js-2 | JavaScript | bots | - |

São 2 clientes e 2 servidores para cada linguagem. O `broker.py` é o mesmo que o professor passou na aula de request-reply e o `proxy.py` é o da aula de pub/sub, só trocamos as portas para 5557 e 5558.

Temos dois Dockerfiles. O da raiz é a imagem de Python (com `pyzmq` e `msgpack`), usada por o broker, o proxy e pelo cliente e servidor em Python. O `javascript/Dockerfile` é a imagem do Node (com `zeromq` e `@msgpack/msgpack`). Igual nos exemplos das aulas, cada serviço monta o seu arquivo como `main.py` ou `main.js` pelo volume do docker-compose.

## Escolhas

### Linguagens

Escolhemos Python e JavaScript porque as duas têm biblioteca de ZeroMQ e de MessagePack e rodam fácil em container. Cada um da dupla ficou com uma linguagem e fez o cliente e o servidor nela.

### Serialização

Como o enunciado não deixa usar JSON, XML ou texto puro, usamos MessagePack. Escolhemos ele porque não precisa de arquivo de esquema como o Protobuf: a mensagem é um dicionário (ou objeto no JS) normal e a biblioteca transforma em bytes (`msgpack.packb` no Python e `msgpack.encode` no JavaScript). Assim ficou simples fazer as duas linguagens se entenderem.

### Formato das mensagens

Todas as mensagens são um dicionário com estes campos:

```
{
  "tipo": "login",              # tipo da mensagem
  "timestamp": 1794798387.028,  # hora do envio em segundos
  "dados": {...}                # conteudo da mensagem
}
```

Nas respostas também vai o campo `status`, que pode ser `ok` ou `erro`. Quando dá erro, o motivo vai em `dados.erro`.

Requisições que os bots fazem para os servidores:

| tipo | o bot manda | o servidor responde |
|---|---|---|
| login | usuario | usuario |
| canal | canal, usuario | canal |
| listar | nada | canais (lista com os nomes) |
| publicar | canal, usuario, mensagem | canal |

O cliente e o servidor mostram na tela todas as mensagens que mandam e recebem, numa linha só, com todos os campos e o timestamp no formato `dd/mm/aaaa hh:mm:ss.mmm`.

### Erros

- login: o nome do usuário tem que ter de 2 a 30 caracteres (letras, números, `_` ou `-`). Se não tiver, o servidor responde erro e o bot tenta de novo. Não tem senha, então o mesmo usuário pode logar mais de uma vez e todos os logins ficam salvos.
- canal: o nome tem que ter de 2 a 30 caracteres (letras minúsculas, números, `_` ou `-`). Se o canal já existe o servidor responde erro e não cria de novo.
- publicar: dá erro se o nome do canal for inválido ou se a mensagem estiver vazia.
- se o tipo da mensagem não existir, o servidor responde erro.
- se o servidor não responder em 5 segundos, o bot fecha o socket REQ, cria outro e manda a requisição de novo (aí o broker manda para outro servidor). Assim os bots continuam funcionando se algum servidor cair.

### Persistência

Cada servidor salva os dados na sua própria pasta (`dados/<nome do servidor>/`), então nenhum arquivo é compartilhado entre servidores. Usamos o formato JSON Lines, que é um JSON por linha. Escolhemos esse formato porque dá para ir adicionando linhas no final do arquivo sem precisar reescrever tudo e também dá para abrir e ler fácil. O JSON é usado só para salvar no disco, as mensagens trocadas continuam sendo MessagePack.

Arquivos:

- `logins.jsonl`: todos os logins, com o usuário e o timestamp
- `canais.jsonl`: canais criados (nome, quem criou e quando). Quando o servidor reinicia ele lê os canais desse arquivo
- `mensagens.jsonl`: todas as publicações nos canais (canal, usuário, mensagem e timestamp)

## Pub/Sub e bots

### Publicação

O bot manda uma requisição `publicar` para o servidor pelo broker. O servidor publica a mensagem no proxy usando o nome do canal como tópico (a mensagem tem 2 partes: o canal e a mensagem em MessagePack), salva no disco e responde ok ou erro para o bot.

A publicação tem `tipo` igual a `publicacao`, o `timestamp` e em `dados` vai o canal, o usuário e a mensagem.

Até a parte 5 cada servidor só tem uma parte dos dados, então pode chegar num servidor uma publicação para um canal que foi criado em outro. Para não dar erro nesse caso, o servidor só confere se o nome do canal é válido e não se o canal está na lista dele.

### Inscrição

Cada bot tem um socket SUB conectado na porta 5558 do proxy e se inscreve em vários canais usando a mesma conexão. A inscrição é feita só no cliente, com `zmq.SUBSCRIBE`. Como o filtro do ZeroMQ é por prefixo (se inscrever em `geral` também pegaria `geral2`), o bot confere se o tópico que chegou é mesmo um dos canais dele.

Quando chega uma publicação o bot mostra o canal, quem mandou, a mensagem, a hora que foi enviada e a hora que chegou.

### Funcionamento dos bots

Fizemos os bots do jeito que o enunciado pede. Cada bot faz login e depois fica em loop:

1. pega a lista de canais e, se tiver menos de 5, cria um novo
2. se estiver inscrito em menos de 3 canais, se inscreve em mais um
3. escolhe um canal da lista e manda 10 mensagens aleatórias, uma por segundo

Deixamos os passos 1 e 2 dentro do loop para o bot ir chegando nos 5 canais e nas 3 inscrições com o tempo. Enquanto espera 1 segundo entre as mensagens, o bot vai mostrando as publicações que chegam. No Python fizemos isso com `poll` no socket SUB e no JavaScript com um `for await` que roda junto com o resto.
