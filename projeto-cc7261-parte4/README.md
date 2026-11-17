# Projeto CC7261 - Sistema de troca de mensagens instantâneas

Projeto da disciplina de Sistemas Distribuídos. Fizemos uma versão simples de um BBS/IRC, onde os usuários são bots que fazem login, criam canais, publicam mensagens nos canais e recebem as mensagens dos canais em que estão inscritos. Tudo roda em containers com Docker Compose e a troca de mensagens é feita com ZeroMQ.

Fizemos o projeto em dupla, então usamos 2 linguagens: Python e JavaScript (Node.js). Cada linguagem tem o seu cliente (bot) e o seu servidor:

- Python: `python/cliente.py` e `python/servidor.py`
- JavaScript: `javascript/cliente.js` e `javascript/servidor.js`

Os bots em Python conversam com os servidores em JavaScript e o contrário também, porque todos usam o mesmo formato de mensagem. Os servidores das duas linguagens também conversam entre si.

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
| referencia | Python | serviço de referência (rank, lista de servidores e heartbeat) | 5559 |
| servidor-py-1 e servidor-py-2 | Python | servidores | 5560 |
| servidor-js-1 e servidor-js-2 | JavaScript | servidores | 5560 |
| bot-py-1 e bot-py-2 | Python | bots | - |
| bot-js-1 e bot-js-2 | JavaScript | bots | - |

São 2 clientes e 2 servidores para cada linguagem. O `broker.py` é o mesmo que o professor passou na aula de request-reply e o `proxy.py` é o da aula de pub/sub, só trocamos as portas para 5557 e 5558.

Temos dois Dockerfiles. O da raiz é a imagem de Python (com `pyzmq` e `msgpack`), usada por o broker, o proxy, a referência e pelo cliente e servidor em Python. O `javascript/Dockerfile` é a imagem do Node (com `zeromq` e `@msgpack/msgpack`). Igual nos exemplos das aulas, cada serviço monta o seu arquivo como `main.py` ou `main.js` pelo volume do docker-compose.

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
  "relogio": 15,                # relogio logico de quem enviou
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
- não dá para criar canal com o nome `servers`, porque os servidores usam esse tópico.
- se o tipo da mensagem não existir, o servidor responde erro.
- se o servidor não responder em 5 segundos, o bot fecha o socket REQ, cria outro e manda a requisição de novo (aí o broker manda para outro servidor). Assim os bots continuam funcionando se algum servidor cair.

### Persistência

Cada servidor salva os dados na sua própria pasta (`dados/<nome do servidor>/`), então nenhum arquivo é compartilhado entre servidores. Usamos o formato JSON Lines, que é um JSON por linha. Escolhemos esse formato porque dá para ir adicionando linhas no final do arquivo sem precisar reescrever tudo e também dá para abrir e ler fácil. O JSON é usado só para salvar no disco, as mensagens trocadas continuam sendo MessagePack.

Arquivos:

- `logins.jsonl`: todos os logins, com o usuário e o timestamp
- `canais.jsonl`: canais criados (nome, quem criou e quando). Quando o servidor reinicia ele lê os canais desse arquivo
- `mensagens.jsonl`: todas as publicações nos canais (canal, usuário, mensagem e timestamp)

Desde a parte 3 os registros também guardam o `relogio` da mensagem.

## Pub/Sub e bots

### Publicação

O bot manda uma requisição `publicar` para o servidor pelo broker. O servidor publica a mensagem no proxy usando o nome do canal como tópico (a mensagem tem 2 partes: o canal e a mensagem em MessagePack), salva no disco e responde ok ou erro para o bot.

A publicação tem `tipo` igual a `publicacao`, o `timestamp`, o `relogio` e em `dados` vai o canal, o usuário e a mensagem.

Até a parte 5 cada servidor só tem uma parte dos dados, então pode chegar num servidor uma publicação para um canal que foi criado em outro. Para não dar erro nesse caso, o servidor só confere se o nome do canal é válido e não se o canal está na lista dele.

### Inscrição

Cada bot tem um socket SUB conectado na porta 5558 do proxy e se inscreve em vários canais usando a mesma conexão. A inscrição é feita só no cliente, com `zmq.SUBSCRIBE`. Como o filtro do ZeroMQ é por prefixo (se inscrever em `geral` também pegaria `geral2`), o bot confere se o tópico que chegou é mesmo um dos canais dele.

Quando chega uma publicação o bot mostra o canal, quem mandou, a mensagem, a hora que foi enviada e a hora que chegou, e também o relógio lógico.

### Funcionamento dos bots

Fizemos os bots do jeito que o enunciado pede. Cada bot faz login e depois fica em loop:

1. pega a lista de canais e, se tiver menos de 5, cria um novo
2. se estiver inscrito em menos de 3 canais, se inscreve em mais um
3. escolhe um canal da lista e manda 10 mensagens aleatórias, uma por segundo

Deixamos os passos 1 e 2 dentro do loop para o bot ir chegando nos 5 canais e nas 3 inscrições com o tempo. Enquanto espera 1 segundo entre as mensagens, o bot vai mostrando as publicações que chegam. No Python fizemos isso com `poll` no socket SUB e no JavaScript com um `for await` que roda junto com o resto.

## Relógios e serviço de referência

### Relógio lógico

Todos os processos (os bots, os servidores e a referência) têm um contador que é o relógio lógico:

1. antes de mandar uma mensagem o contador aumenta 1 e vai junto no campo `relogio`
2. quando recebe uma mensagem, o processo fica com o maior valor entre o contador dele e o `relogio` que veio na mensagem

Isso é feito em todas as mensagens: requisições e respostas, publicações, mensagens com a referência e mensagens entre servidores.

### Serviço de referência

É um container novo (`referencia`, porta 5559, REQ/REP) que só conversa com os servidores. Ele tem 3 tipos de requisição:

| tipo | o servidor manda | a referência responde |
|---|---|---|
| rank | nome | rank |
| list | nada | lista com nome e rank dos servidores disponíveis |
| heartbeat | nome | nada (desde a parte 4 não manda mais a hora) |

- rank: quando o servidor sobe ele pede o rank mandando o nome dele. A referência dá os ranks em ordem (1, 2, 3...) e guarda o nome com o rank. Se o mesmo nome pedir de novo recebe o mesmo rank, então não tem nome repetido.
- list: devolve nome e rank de todos os servidores disponíveis.
- heartbeat: o servidor manda um heartbeat a cada 10 mensagens de clientes. Se a referência ficar 30 segundos sem receber nada de um servidor, tira ele da lista. Se ele voltar a mandar, entra de novo com o mesmo rank.

## Eleição e sincronização do relógio

Na parte 4 a referência parou de mandar a hora e um dos servidores passou a ser o coordenador, que cuida da hora. Cada servidor guarda o nome do coordenador na variável `coordenador`.

Para os servidores conversarem entre si, cada um abre um socket REP na porta 5560. No Docker Compose o nome do serviço é o mesmo nome do servidor, então um servidor acha o outro pelo endereço `tcp://<nome>:5560`, usando a lista da referência. Mensagens entre servidores:

| tipo | para que serve |
|---|---|
| eleicao | pedir eleição (a resposta é ok) |
| relogio | pedir a hora para o coordenador |
| hora | o coordenador pedindo a hora de um servidor (usado no Berkeley) |

### Eleição

Usamos o algoritmo do valentão (bully) que vimos na aula, com o rank que a referência dá:

1. o servidor pega a lista de servidores na referência e manda `eleicao` para todos que têm rank maior que o dele
2. quem recebe `eleicao` responde ok e começa uma eleição também
3. se nenhum servidor com rank maior responder (espera 1 segundo), ele vira o coordenador e avisa todo mundo publicando o nome dele no tópico `servers`
4. se algum maior respondeu, ele espera o anúncio. Se ninguém anunciar em 10 segundos, faz a eleição de novo

Todos os servidores se inscrevem no tópico `servers` e atualizam a variável `coordenador` quando chega o anúncio. A eleição acontece quando o servidor sobe e quando o coordenador não responde o pedido de hora. Então se o coordenador cair, o próximo com maior rank vira coordenador.

### Berkeley

A cada 15 mensagens de clientes o servidor manda `relogio` para o coordenador, e o coordenador faz o algoritmo de Berkeley:

1. pergunta a hora de todos os servidores da lista, inclusive de quem pediu
2. calcula a diferença de cada um para o relógio dele (hora que recebeu + metade do tempo de ida e volta - hora dele)
3. tira a média das diferenças (ele mesmo entra com diferença 0) e ajusta o próprio relógio com essa média
4. responde para quem pediu com a hora certa e o ajuste que ele tem que fazer (média - diferença dele)

O servidor soma esse ajuste no relógio. Mandamos o ajuste e não só a hora porque o coordenador demora um pouco para perguntar a hora de todo mundo, e esse tempo ia atrapalhar se o servidor usasse a hora direto. Se o coordenador não responder, o servidor pede eleição.

No Python o socket que atende os outros servidores roda numa thread separada (com uma trava nas variáveis compartilhadas), para o servidor conseguir responder o coordenador enquanto espera uma resposta. No JavaScript não precisou, porque cada socket já tem o seu `for await`.

O heartbeat para a referência continua a cada 10 mensagens, mas sem a hora na resposta.
