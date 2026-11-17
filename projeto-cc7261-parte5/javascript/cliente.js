const zmq = require("zeromq");
const msgpack = require("@msgpack/msgpack");

const NOME = process.env.NOME || "bot-js";
const BROKER = process.env.BROKER || "tcp://broker:5555";
const PROXY = process.env.PROXY || "tcp://proxy:5558";

// nomes que o bot usa quando precisa criar um canal
const NOMES_CANAIS = ["geral", "games", "musica", "filmes", "esportes",
  "noticias", "tecnologia", "memes", "estudos", "aleatorio"];

// frases que o bot sorteia pra mandar nos canais
const FRASES = ["oi pessoal", "alguem ai?", "bom dia", "boa noite", "que dia hein",
  "alguem viu o jogo ontem?", "to estudando sistemas distribuidos",
  "zeromq e muito bom", "kkkkkk", "partiu almoco", "ta chovendo aqui",
  "alguem recomenda uma serie?", "o docker ta funcionando", "teste 123"];

let relogio = 0; // relogio logico

function incrementa() {
  // incrementa antes de enviar uma mensagem
  relogio += 1;
  return relogio;
}

function atualiza(recebido) {
  // quando recebe fica com o maior valor
  relogio = Math.max(relogio, recebido);
}

function agora() {
  return Date.now() / 1000;
}

function espera(segundos) {
  return new Promise((resolve) => setTimeout(resolve, segundos * 1000));
}

function sorteia(lista) {
  return lista[Math.floor(Math.random() * lista.length)];
}

function horaFormatada(ts) {
  const d = new Date(ts * 1000);
  const dois = (n) => String(n).padStart(2, "0");
  return `${dois(d.getDate())}/${dois(d.getMonth() + 1)}/${d.getFullYear()} ` +
    `${dois(d.getHours())}:${dois(d.getMinutes())}:${dois(d.getSeconds())}.` +
    String(d.getMilliseconds()).padStart(3, "0");
}

function mostra(direcao, msg) {
  // mostra a mensagem inteira numa linha so
  const partes = [];
  for (const [chave, valor] of Object.entries(msg)) {
    if (chave === "timestamp") partes.push(`${chave}=${horaFormatada(valor)}`);
    else if (typeof valor === "object") partes.push(`${chave}=${JSON.stringify(valor)}`);
    else partes.push(`${chave}=${valor}`);
  }
  console.log(`[${NOME}] ${direcao}: ` + partes.join(" | "));
}

let socket = null;

function conecta() {
  // cria o socket de novo (o REQ trava se ficar sem resposta)
  if (socket) socket.close();
  socket = new zmq.Request({ receiveTimeout: 5000 });
  socket.connect(BROKER);
}

async function requisicao(tipo, dados) {
  // se nao tiver resposta em 5s recria o socket e manda de novo
  while (true) {
    const msg = { tipo, timestamp: agora(), relogio: incrementa(), dados };
    mostra("ENVIOU", msg);
    await socket.send(msgpack.encode(msg));
    try {
      const [buffer] = await socket.receive();
      const resposta = msgpack.decode(buffer);
      atualiza(resposta.relogio);
      mostra("RECEBEU", resposta);
      return resposta;
    } catch (erro) {
      console.log(`[${NOME}] sem resposta do servidor, tentando de novo`);
      conecta();
    }
  }
}

async function login() {
  // repete ate o servidor aceitar o login
  while (true) {
    const resposta = await requisicao("login", { usuario: NOME });
    if (resposta.status === "ok") {
      console.log(`[${NOME}] login feito`);
      return;
    }
    console.log(`[${NOME}] erro no login: ${resposta.dados.erro}, tentando de novo`);
    await espera(1);
  }
}

async function listaCanais() {
  const resposta = await requisicao("listar", {});
  return resposta.dados.canais || [];
}

async function criaCanal(canal) {
  const resposta = await requisicao("canal", { canal, usuario: NOME });
  if (resposta.status !== "ok") {
    console.log(`[${NOME}] nao criou o canal ${canal}: ${resposta.dados.erro}`);
  }
}

async function publica(canal, mensagem) {
  const resposta = await requisicao("publicar", { canal, usuario: NOME, mensagem });
  if (resposta.status !== "ok") {
    console.log(`[${NOME}] erro ao publicar no canal ${canal}: ${resposta.dados.erro}`);
  }
}

// inscricao nos canais (pub/sub)

const sub = new zmq.Subscriber();
sub.connect(PROXY);
const inscritos = [];

function inscreve(canal) {
  sub.subscribe(canal);
  inscritos.push(canal);
  console.log(`[${NOME}] se inscreveu no canal ${canal} (inscrito em ${JSON.stringify(inscritos)})`);
}

async function recebePublicacoes() {
  // fica rodando junto com o resto, mostrando o que chega nos canais
  for await (const [topico, conteudo] of sub) {
    const recebido = agora();
    const canal = topico.toString();
    // o filtro do zmq e por prefixo, entao confere se e o canal certo
    if (!inscritos.includes(canal)) continue;
    const msg = msgpack.decode(conteudo);
    atualiza(msg.relogio);
    const dados = msg.dados;
    console.log(`[${NOME}] RECEBEU PUBLICACAO: canal=${canal} | usuario=${dados.usuario} | ` +
      `mensagem=${dados.mensagem} | enviada=${horaFormatada(msg.timestamp)} | ` +
      `recebida=${horaFormatada(recebido)} | relogio da mensagem=${msg.relogio} | ` +
      `meu relogio=${relogio}`);
  }
}

async function main() {
  conecta();
  recebePublicacoes();
  await espera(2); // espera o broker e os servidores subirem

  await login();

  while (true) {
    let canais = await listaCanais();

    // se tiver menos que 5 canais o bot cria um novo
    if (canais.length < 5) {
      const opcoes = NOMES_CANAIS.filter((c) => !canais.includes(c));
      await criaCanal(sorteia(opcoes));
      canais = await listaCanais();
    }

    // se estiver inscrito em menos de 3 canais se inscreve em mais um
    if (inscritos.length < 3) {
      const opcoes = canais.filter((c) => !inscritos.includes(c));
      if (opcoes.length > 0) inscreve(sorteia(opcoes));
    }

    if (canais.length === 0) {
      await espera(1);
      continue;
    }

    // escolhe um canal e manda 10 mensagens, uma por segundo
    const canal = sorteia(canais);
    for (let i = 0; i < 10; i++) {
      await publica(canal, sorteia(FRASES));
      await espera(1);
    }
  }
}

main();
