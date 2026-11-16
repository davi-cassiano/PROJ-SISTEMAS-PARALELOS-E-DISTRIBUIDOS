const zmq = require("zeromq");
const msgpack = require("@msgpack/msgpack");

const NOME = process.env.NOME || "bot-js";
const BROKER = process.env.BROKER || "tcp://broker:5555";

// nomes que o bot usa quando precisa criar um canal
const NOMES_CANAIS = ["geral", "games", "musica", "filmes", "esportes",
  "noticias", "tecnologia", "memes", "estudos", "aleatorio"];

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
    const msg = { tipo, timestamp: agora(), dados };
    mostra("ENVIOU", msg);
    await socket.send(msgpack.encode(msg));
    try {
      const [buffer] = await socket.receive();
      const resposta = msgpack.decode(buffer);
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

async function main() {
  conecta();
  await espera(2); // espera o broker e os servidores subirem

  await login();

  while (true) {
    const canais = await listaCanais();
    // se tiver menos que 5 canais o bot cria um novo
    if (canais.length < 5) {
      const opcoes = NOMES_CANAIS.filter((c) => !canais.includes(c));
      await criaCanal(sorteia(opcoes));
    }
    await espera(3);
  }
}

main();
