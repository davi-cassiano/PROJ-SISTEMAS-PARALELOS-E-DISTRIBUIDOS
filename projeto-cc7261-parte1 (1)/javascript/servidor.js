const fs = require("fs");
const path = require("path");
const zmq = require("zeromq");
const msgpack = require("@msgpack/msgpack");

const NOME = process.env.NOME || "servidor-js";
const BROKER = process.env.BROKER || "tcp://broker:5556";

// cada servidor tem a sua propria pasta de dados
const PASTA = path.join("dados", NOME);
fs.mkdirSync(PASTA, { recursive: true });

const NOME_USUARIO_VALIDO = /^[A-Za-z0-9_-]{2,30}$/;
const NOME_CANAL_VALIDO = /^[a-z0-9_-]{2,30}$/;

function agora() {
  return Date.now() / 1000;
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

// persistencia (um json por linha)

function salva(arquivo, registro) {
  fs.appendFileSync(path.join(PASTA, arquivo), JSON.stringify(registro) + "\n");
}

function carrega(arquivo) {
  const caminho = path.join(PASTA, arquivo);
  if (!fs.existsSync(caminho)) return [];
  return fs.readFileSync(caminho, "utf-8")
    .split("\n")
    .filter((linha) => linha.trim())
    .map((linha) => JSON.parse(linha));
}

const canais = carrega("canais.jsonl").map((c) => c.canal);
console.log(`[${NOME}] canais carregados do disco: ${JSON.stringify(canais)}`);

// tratamento das requisicoes

function fazLogin(dados) {
  const usuario = dados.usuario || "";
  if (!NOME_USUARIO_VALIDO.test(usuario)) {
    return ["erro", { erro: "nome de usuario invalido (2 a 30 letras, numeros, _ ou -)" }];
  }
  salva("logins.jsonl", { usuario, timestamp: agora(), servidor: NOME });
  return ["ok", { usuario }];
}

function criaCanal(dados) {
  const canal = dados.canal || "";
  if (!NOME_CANAL_VALIDO.test(canal)) {
    return ["erro", { erro: "nome de canal invalido (2 a 30 letras minusculas, numeros, _ ou -)" }];
  }
  if (canais.includes(canal)) {
    return ["erro", { erro: `canal ${canal} ja existe` }];
  }
  canais.push(canal);
  salva("canais.jsonl", { canal, criado_por: dados.usuario || "", timestamp: agora(), servidor: NOME });
  return ["ok", { canal }];
}

function listaCanais(dados) {
  return ["ok", { canais }];
}

const acoes = {
  login: fazLogin,
  canal: criaCanal,
  listar: listaCanais,
};

function trata(msg) {
  const tipo = msg.tipo;
  const acao = acoes[tipo];
  let status, dados;
  if (!acao) {
    [status, dados] = ["erro", { erro: `tipo de mensagem desconhecido: ${tipo}` }];
  } else {
    [status, dados] = acao(msg.dados || {});
  }
  return { tipo, status, timestamp: agora(), dados };
}

async function main() {
  const socket = new zmq.Reply();
  socket.connect(BROKER);
  console.log(`[${NOME}] servidor pronto, conectado no broker ${BROKER}`);

  for await (const [buffer] of socket) {
    const msg = msgpack.decode(buffer);
    mostra("RECEBEU", msg);

    const resposta = trata(msg);

    mostra("ENVIOU", resposta);
    await socket.send(msgpack.encode(resposta));
  }
}

main();
