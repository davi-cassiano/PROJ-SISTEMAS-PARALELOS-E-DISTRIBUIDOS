const fs = require("fs");
const path = require("path");
const zmq = require("zeromq");
const msgpack = require("@msgpack/msgpack");

const NOME = process.env.NOME || "servidor-js";
const BROKER = process.env.BROKER || "tcp://broker:5556";
const PROXY = process.env.PROXY || "tcp://proxy:5557";
const REFERENCIA = process.env.REFERENCIA || "tcp://referencia:5559";

// cada servidor tem a sua propria pasta de dados
const PASTA = path.join("dados", NOME);
fs.mkdirSync(PASTA, { recursive: true });

const NOME_USUARIO_VALIDO = /^[A-Za-z0-9_-]{2,30}$/;
const NOME_CANAL_VALIDO = /^[a-z0-9_-]{2,30}$/;

function espera(segundos) {
  return new Promise((resolve) => setTimeout(resolve, segundos * 1000));
}

// relogios

let relogio = 0;  // relogio logico
let desvio = 0;   // diferenca entre o relogio fisico do servidor e a hora certa

function incrementa() {
  // incrementa antes de enviar uma mensagem
  relogio += 1;
  return relogio;
}

function atualiza(recebido) {
  // quando recebe fica com o maior valor
  relogio = Math.max(relogio, recebido);
}

function hora() {
  // hora do servidor ja com o ajuste
  return Date.now() / 1000 + desvio;
}

function ajustaHora(horaCerta) {
  const antes = hora();
  desvio = horaCerta - Date.now() / 1000;
  console.log(`[${NOME}] relogio fisico ajustado: ${horaFormatada(antes)} -> ${horaFormatada(horaCerta)} ` +
    `(diferenca de ${((horaCerta - antes) * 1000).toFixed(1)} ms)`);
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

// tratamento das requisicoes dos clientes

function fazLogin(msg) {
  const usuario = msg.dados.usuario || "";
  if (!NOME_USUARIO_VALIDO.test(usuario)) {
    return ["erro", { erro: "nome de usuario invalido (2 a 30 letras, numeros, _ ou -)" }];
  }
  salva("logins.jsonl", { usuario, timestamp: hora(), relogio: msg.relogio, servidor: NOME });
  return ["ok", { usuario }];
}

function criaCanal(msg) {
  const canal = msg.dados.canal || "";
  if (!NOME_CANAL_VALIDO.test(canal)) {
    return ["erro", { erro: "nome de canal invalido (2 a 30 letras minusculas, numeros, _ ou -)" }];
  }
  if (canais.includes(canal)) {
    return ["erro", { erro: `canal ${canal} ja existe` }];
  }
  canais.push(canal);
  salva("canais.jsonl", {
    canal, criado_por: msg.dados.usuario || "", timestamp: hora(), relogio: msg.relogio, servidor: NOME,
  });
  return ["ok", { canal }];
}

function listaCanais(msg) {
  return ["ok", { canais }];
}

// socket pra publicar nos canais (passa pelo proxy)
const pub = new zmq.Publisher();
pub.connect(PROXY);

async function publica(msg) {
  const canal = msg.dados.canal || "";
  const mensagem = msg.dados.mensagem || "";
  const usuario = msg.dados.usuario || "";
  if (!NOME_CANAL_VALIDO.test(canal)) return ["erro", { erro: "nome de canal invalido" }];
  if (!mensagem) return ["erro", { erro: "mensagem vazia" }];

  // publica no canal usando o nome do canal como topico
  const publicacao = {
    tipo: "publicacao", timestamp: hora(), relogio: incrementa(), dados: { canal, usuario, mensagem },
  };
  await pub.send([canal, msgpack.encode(publicacao)]);
  mostra(`PUBLICOU no canal ${canal}`, publicacao);

  salva("mensagens.jsonl", {
    canal, usuario, mensagem, timestamp: publicacao.timestamp, relogio: msg.relogio, servidor: NOME,
  });
  return ["ok", { canal }];
}

const acoes = {
  login: fazLogin,
  canal: criaCanal,
  listar: listaCanais,
  publicar: publica,
};

async function trata(msg) {
  const tipo = msg.tipo;
  const acao = acoes[tipo];
  let status, dados;
  if (!acao) {
    [status, dados] = ["erro", { erro: `tipo de mensagem desconhecido: ${tipo}` }];
  } else {
    [status, dados] = await acao(msg);
  }
  return { tipo, status, timestamp: hora(), relogio: incrementa(), dados };
}

// comunicacao com o servico de referencia

async function pedeReferencia(tipo, dados, insiste = true) {
  // usa um socket novo em cada pedido pra nao travar se a referencia nao responder
  while (true) {
    const req = new zmq.Request({ receiveTimeout: 3000, linger: 0 });
    req.connect(REFERENCIA);

    const msg = { tipo, timestamp: hora(), relogio: incrementa(), dados };
    mostra("ENVIOU para referencia", msg);
    await req.send(msgpack.encode(msg));
    try {
      const [buffer] = await req.receive();
      req.close();
      const resposta = msgpack.decode(buffer);
      atualiza(resposta.relogio);
      mostra("RECEBEU da referencia", resposta);
      return resposta;
    } catch (erro) {
      req.close();
      console.log(`[${NOME}] referencia nao respondeu`);
      if (!insiste) return null;
      await espera(1);
    }
  }
}

async function heartbeat() {
  // avisa a referencia que ainda ta funcionando e aproveita pra pegar a hora certa
  const enviado = Date.now() / 1000;
  const resposta = await pedeReferencia("heartbeat", { nome: NOME }, false);
  if (!resposta) return;
  const recebido = Date.now() / 1000;
  // soma metade do tempo de ida e volta da mensagem
  ajustaHora(resposta.dados.hora + (recebido - enviado) / 2);
}

// inicio

async function main() {
  const socket = new zmq.Reply();
  socket.connect(BROKER);

  let resposta = await pedeReferencia("rank", { nome: NOME });
  const meuRank = resposta.dados.rank;
  console.log(`[${NOME}] meu rank e ${meuRank}`);

  resposta = await pedeReferencia("list", {});
  console.log(`[${NOME}] servidores disponiveis: ${JSON.stringify(resposta.dados.servidores)}`);

  console.log(`[${NOME}] servidor pronto, conectado no broker ${BROKER} e no proxy ${PROXY}`);

  let mensagensRecebidas = 0;

  for await (const [buffer] of socket) {
    const msg = msgpack.decode(buffer);
    atualiza(msg.relogio || 0);
    mostra("RECEBEU", msg);

    const resposta = await trata(msg);

    mostra("ENVIOU", resposta);
    await socket.send(msgpack.encode(resposta));

    // a cada 10 mensagens de clientes manda o heartbeat
    mensagensRecebidas += 1;
    if (mensagensRecebidas % 10 === 0) await heartbeat();
  }
}

main();
