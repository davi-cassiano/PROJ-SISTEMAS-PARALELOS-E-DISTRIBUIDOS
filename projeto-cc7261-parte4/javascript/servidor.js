const fs = require("fs");
const path = require("path");
const zmq = require("zeromq");
const msgpack = require("@msgpack/msgpack");

const NOME = process.env.NOME || "servidor-js";
const BROKER = process.env.BROKER || "tcp://broker:5556";
const PROXY = process.env.PROXY || "tcp://proxy:5557";          // pra publicar
const PROXY_SUB = process.env.PROXY_SUB || "tcp://proxy:5558";  // pra receber o topico servers
const REFERENCIA = process.env.REFERENCIA || "tcp://referencia:5559";

// porta que cada servidor abre pra falar com os outros servidores (eleicao e relogio)
const PORTA_SERVIDORES = 5560;
const ENDERECO = process.env.ENDERECO || `tcp://*:${PORTA_SERVIDORES}`;

// cada servidor tem a sua propria pasta de dados
const PASTA = path.join("dados", NOME);
fs.mkdirSync(PASTA, { recursive: true });

const NOME_USUARIO_VALIDO = /^[A-Za-z0-9_-]{2,30}$/;
const NOME_CANAL_VALIDO = /^[a-z0-9_-]{2,30}$/;
const TOPICO_SERVIDORES = "servers";

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

function ajustaHora(ajuste) {
  // soma o ajuste (em segundos) no relogio fisico do servidor
  const antes = hora();
  desvio += ajuste;
  console.log(`[${NOME}] relogio fisico ajustado: ${horaFormatada(antes)} -> ${horaFormatada(antes + ajuste)} ` +
    `(ajuste de ${(ajuste * 1000).toFixed(1)} ms)`);
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

// publicacao no proxy

const pub = new zmq.Publisher();
pub.connect(PROXY);

// o zeromq do node so deixa um send por vez, entao os envios ficam numa fila
let filaPub = Promise.resolve();
function publicaNoProxy(topico, msg) {
  filaPub = filaPub.then(() => pub.send([topico, msgpack.encode(msg)]));
  return filaPub;
}

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
  if (canal === TOPICO_SERVIDORES) {
    return ["erro", { erro: `o nome ${canal} e reservado pros servidores` }];
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

async function publica(msg) {
  const canal = msg.dados.canal || "";
  const mensagem = msg.dados.mensagem || "";
  const usuario = msg.dados.usuario || "";
  if (!NOME_CANAL_VALIDO.test(canal) || canal === TOPICO_SERVIDORES) {
    return ["erro", { erro: "nome de canal invalido" }];
  }
  if (!mensagem) return ["erro", { erro: "mensagem vazia" }];

  // publica no canal usando o nome do canal como topico
  const publicacao = {
    tipo: "publicacao", timestamp: hora(), relogio: incrementa(), dados: { canal, usuario, mensagem },
  };
  await publicaNoProxy(canal, publicacao);
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

// pedidos pra referencia e pros outros servidores

async function pede(destino, endereco, tipo, dados, tempo) {
  // usa um socket novo em cada pedido, se nao responder no tempo volta null
  const req = new zmq.Request({ receiveTimeout: tempo, sendTimeout: tempo, linger: 0 });
  try {
    req.connect(endereco);
  } catch (erro) {
    req.close();
    console.log(`[${NOME}] nao consegui conectar em ${destino}`);
    return null;
  }

  const msg = { tipo, timestamp: hora(), relogio: incrementa(), dados };
  mostra(`ENVIOU para ${destino}`, msg);
  try {
    await req.send(msgpack.encode(msg));
    const [buffer] = await req.receive();
    req.close();
    const resposta = msgpack.decode(buffer);
    atualiza(resposta.relogio);
    mostra(`RECEBEU de ${destino}`, resposta);
    return resposta;
  } catch (erro) {
    req.close();
    console.log(`[${NOME}] ${destino} nao respondeu`);
    return null;
  }
}

async function pedeReferencia(tipo, dados, insiste = true) {
  while (true) {
    const resposta = await pede("referencia", REFERENCIA, tipo, dados, 3000);
    if (resposta || !insiste) return resposta;
    await espera(1);
  }
}

function pedeServidor(nome, tipo, dados, tempo = 1000) {
  return pede(nome, `tcp://${nome}:${PORTA_SERVIDORES}`, tipo, dados, tempo);
}

async function listaServidores() {
  const resposta = await pedeReferencia("list", {}, false);
  if (!resposta) return [];
  return resposta.dados.servidores;
}

async function heartbeat() {
  // avisa a referencia que ainda ta funcionando
  await pedeReferencia("heartbeat", { nome: NOME }, false);
}

// eleicao (algoritmo do valentao)

let meuRank = 0;
let coordenador = null;
let esperandoAnuncio = null; // hora que comecou a esperar o anuncio do novo coordenador
let emEleicao = false;

async function eleicao() {
  if (emEleicao) return; // ja tem uma eleicao rodando
  emEleicao = true;
  console.log(`[${NOME}] comecando eleicao (meu rank e ${meuRank})`);
  const maiores = (await listaServidores()).filter((s) => s.rank > meuRank);

  // pede eleicao pra todos com rank maior que o meu
  let alguemRespondeu = false;
  for (const s of maiores) {
    const resposta = await pedeServidor(s.nome, "eleicao", { nome: NOME, rank: meuRank });
    if (resposta && resposta.status === "ok") alguemRespondeu = true;
  }

  if (alguemRespondeu) {
    // algum maior ta vivo, agora ele que continua a eleicao
    console.log(`[${NOME}] servidor com rank maior respondeu, esperando o anuncio do coordenador`);
    esperandoAnuncio = Date.now() / 1000;
  } else {
    // ninguem maior respondeu, entao eu sou o coordenador
    await anunciaCoordenador();
  }
  emEleicao = false;
}

async function anunciaCoordenador() {
  coordenador = NOME;
  esperandoAnuncio = null;
  const msg = { tipo: "coordenador", timestamp: hora(), relogio: incrementa(), dados: { coordenador: NOME } };
  await publicaNoProxy(TOPICO_SERVIDORES, msg);
  mostra(`PUBLICOU no topico ${TOPICO_SERVIDORES}`, msg);
  console.log(`[${NOME}] agora eu sou o coordenador`);
}

function recebeAnuncio(msg) {
  coordenador = msg.dados.coordenador;
  esperandoAnuncio = null;
  console.log(`[${NOME}] coordenador agora e ${coordenador}`);
}

// sincronizacao do relogio (algoritmo de Berkeley)

async function sincronizaRelogio() {
  if (coordenador === NOME) {
    console.log(`[${NOME}] sou o coordenador, nao preciso pedir a hora`);
    return;
  }
  if (!coordenador) {
    await eleicao();
    return;
  }

  const enviado = Date.now() / 1000;
  const resposta = await pedeServidor(coordenador, "relogio", { nome: NOME }, 8000);
  if (!resposta || resposta.status !== "ok") {
    console.log(`[${NOME}] coordenador ${coordenador} nao respondeu, vou pedir eleicao`);
    coordenador = null;
    await eleicao();
    return;
  }

  const dados = resposta.dados;
  if (dados.ajuste !== undefined) {
    ajustaHora(dados.ajuste);
  } else {
    // o coordenador nao conseguiu a minha hora, entao usa a hora dele + metade do tempo de ida e volta
    ajustaHora(dados.hora + (Date.now() / 1000 - enviado) / 2 - hora());
  }
}

async function berkeley(msg) {
  // sou o coordenador: pergunto a hora de todos os servidores, calculo a diferenca
  // de cada um pro meu relogio e tiro a media dessas diferencas
  const quemPediu = msg.dados.nome;
  const diferencas = { [NOME]: 0 };

  const nomes = [quemPediu, ...(await listaServidores()).map((s) => s.nome)];
  for (const nome of nomes) {
    if (nome in diferencas) continue;
    const enviado = Date.now() / 1000;
    const resposta = await pedeServidor(nome, "hora", { nome: NOME });
    if (resposta) {
      // hora do outro servidor + metade do tempo de ida e volta - a minha hora agora
      diferencas[nome] = resposta.dados.hora + (Date.now() / 1000 - enviado) / 2 - hora();
    }
  }

  const valores = Object.values(diferencas);
  const media = valores.reduce((a, b) => a + b, 0) / valores.length;
  const emMs = {};
  for (const [n, d] of Object.entries(diferencas)) emMs[n] = Number((d * 1000).toFixed(1));
  console.log(`[${NOME}] berkeley: diferencas pro meu relogio (ms) = ${JSON.stringify(emMs)} ` +
    `-> media = ${(media * 1000).toFixed(1)} ms`);

  // cada um tem que somar (media - diferenca dele) no relogio
  ajustaHora(media);
  const dados = { hora: hora() };
  if (quemPediu in diferencas) dados.ajuste = media - diferencas[quemPediu];
  return dados;
}

// atende os pedidos dos outros servidores

async function atendeServidores() {
  const rep = new zmq.Reply();
  await rep.bind(ENDERECO);
  for await (const [buffer] of rep) {
    const msg = msgpack.decode(buffer);
    atualiza(msg.relogio || 0);
    mostra("RECEBEU de servidor", msg);
    const tipo = msg.tipo;
    let status = "ok";
    let dados = {};
    let pedirEleicao = false;

    if (tipo === "eleicao") {
      // responde OK e comeca uma eleicao tambem
      pedirEleicao = true;
    } else if (tipo === "hora") {
      dados = { hora: hora() };
    } else if (tipo === "relogio") {
      if (coordenador === NOME) dados = await berkeley(msg);
      else [status, dados] = ["erro", { erro: "nao sou o coordenador" }];
    } else {
      [status, dados] = ["erro", { erro: `tipo de mensagem desconhecido: ${tipo}` }];
    }

    const resposta = { tipo, status, timestamp: hora(), relogio: incrementa(), dados };
    mostra("ENVIOU para servidor", resposta);
    await rep.send(msgpack.encode(resposta));

    if (pedirEleicao) eleicao();
  }
}

// recebe o anuncio do coordenador no topico servers

async function recebeTopicoServidores() {
  const sub = new zmq.Subscriber();
  sub.connect(PROXY_SUB);
  sub.subscribe(TOPICO_SERVIDORES);
  for await (const [topico, conteudo] of sub) {
    if (topico.toString() !== TOPICO_SERVIDORES) continue;
    const msg = msgpack.decode(conteudo);
    atualiza(msg.relogio);
    mostra(`RECEBEU do topico ${TOPICO_SERVIDORES}`, msg);
    recebeAnuncio(msg);
  }
}

// inicio

async function main() {
  const socket = new zmq.Reply();
  socket.connect(BROKER);

  let resposta = await pedeReferencia("rank", { nome: NOME });
  meuRank = resposta.dados.rank;
  console.log(`[${NOME}] meu rank e ${meuRank}`);

  resposta = await pedeReferencia("list", {});
  console.log(`[${NOME}] servidores disponiveis: ${JSON.stringify(resposta.dados.servidores)}`);

  atendeServidores();
  recebeTopicoServidores();
  await espera(1); // da um tempo pro sub conectar no proxy

  // quando entra ja faz uma eleicao pra descobrir quem e o coordenador
  await eleicao();

  // se ninguem anunciou o coordenador depois de um tempo, faz a eleicao de novo
  setInterval(() => {
    if (esperandoAnuncio !== null && Date.now() / 1000 - esperandoAnuncio > 10) {
      console.log(`[${NOME}] ninguem anunciou o coordenador, fazendo eleicao de novo`);
      eleicao();
    }
  }, 1000);

  console.log(`[${NOME}] servidor pronto, conectado no broker ${BROKER} e no proxy ${PROXY}`);

  let mensagensRecebidas = 0;

  for await (const [buffer] of socket) {
    const msg = msgpack.decode(buffer);
    atualiza(msg.relogio || 0);
    mostra("RECEBEU", msg);

    const resposta = await trata(msg);

    mostra("ENVIOU", resposta);
    await socket.send(msgpack.encode(resposta));

    mensagensRecebidas += 1;
    // a cada 10 mensagens de clientes manda o heartbeat
    if (mensagensRecebidas % 10 === 0) await heartbeat();
    // a cada 15 mensagens sincroniza o relogio com o coordenador
    if (mensagensRecebidas % 15 === 0) await sincronizaRelogio();
  }
}

main();
