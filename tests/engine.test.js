/* Testes do motor do Mini-DFS. Rode com:  node tests/engine.test.js */
'use strict';
const assert = require('assert');
const { Cluster, DFSError } = require('../js/dfs.js');

let passed = 0;
let failed = 0;
function test(name, fn) {
  try {
    fn();
    passed++;
    console.log('  ok   ' + name);
  } catch (e) {
    failed++;
    console.log('  FAIL ' + name + '\n       ' + (e && e.stack ? e.stack.split('\n').slice(0, 3).join('\n       ') : e));
  }
}
function throwsCode(fn, code) {
  try {
    fn();
  } catch (e) {
    assert.ok(e instanceof DFSError, 'esperava DFSError, veio ' + e);
    assert.strictEqual(e.code, code, `código esperado ${code}, veio ${e.code} (${e.detail})`);
    return;
  }
  assert.fail('deveria ter lançado ' + code);
}
const text = (r) => new TextDecoder().decode(r.bytes);
const fresh = (cfg) => new Cluster(Object.assign({ blockSize: 8, replication: 3, nodeCapacity: 32, nodes: 4 }, cfg));

console.log('\nNamespace');
test('mkdir, mkdir -p e erros', () => {
  const c = fresh();
  c.mkdir('/a');
  c.mkdir('/a/b/c', { parents: true });
  throwsCode(() => c.mkdir('/a'), 'EEXIST');
  throwsCode(() => c.mkdir('/x/y'), 'ENOENT');
  assert.strictEqual(c.resolve('/a/b/c').type, 'dir');
});
test('nomes inválidos são recusados', () => {
  const c = fresh();
  throwsCode(() => c.mkdir('/a\u0001b'), 'EINVAL');
  throwsCode(() => c.write('/__proto__', 'x'), 'EINVAL');
});
test('cd e caminhos relativos / ..', () => {
  const c = fresh();
  c.mkdir('/a/b', { parents: true });
  c.cd('/a/b');
  c.write('f.txt', 'oi');
  assert.strictEqual(text(c.read('/a/b/f.txt')), 'oi');
  assert.strictEqual(text(c.read('../b/./f.txt')), 'oi');
});

console.log('\nEscrever / ler');
test('write divide em blocos e replica 3x', () => {
  const c = fresh();
  c.write('/f.txt', '0123456789ABCDEFGHIJ'); // 20 B -> 3 blocos de até 8 B
  const f = c.resolve('/f.txt');
  assert.strictEqual(f.blocks.length, 3);
  for (const id of f.blocks) assert.strictEqual(c.blocks.get(id).replicas.size, 3);
  assert.strictEqual(text(c.read('/f.txt')), '0123456789ABCDEFGHIJ');
});
test('write em arquivo existente falha (EEXIST)', () => {
  const c = fresh();
  c.write('/f', 'a');
  throwsCode(() => c.write('/f', 'b'), 'EEXIST');
});
test('arquivo vazio não tem blocos', () => {
  const c = fresh();
  c.write('/vazio', '');
  assert.strictEqual(c.resolve('/vazio').blocks.length, 0);
  assert.strictEqual(text(c.read('/vazio')), '');
});
test('UTF-8 com acentos atravessa fronteira de bloco', () => {
  const c = fresh({ blockSize: 5 });
  const s = 'ação é coração, órgão não é pão';
  c.write('/u.txt', s);
  assert.strictEqual(text(c.read('/u.txt')), s);
});
test('ler arquivo/diretório inexistente ou diretório', () => {
  const c = fresh();
  c.mkdir('/d');
  throwsCode(() => c.read('/nada'), 'ENOENT');
  throwsCode(() => c.read('/d'), 'EISDIR');
});
test('réplicas ficam em DataNodes distintos', () => {
  const c = fresh();
  c.write('/f', 'x'.repeat(64));
  for (const b of c.blocks.values()) assert.strictEqual(new Set(b.replicas).size, b.replicas.size);
});
test('política de racks: 2ª réplica vai para outro rack', () => {
  const c = fresh();
  c.write('/f', 'x'.repeat(8));
  const b = [...c.blocks.values()][0];
  const racks = new Set([...b.replicas].map((id) => c._node(id).rack));
  assert.ok(racks.size >= 2);
});

console.log('\nSobrescrever / anexar / substituir');
test('overwrite troca conteúdo, sobe a versão e libera blocos antigos', () => {
  const c = fresh();
  c.write('/f', 'AAAAAAAAAAAAAAAA'); // 2 blocos
  const oldIds = [...c.resolve('/f').blocks];
  c.overwrite('/f', 'novo');
  const f = c.resolve('/f');
  assert.strictEqual(f.version, 2);
  assert.strictEqual(text(c.read('/f')), 'novo');
  for (const id of oldIds) assert.ok(!c.blocks.has(id), 'bloco antigo deveria ter sido liberado');
  assert.strictEqual(c.blocks.size, 1);
});
test('overwrite que não cabe preserva o arquivo antigo (atomicidade)', () => {
  const c = fresh({ nodes: 3, nodeCapacity: 4 });
  c.write('/f', 'x'.repeat(16)); // 2 blocos x3 réplicas = 6 de 12 slots
  throwsCode(() => c.overwrite('/f', 'y'.repeat(80)), 'ENOSPC'); // 10 blocos não cabem
  assert.strictEqual(text(c.read('/f')), 'x'.repeat(16));
  assert.strictEqual(c.resolve('/f').version, 1);
  assert.strictEqual(c.blocks.size, 2);
});
test('write sem espaço não deixa lixo', () => {
  const c = fresh({ nodes: 3, nodeCapacity: 4 });
  throwsCode(() => c.write('/g', 'z'.repeat(200)), 'ENOSPC');
  assert.strictEqual(c.blocks.size, 0);
  throwsCode(() => c.resolve('/g'), 'ENOENT');
  for (const n of c.nodes) assert.strictEqual(n.store.size, 0);
});
test('append acrescenta ao final', () => {
  const c = fresh();
  c.write('/f', 'Olá, ');
  c.append('/f', 'mundo!');
  assert.strictEqual(text(c.read('/f')), 'Olá, mundo!');
  assert.strictEqual(c.resolve('/f').version, 2);
});
test('replace copia o conteúdo da origem para o destino e preserva a origem', () => {
  const c = fresh();
  c.write('/a', 'conteudo A');
  c.write('/b', 'conteudo B');
  c.replace('/a', '/b');
  assert.strictEqual(text(c.read('/a')), 'conteudo B');
  assert.strictEqual(text(c.read('/b')), 'conteudo B');
  assert.strictEqual(c.resolve('/a').version, 2);
  throwsCode(() => c.replace('/a', '/a'), 'EINVAL');
});

console.log('\nRenomear / mover / copiar');
test('rename não move blocos', () => {
  const c = fresh();
  c.write('/f', 'abcdefghijkl');
  const before = JSON.stringify(c.blockLocations(c.resolve('/f')));
  c.rename('/f', 'g');
  assert.strictEqual(JSON.stringify(c.blockLocations(c.resolve('/g'))), before);
  throwsCode(() => c.resolve('/f'), 'ENOENT');
  c.write('/h', 'x');
  throwsCode(() => c.rename('/g', 'h'), 'EEXIST');
  throwsCode(() => c.rename('/g', 'a/b'), 'EINVAL');
});
test('mv para pasta, para novo nome e com conflito', () => {
  const c = fresh();
  c.mkdir('/d');
  c.write('/f', 'abc');
  c.mv('/f', '/d');
  assert.strictEqual(text(c.read('/d/f')), 'abc');
  c.mv('/d/f', '/novo.txt');
  assert.strictEqual(text(c.read('/novo.txt')), 'abc');
  c.write('/d/novo.txt', 'outro');
  throwsCode(() => c.mv('/novo.txt', '/d'), 'EEXIST');
  c.mv('/novo.txt', '/d', { force: true });
  assert.strictEqual(text(c.read('/d/novo.txt')), 'abc');
  assert.strictEqual(c.blocks.size, 1); // o antigo foi liberado
});
test('mv de pasta para dentro de si mesma é recusado', () => {
  const c = fresh();
  c.mkdir('/a/b', { parents: true });
  throwsCode(() => c.mv('/a', '/a/b'), 'EINVAL');
});
test('mv de pasta mantém o conteúdo legível', () => {
  const c = fresh();
  c.mkdir('/a');
  c.write('/a/f', 'dados');
  c.mkdir('/z');
  c.mv('/a', '/z');
  assert.strictEqual(text(c.read('/z/a/f')), 'dados');
});
test('cp cria blocos novos e independentes', () => {
  const c = fresh();
  c.write('/f', 'abcdefghijkl');
  c.cp('/f', '/g');
  assert.strictEqual(text(c.read('/g')), 'abcdefghijkl');
  assert.strictEqual(c.blocks.size, 4);
  c.overwrite('/f', 'mudou');
  assert.strictEqual(text(c.read('/g')), 'abcdefghijkl');
});

console.log('\nApagar / lixeira');
test('rm vai para a lixeira e restore devolve', () => {
  const c = fresh();
  c.mkdir('/d');
  c.write('/d/f', 'segredo');
  c.rm('/d/f');
  throwsCode(() => c.resolve('/d/f'), 'ENOENT');
  assert.strictEqual(c.blocks.size, 1, 'blocos continuam armazenados');
  assert.strictEqual(text(c.read('/.lixeira/f')), 'segredo');
  c.restore('/.lixeira/f');
  assert.strictEqual(text(c.read('/d/f')), 'segredo');
});
test('restore recria a pasta de origem se sumiu e detecta conflito', () => {
  const c = fresh();
  c.mkdir('/d');
  c.write('/d/f', 'x');
  c.rm('/d/f');
  c.rm('/d');
  c.restore('/.lixeira/f');
  assert.strictEqual(text(c.read('/d/f')), 'x');
  c.rm('/d/f');
  c.write('/d/f', 'novo');
  throwsCode(() => c.restore('/.lixeira/f'), 'EEXIST');
});
test('rm -f e esvaziar a lixeira liberam blocos', () => {
  const c = fresh();
  c.write('/a', 'x'.repeat(16));
  c.write('/b', 'y'.repeat(16));
  c.rm('/a', { permanent: true });
  assert.strictEqual(c.blocks.size, 2);
  c.rm('/b');
  c.emptyTrash();
  assert.strictEqual(c.blocks.size, 0);
  for (const n of c.nodes) assert.strictEqual(n.store.size, 0);
});
test('rm de pasta não vazia exige -r; raiz e lixeira são protegidas', () => {
  const c = fresh();
  c.mkdir('/d');
  c.write('/d/f', 'x');
  throwsCode(() => c.rm('/d'), 'ENOTEMPTY');
  c.rm('/d', { recursive: true });
  throwsCode(() => c.rm('/'), 'EPERM');
  throwsCode(() => c.rm('/.lixeira'), 'EPERM');
});
test('lixeira recusa novos arquivos e alterações', () => {
  const c = fresh();
  c.write('/f', 'x');
  c.rm('/f');
  throwsCode(() => c.write('/.lixeira/novo', 'y'), 'EPERM');
  throwsCode(() => c.overwrite('/.lixeira/f', 'y'), 'EPERM');
  throwsCode(() => c.rename('/.lixeira/f', 'g'), 'EPERM');
});
test('nomes repetidos na lixeira não colidem', () => {
  const c = fresh();
  c.write('/f', '1');
  c.rm('/f');
  c.write('/f', '2');
  c.rm('/f');
  const names = [...c._inode(c.trashId).children.keys()].sort();
  assert.deepStrictEqual(names, ['f', 'f.1']);
});

console.log('\nFalhas e recuperação');
test('leitura continua com um DataNode caído (failover)', () => {
  const c = fresh();
  c.write('/f', 'x'.repeat(40));
  c.killNode('dn1');
  assert.strictEqual(text(c.read('/f')), 'x'.repeat(40));
});
test('declareDead + recover re-replica nos nós restantes', () => {
  const c = fresh();
  c.write('/f', 'x'.repeat(64));
  c.killNode('dn2');
  c.declareDead('dn2');
  assert.ok(c.health().underReplicated > 0);
  const r = c.recover();
  assert.ok(r.copied > 0);
  const h = c.health();
  assert.strictEqual(h.underReplicated, 0);
  assert.strictEqual(h.status, 'HEALTHY');
});
test('reviver nó morto gera excesso que é removido', () => {
  const c = fresh();
  c.write('/f', 'x'.repeat(64));
  c.killNode('dn2');
  c.declareDead('dn2');
  c.recover();
  c.reviveNode('dn2');
  const h = c.health();
  assert.strictEqual(h.overReplicated, 0);
  assert.strictEqual(h.status, 'HEALTHY');
  for (const b of c.blocks.values()) assert.strictEqual(b.replicas.size, 3);
});
test('arquivo apagado enquanto o nó estava offline: block report descarta órfãos', () => {
  const c = fresh();
  c.write('/f', 'x'.repeat(24));
  c.killNode('dn1');
  c.rm('/f', { permanent: true });
  c.reviveNode('dn1');
  assert.strictEqual(c._node('dn1').store.size, 0);
});
test('replicação 1 + nó caído = EIO; ao reviver volta a ler', () => {
  const c = fresh();
  c.write('/f', 'abc');
  c.setReplication('/f', 1);
  const id = c.resolve('/f').blocks[0];
  const holder = [...c.blocks.get(id).replicas][0];
  c.killNode(holder);
  throwsCode(() => c.read('/f'), 'EIO');
  assert.strictEqual(c.health().status, 'CORRUPT');
  c.reviveNode(holder);
  assert.strictEqual(text(c.read('/f')), 'abc');
});
test('setrep aumenta e diminui réplicas', () => {
  const c = fresh();
  c.write('/f', 'x'.repeat(16));
  c.setReplication('/f', 4);
  for (const b of c.blocks.values()) assert.strictEqual(b.replicas.size, 4);
  c.setReplication('/f', 2);
  for (const b of c.blocks.values()) assert.strictEqual(b.replicas.size, 2);
  throwsCode(() => c.setReplication('/f', 9), 'EINVAL');
});
test('write com menos nós que o fator fica sub-replicado e se cura ao adicionar nó', () => {
  const c = fresh({ nodes: 2 });
  c.write('/f', 'abcdefgh');
  assert.strictEqual(c.health().status, 'DEGRADED');
  c.addNode();
  c.recover();
  assert.strictEqual(c.health().status, 'HEALTHY');
});

console.log('\nIntegridade (checksum)');
test('leitura detecta réplica corrompida, usa outra e re-replica', () => {
  const c = fresh();
  c.write('/f', 'integridade!');
  const id = c.resolve('/f').blocks[0];
  const victim = [...c.blocks.get(id).replicas][0];
  c.corrupt(id, victim);
  assert.ok(c.health().corruptReplicas >= 1);
  assert.strictEqual(text(c.read('/f')), 'integridade!');
  assert.strictEqual(c.health().status, 'HEALTHY');
  assert.strictEqual(c.stats.corruptDetected, 1);
});
test('fsck detecta e fsck --repair conserta', () => {
  const c = fresh();
  c.write('/f', 'x'.repeat(24));
  const r = c.corruptFile('/f');
  assert.ok(r.blockId && r.nodeId);
  const h1 = c.fsck();
  assert.strictEqual(h1.status, 'DEGRADED');
  assert.ok(h1.found >= 1);
  const h2 = c.fsck({ repair: true });
  assert.strictEqual(h2.status, 'HEALTHY');
  assert.strictEqual(h2.corruptReplicas, 0);
});
test('todas as réplicas corrompidas: leitura falha (EIO) e fsck acusa CORRUPT', () => {
  const c = fresh({ replication: 2 });
  c.write('/f', 'abc');
  const id = c.resolve('/f').blocks[0];
  for (const nid of [...c.blocks.get(id).replicas]) c.corrupt(id, nid);
  throwsCode(() => c.read('/f'), 'EIO');
  assert.strictEqual(c.fsck({ repair: true }).status, 'CORRUPT');
});

console.log('\nCluster');
test('rebalance equilibra após adicionar nó', () => {
  const c = fresh();
  c.write('/f', 'x'.repeat(8 * 16));
  const n = c.addNode();
  assert.strictEqual(n.store.size, 0);
  c.rebalance();
  const sizes = c.nodes.map((x) => x.store.size);
  assert.ok(Math.max(...sizes) - Math.min(...sizes) <= 1, 'desbalanceado: ' + sizes);
  assert.strictEqual(text(c.read('/f')), 'x'.repeat(8 * 16));
  assert.strictEqual(c.health().status, 'HEALTHY');
});
test('limite de 8 DataNodes', () => {
  const c = fresh({ nodes: 8 });
  throwsCode(() => c.addNode(), 'EINVAL');
});
test('serialize/deserialize preserva tudo', () => {
  const c = fresh();
  c.mkdir('/d');
  c.write('/d/f', 'persistir em JSON!');
  c.killNode('dn3');
  c.rm('/d/f');
  const copy = Cluster.deserialize(JSON.parse(JSON.stringify(c.serialize())));
  assert.strictEqual(text(copy.read('/.lixeira/f')), 'persistir em JSON!');
  assert.strictEqual(copy._node('dn3').online, false);
  assert.strictEqual(copy.blocks.size, c.blocks.size);
  assert.deepStrictEqual(copy.health(), c.health());
});
test('setCapacity / setBlockSize validam entrada', () => {
  const c = fresh();
  throwsCode(() => c.setCapacity(1), 'EINVAL');
  throwsCode(() => c.setBlockSize(0), 'EINVAL');
  c.setBlockSize(16);
  c.write('/f', 'x'.repeat(32));
  assert.strictEqual(c.resolve('/f').blocks.length, 2);
});

console.log(`\n${passed} passaram, ${failed} falharam`);
process.exit(failed ? 1 : 0);
