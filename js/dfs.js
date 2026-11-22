/*!
 * Mini-DFS — motor de um sistema de arquivos distribuído (didático)
 *
 * Arquitetura (inspirada em HDFS/GFS, com vocabulário que também vale para o DFS da Microsoft):
 *   - NameNode  : guarda SÓ metadados (árvore de diretórios, inodes, mapa de blocos).
 *   - DataNodes : guardam os BLOCOS de dados (réplicas) e verificam checksum (CRC32).
 *   - Cliente   : pede ao NameNode onde gravar/ler e fala direto com os DataNodes.
 *
 * Este arquivo não depende de DOM: roda no navegador e no Node (veja tests/engine.test.js).
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.MiniDFS = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  const ENC = new TextEncoder();
  const DEC = new TextDecoder('utf-8');

  /* ------------------------------------------------------------------ erros */
  class DFSError extends Error {
    constructor(code, detail) {
      super(code + ': ' + detail);
      this.name = 'DFSError';
      this.code = code;
      this.detail = detail;
    }
  }

  /* ------------------------------------------------------------------ utilidades */
  const CRC_TABLE = (() => {
    const t = new Uint32Array(256);
    for (let n = 0; n < 256; n++) {
      let c = n;
      for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
      t[n] = c >>> 0;
    }
    return t;
  })();

  function crc32(bytes) {
    let c = 0xffffffff;
    for (let i = 0; i < bytes.length; i++) c = CRC_TABLE[(c ^ bytes[i]) & 0xff] ^ (c >>> 8);
    return (c ^ 0xffffffff) >>> 0;
  }

  const hex = (n) => (n >>> 0).toString(16).padStart(8, '0');

  function toBytes(data) {
    if (data instanceof Uint8Array) return data;
    if (typeof data === 'string') return ENC.encode(data);
    throw new DFSError('EINVAL', 'conteúdo deve ser texto ou bytes');
  }

  function concat(arrays) {
    let total = 0;
    for (const a of arrays) total += a.length;
    const out = new Uint8Array(total);
    let off = 0;
    for (const a of arrays) {
      out.set(a, off);
      off += a.length;
    }
    return out;
  }

  function bytesToB64(bytes) {
    if (typeof Buffer !== 'undefined') return Buffer.from(bytes).toString('base64');
    let s = '';
    for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
    return btoa(s);
  }

  function b64ToBytes(b64) {
    if (typeof Buffer !== 'undefined') return new Uint8Array(Buffer.from(b64, 'base64'));
    const s = atob(b64);
    const out = new Uint8Array(s.length);
    for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i);
    return out;
  }

  /** Quebra um caminho em segmentos, resolvendo ".", ".." e caminhos relativos ao cwd. */
  function parsePath(path, cwd) {
    if (typeof path !== 'string' || !path.trim()) throw new DFSError('EINVAL', 'caminho vazio');
    path = path.trim();
    const abs = path.startsWith('/') ? path : (cwd || '/').replace(/\/+$/, '') + '/' + path;
    const out = [];
    for (const seg of abs.split('/')) {
      if (!seg || seg === '.') continue;
      if (seg === '..') {
        out.pop();
        continue;
      }
      out.push(seg);
    }
    return out;
  }

  const toPath = (parts) => '/' + parts.join('/');

  function validName(name) {
    if (typeof name !== 'string' || !name.trim()) throw new DFSError('EINVAL', 'nome vazio');
    if (name.includes('/')) throw new DFSError('EINVAL', 'o nome não pode conter "/"');
    if (name === '.' || name === '..') throw new DFSError('EINVAL', `nome inválido: "${name}"`);
    if (name === '__proto__') throw new DFSError('EINVAL', 'nome reservado');
    if (name.length > 120) throw new DFSError('EINVAL', 'nome muito longo (máx. 120)');
    // eslint-disable-next-line no-control-regex
    if (/[\u0000-\u001f]/.test(name)) throw new DFSError('EINVAL', 'nome contém caracteres de controle');
  }

  const MAX_NODES = 8;
  const TRASH = '.lixeira';
  const DEFAULTS = { blockSize: 32, replication: 3, nodeCapacity: 32, nodes: 4 };

  /* ------------------------------------------------------------------ cluster */
  class Cluster {
    constructor(opts) {
      this.config = Object.assign({}, DEFAULTS, opts || {});
      this.listeners = new Set();
      this.seq = 0;
      this.reset();
    }

    subscribe(fn) {
      this.listeners.add(fn);
      return () => this.listeners.delete(fn);
    }

    _emit(level, actor, msg) {
      const ev = { id: ++this.seq, time: Date.now(), level, actor, msg };
      this.events.push(ev);
      if (this.events.length > 600) this.events.splice(0, this.events.length - 600);
      this.listeners.forEach((fn) => {
        try {
          fn(ev);
        } catch (e) {
          /* listener não pode derrubar o motor */
        }
      });
      return ev;
    }

    reset() {
      this.events = [];
      this.nextInode = 1;
      this.nextBlock = 1;
      this.nextNode = 1;
      this.inodes = new Map();
      this.blocks = new Map(); // block map do NameNode: id -> {id,fileId,index,size,crc,replicas:Set}
      this.nodes = [];
      this.cwd = '/';
      this.stats = { corruptDetected: 0 };
      const root = this._newInode('dir', '', null);
      this.rootId = root.id;
      const trash = this._newInode('dir', TRASH, root.id);
      root.children.set(TRASH, trash.id);
      this.trashId = trash.id;
      for (let i = 0; i < this.config.nodes; i++) this._addNode(true);
      this._emit(
        'info',
        'sistema',
        `cluster iniciado: ${this.nodes.length} DataNodes, bloco de ${this.config.blockSize} B, replicação ${this.config.replication}`
      );
    }

    /* ---------------------------------------------------------------- inodes */
    _newInode(type, name, parentId) {
      const now = Date.now();
      const ino = { id: this.nextInode++, type, name, parent: parentId, ctime: now, mtime: now };
      if (type === 'dir') ino.children = new Map();
      else {
        ino.blocks = [];
        ino.size = 0;
        ino.version = 1;
        ino.replication = this.config.replication;
      }
      this.inodes.set(ino.id, ino);
      return ino;
    }

    _inode(id) {
      return this.inodes.get(id);
    }

    _node(id) {
      return this.nodes.find((n) => n.id === id);
    }

    pathOf(ino) {
      const parts = [];
      for (let c = ino; c && c.parent !== null; c = this.inodes.get(c.parent)) parts.unshift(c.name);
      return '/' + parts.join('/');
    }

    _parts(path) {
      return parsePath(path, this.cwd);
    }

    _lookup(parts) {
      let cur = this._inode(this.rootId);
      for (let i = 0; i < parts.length; i++) {
        if (cur.type !== 'dir') throw new DFSError('ENOTDIR', toPath(parts.slice(0, i)) + ' não é um diretório');
        const id = cur.children.get(parts[i]);
        if (id === undefined) throw new DFSError('ENOENT', toPath(parts.slice(0, i + 1)) + ': não existe');
        cur = this._inode(id);
      }
      return cur;
    }

    _tryLookup(parts) {
      try {
        return this._lookup(parts);
      } catch (e) {
        if (e instanceof DFSError && e.code === 'ENOENT') return null;
        throw e;
      }
    }

    resolve(path) {
      return this._lookup(this._parts(path));
    }

    _parentOf(parts) {
      if (!parts.length) throw new DFSError('EINVAL', 'a raiz não tem diretório pai');
      const parent = this._lookup(parts.slice(0, -1));
      if (parent.type !== 'dir') throw new DFSError('ENOTDIR', toPath(parts.slice(0, -1)) + ' não é um diretório');
      return { parent, name: parts[parts.length - 1] };
    }

    _file(path) {
      const ino = this.resolve(path);
      if (ino.type !== 'file') throw new DFSError('EISDIR', this.pathOf(ino) + ' é um diretório');
      return ino;
    }

    _inTrash(ino) {
      for (let c = ino; c; c = c.parent === null ? null : this._inode(c.parent)) if (c.id === this.trashId) return true;
      return false;
    }

    _assertWritable(dir) {
      if (this._inTrash(dir)) throw new DFSError('EPERM', 'a lixeira só recebe itens apagados (use rm / restore)');
    }

    _assertLive(ino) {
      if (ino.id === this.rootId || ino.id === this.trashId)
        throw new DFSError('EPERM', 'a raiz e a lixeira não podem ser alteradas');
      if (this._inTrash(ino)) throw new DFSError('EPERM', `${this.pathOf(ino)} está na lixeira: restaure antes de alterar`);
    }

    _detach(ino) {
      const parent = this._inode(ino.parent);
      if (parent) {
        parent.children.delete(ino.name);
        parent.mtime = Date.now();
      }
    }

    _attach(ino, parent, name) {
      ino.name = name;
      ino.parent = parent.id;
      parent.children.set(name, ino.id);
      parent.mtime = Date.now();
      ino.mtime = Date.now();
    }

    _countBlocks(ino) {
      if (ino.type === 'file') return ino.blocks.length;
      let n = 0;
      for (const id of ino.children.values()) n += this._countBlocks(this._inode(id));
      return n;
    }

    _fixCwd() {
      if (!this._tryLookup(this._parts(this.cwd))) this.cwd = '/';
    }

    children(dir) {
      return [...dir.children.values()]
        .map((id) => this._inode(id))
        .sort((a, b) => (a.type === b.type ? a.name.localeCompare(b.name, 'pt') : a.type === 'dir' ? -1 : 1));
    }

    /* ---------------------------------------------------------------- namespace */
    cd(path) {
      const ino = this.resolve(path);
      if (ino.type !== 'dir') throw new DFSError('ENOTDIR', this.pathOf(ino) + ' não é um diretório');
      this.cwd = this.pathOf(ino);
      return this.cwd;
    }

    mkdir(path, opts = {}) {
      const parts = this._parts(path);
      if (!parts.length) {
        if (opts.parents) return this._inode(this.rootId);
        throw new DFSError('EEXIST', '/ já existe');
      }
      let cur = this._inode(this.rootId);
      let created = null;
      for (let i = 0; i < parts.length; i++) {
        const name = parts[i];
        if (cur.type !== 'dir') throw new DFSError('ENOTDIR', this.pathOf(cur) + ' não é um diretório');
        const last = i === parts.length - 1;
        const existing = cur.children.get(name);
        if (existing !== undefined) {
          const ex = this._inode(existing);
          if (last && !opts.parents) throw new DFSError('EEXIST', toPath(parts) + ' já existe');
          if (ex.type !== 'dir') throw new DFSError('ENOTDIR', this.pathOf(ex) + ' é um arquivo');
          cur = ex;
          continue;
        }
        if (!last && !opts.parents)
          throw new DFSError('ENOENT', toPath(parts.slice(0, i + 1)) + ': a pasta pai não existe (use -p)');
        this._assertWritable(cur);
        validName(name);
        const d = this._newInode('dir', name, cur.id);
        cur.children.set(name, d.id);
        cur.mtime = Date.now();
        this._emit('info', 'namenode', `mkdir ${this.pathOf(d)}: só metadados, nenhum DataNode é acionado`);
        cur = d;
        created = d;
      }
      return created || cur;
    }

    /* ---------------------------------------------------------------- blocos */
    /** Política de posicionamento: espalha entre racks e equilibra por uso. */
    _chooseTargets(n, exclude) {
      const chosen = [];
      const pool = this.nodes.filter((x) => x.online && !exclude.has(x.id) && x.store.size < x.capacity);
      const usedRacks = new Set();
      for (const id of exclude) {
        const nd = this._node(id);
        if (nd) usedRacks.add(nd.rack);
      }
      while (chosen.length < n && pool.length) {
        pool.sort(
          (a, b) =>
            (usedRacks.has(a.rack) ? 1 : 0) - (usedRacks.has(b.rack) ? 1 : 0) || a.store.size - b.store.size || a.num - b.num
        );
        const pick = pool.shift();
        chosen.push(pick);
        usedRacks.add(pick.rack);
      }
      return chosen;
    }

    _allocBlock(file, index, chunk) {
      const targets = this._chooseTargets(file.replication, new Set());
      if (!targets.length) throw new DFSError('ENOSPC', 'nenhum DataNode online com espaço livre');
      const id = this.nextBlock++;
      const crc = crc32(chunk);
      const blk = { id, fileId: file.id, index, size: chunk.length, crc, replicas: new Set() };
      this.blocks.set(id, blk);
      for (const n of targets) {
        n.store.set(id, { data: chunk.slice(), crc });
        blk.replicas.add(n.id);
      }
      this._emit(
        'info',
        'namenode',
        `aloca blk_${id} (bloco ${index}, ${chunk.length} B, crc ${hex(crc)}) → pipeline ${targets.map((t) => t.id).join(' → ')}`
      );
      if (targets.length < file.replication)
        this._emit('warn', 'namenode', `blk_${id} sub-replicado: ${targets.length}/${file.replication} réplicas (poucos DataNodes disponíveis)`);
      return blk;
    }

    /** Grava os blocos de `bytes` e devolve os ids. Em caso de erro, desfaz tudo (atomicidade). */
    _storeBlocks(file, bytes, startIndex = 0) {
      const created = [];
      const bs = this.config.blockSize;
      try {
        for (let off = 0, idx = startIndex; off < bytes.length; off += bs, idx++) {
          created.push(this._allocBlock(file, idx, bytes.slice(off, off + bs)).id);
        }
      } catch (e) {
        created.forEach((id) => this._dropBlock(id));
        if (e instanceof DFSError && e.code === 'ENOSPC')
          throw new DFSError('ENOSPC', 'sem espaço nos DataNodes; nada foi gravado. Aumente a capacidade, adicione um DataNode ou use blocos maiores');
        throw e;
      }
      return created;
    }

    _dropBlock(id) {
      const b = this.blocks.get(id);
      if (!b) return [];
      const removedFrom = [];
      for (const n of this.nodes) {
        if (n.online && n.store.delete(id)) removedFrom.push(n.id);
      }
      this.blocks.delete(id);
      return removedFrom;
    }

    _freeBlocks(ids, why) {
      if (!ids.length) return;
      const perNode = new Map();
      for (const id of ids) {
        for (const nid of this._dropBlock(id)) {
          if (!perNode.has(nid)) perNode.set(nid, []);
          perNode.get(nid).push(id);
        }
      }
      this._emit('info', 'namenode', `libera ${ids.length} bloco(s) (${why})`);
      for (const [nid, list] of perNode) this._emit('info', 'datanode:' + nid, `remove ${list.map((i) => 'blk_' + i).join(', ')}`);
      const off = this.nodes.filter((n) => !n.online && ids.some((id) => n.store.has(id)));
      for (const n of off)
        this._emit('warn', 'datanode:' + n.id, 'offline: a limpeza das réplicas fica pendente até o próximo block report');
    }

    _blockValidNodes(b) {
      const out = [];
      for (const id of b.replicas) {
        const n = this._node(id);
        if (!n || !n.online) continue;
        const rep = n.store.get(b.id);
        if (rep && crc32(rep.data) === rep.crc) out.push(n);
      }
      return out;
    }

    /* ---------------------------------------------------------------- escrita */
    /**
     * Escrever: cria um arquivo novo. Falha se já existir.
     * `opts.replication` define o fator de replicação do arquivo ANTES de alocar os blocos
     * (usado pelo cp, para a cópia já nascer com o fator da origem).
     */
    write(path, data, opts = {}) {
      const bytes = toBytes(data);
      const parts = this._parts(path);
      const { parent, name } = this._parentOf(parts);
      this._assertWritable(parent);
      validName(name);
      const full = toPath(parts);
      if (parent.children.has(name)) throw new DFSError('EEXIST', `${full} já existe (use overwrite ou replace)`);
      this._emit('info', 'cliente', `write ${full} (${bytes.length} B)`);
      this._emit('info', 'namenode', `cria o inode de ${full} e divide o conteúdo em blocos de até ${this.config.blockSize} B`);
      const file = this._newInode('file', name, parent.id);
      if (opts.replication !== undefined) file.replication = opts.replication;
      try {
        file.blocks = this._storeBlocks(file, bytes);
      } catch (e) {
        this.inodes.delete(file.id);
        throw e;
      }
      file.size = bytes.length;
      parent.children.set(name, file.id);
      parent.mtime = Date.now();
      this._emit('ok', 'namenode', `commit: ${full} → ${file.blocks.length} bloco(s), ${bytes.length} B, v${file.version}`);
      return file;
    }

    /** Troca o conteúdo de um arquivo existente, com atomicidade: grava novo, troca ponteiros, libera antigo. */
    _replaceContent(file, bytes, why) {
      const full = this.pathOf(file);
      this._emit('info', 'namenode', `grava os novos blocos primeiro; só depois troca a lista de blocos do inode (se falhar, o arquivo antigo continua intacto)`);
      const fresh = this._storeBlocks(file, bytes);
      const old = file.blocks;
      file.blocks = fresh;
      file.size = bytes.length;
      file.version++;
      file.mtime = Date.now();
      this._freeBlocks(old, 'versão anterior de ' + full);
      this._emit('ok', 'namenode', `commit (${why}): ${full} → ${fresh.length} bloco(s), ${bytes.length} B, v${file.version}`);
      return file;
    }

    /** Sobrescrever: mantém nome/caminho/réplicas-alvo e troca todo o conteúdo. */
    overwrite(path, data) {
      const bytes = toBytes(data);
      const file = this._file(path);
      this._assertLive(file);
      this._emit('info', 'cliente', `overwrite ${this.pathOf(file)} (${bytes.length} B)`);
      return this._replaceContent(file, bytes, 'sobrescrita');
    }

    /** Anexar: acrescenta blocos novos ao final (blocos existentes são imutáveis). */
    append(path, data) {
      const bytes = toBytes(data);
      const file = this._file(path);
      this._assertLive(file);
      const full = this.pathOf(file);
      this._emit('info', 'cliente', `append ${full} (+${bytes.length} B)`);
      this._emit('info', 'namenode', 'blocos existentes são imutáveis: o texto novo vira blocos novos no fim do arquivo');
      const fresh = this._storeBlocks(file, bytes, file.blocks.length);
      file.blocks = file.blocks.concat(fresh);
      file.size += bytes.length;
      file.version++;
      file.mtime = Date.now();
      this._emit('ok', 'namenode', `commit: ${full} agora tem ${file.blocks.length} bloco(s), ${file.size} B, v${file.version}`);
      return file;
    }

    /** Substituir: o arquivo de destino passa a ter o conteúdo do arquivo de origem (origem é preservada). */
    replace(dest, src) {
      const d = this._file(dest);
      const s = this._file(src);
      this._assertLive(d);
      if (d.id === s.id) throw new DFSError('EINVAL', 'destino e origem são o mesmo arquivo');
      this._emit('info', 'cliente', `replace ${this.pathOf(d)} ← ${this.pathOf(s)}`);
      const { bytes } = this._readFile(s);
      return this._replaceContent(d, bytes, 'substituição');
    }

    /* ---------------------------------------------------------------- leitura */
    _readBlock(b) {
      const bads = [];
      const skipped = [];
      for (const id of [...b.replicas]) {
        const n = this._node(id);
        if (!n || !n.online) {
          skipped.push(id);
          continue;
        }
        const rep = n.store.get(b.id);
        if (!rep) {
          bads.push(n);
          continue;
        }
        if (crc32(rep.data) !== rep.crc) {
          this._emit('warn', 'datanode:' + n.id, `blk_${b.id}: checksum não confere (esperado ${hex(rep.crc)}, obtido ${hex(crc32(rep.data))})`);
          bads.push(n);
          continue;
        }
        if (skipped.length) this._emit('warn', 'cliente', `blk_${b.id}: ${skipped.join(', ')} offline, failover para ${n.id}`);
        this._emit('info', 'datanode:' + n.id, `lê blk_${b.id} (bloco ${b.index}): checksum ok`);
        for (const bad of bads) this._dropBadReplica(b, bad);
        return { node: n, data: rep.data, bad: bads.length > 0 };
      }
      const why = [];
      if (skipped.length) why.push('offline: ' + skipped.join(', '));
      if (bads.length) why.push('corrompida: ' + bads.map((x) => x.id).join(', '));
      throw new DFSError('EIO', `blk_${b.id} indisponível, nenhuma réplica íntegra online${why.length ? ' (' + why.join('; ') + ')' : ''}`);
    }

    _dropBadReplica(b, n) {
      this.stats.corruptDetected++;
      n.store.delete(b.id);
      b.replicas.delete(n.id);
      this._emit('warn', 'namenode', `réplica de blk_${b.id} em ${n.id} marcada como inválida; re-replicação agendada`);
    }

    _readFile(file) {
      const chunks = [];
      let bad = false;
      let failure = null;
      for (const id of file.blocks) {
        const b = this.blocks.get(id);
        try {
          const r = this._readBlock(b);
          if (r.bad) bad = true;
          chunks.push({ blockId: id, index: b.index, nodeId: r.node.id, bytes: r.data });
        } catch (e) {
          failure = e;
          break;
        }
      }
      if (bad) this.recover();
      if (failure) throw failure;
      return { file, chunks, bytes: concat(chunks.map((c) => c.bytes)) };
    }

    /** Ler: busca os blocos (com failover entre réplicas e verificação de checksum) e remonta o arquivo. */
    read(path) {
      const file = this._file(path);
      const full = this.pathOf(file);
      this._emit('info', 'cliente', `read ${full}`);
      this._emit('info', 'namenode', `devolve a localização de ${file.blocks.length} bloco(s) de ${full}`);
      const r = this._readFile(file);
      this._emit('ok', 'cliente', `${full} remontado: ${r.bytes.length} B de ${r.chunks.length} bloco(s)`);
      r.path = full;
      return r;
    }

    /** Leitura silenciosa (não gera eventos), só para a interface preencher formulários. */
    peek(path) {
      const file = this._file(path);
      const parts = [];
      for (const id of file.blocks) {
        const b = this.blocks.get(id);
        const n = this._blockValidNodes(b)[0];
        if (!n) throw new DFSError('EIO', `blk_${id} indisponível`);
        parts.push(n.store.get(id).data);
      }
      return concat(parts);
    }

    /* ---------------------------------------------------------------- apagar / lixeira */
    _purge(ino) {
      if (ino.type === 'dir') for (const id of [...ino.children.values()]) this._purge(this._inode(id));
      else this._freeBlocks(ino.blocks, 'exclusão permanente');
      this.inodes.delete(ino.id);
    }

    /** Apagar: por padrão move para a lixeira (só metadados). Com `permanent`, libera os blocos. */
    rm(path, opts = {}) {
      const ino = this.resolve(path);
      if (ino.id === this.rootId || ino.id === this.trashId) throw new DFSError('EPERM', 'a raiz e a lixeira não podem ser apagadas');
      const full = this.pathOf(ino);
      if (ino.type === 'dir' && ino.children.size && !opts.recursive)
        throw new DFSError('ENOTEMPTY', `${full} não está vazia (use -r)`);
      const nBlocks = this._countBlocks(ino);
      this._emit('info', 'cliente', `rm ${opts.permanent || this._inTrash(ino) ? '-f ' : ''}${full}`);
      if (opts.permanent || this._inTrash(ino)) {
        this._detach(ino);
        this._purge(ino);
        this._emit('ok', 'namenode', `${full} removido definitivamente; blocos liberados nos DataNodes`);
      } else {
        const trash = this._inode(this.trashId);
        let name = ino.name;
        for (let k = 1; trash.children.has(name); k++) name = `${ino.name}.${k}`;
        this._detach(ino);
        ino.trashOrigin = full;
        this._attach(ino, trash, name);
        this._emit(
          'ok',
          'namenode',
          `${full} → /${TRASH}/${name}: só metadados, ${nBlocks} bloco(s) continuam nos DataNodes (dá para restaurar)`
        );
      }
      this._fixCwd();
    }

    restore(path) {
      const ino = this.resolve(path);
      if (ino.parent !== this.trashId || !ino.trashOrigin)
        throw new DFSError('EINVAL', 'informe um item que esteja diretamente dentro de /' + TRASH);
      const parts = parsePath(ino.trashOrigin, '/');
      const name = parts[parts.length - 1];
      this.mkdir(toPath(parts.slice(0, -1)), { parents: true });
      const parent = this._lookup(parts.slice(0, -1));
      if (parent.children.has(name))
        throw new DFSError('EEXIST', `${toPath(parts)} já existe: renomeie ou mova o item atual antes de restaurar`);
      this._detach(ino);
      this._attach(ino, parent, name);
      delete ino.trashOrigin;
      this._emit('ok', 'namenode', `restaurado: ${toPath(parts)} (só metadados, os blocos nunca saíram do lugar)`);
      return ino;
    }

    emptyTrash() {
      const trash = this._inode(this.trashId);
      const items = [...trash.children.values()];
      this._emit('info', 'cliente', 'emptytrash');
      for (const id of items) {
        const ino = this._inode(id);
        this._detach(ino);
        this._purge(ino);
      }
      this._emit('ok', 'namenode', `lixeira esvaziada (${items.length} item(ns))`);
      return items.length;
    }

    /* ---------------------------------------------------------------- renomear / mover / copiar */
    /** Renomear: só troca o nome no namespace. Nenhum bloco se move. */
    rename(path, newName) {
      const ino = this.resolve(path);
      this._assertLive(ino);
      validName(newName);
      const parent = this._inode(ino.parent);
      if (parent.children.has(newName)) throw new DFSError('EEXIST', `já existe "${newName}" em ${this.pathOf(parent)}`);
      const old = this.pathOf(ino);
      this._detach(ino);
      this._attach(ino, parent, newName);
      this._emit('info', 'cliente', `rename ${old} → ${newName}`);
      this._emit('ok', 'namenode', `${old} → ${this.pathOf(ino)}: só metadados, ${this._countBlocks(ino)} bloco(s) permanecem onde estão`);
      this._fixCwd();
      return ino;
    }

    /** Mover: troca o diretório pai no namespace. Com `force`, substitui um arquivo existente no destino. */
    mv(src, dest, opts = {}) {
      const ino = this.resolve(src);
      this._assertLive(ino);
      const parts = this._parts(dest);
      const t = this._tryLookup(parts);
      let destDir;
      let newName;
      if (t && t.type === 'dir') {
        destDir = t;
        newName = ino.name;
      } else {
        const p = this._parentOf(parts);
        destDir = p.parent;
        newName = p.name;
      }
      this._assertWritable(destDir);
      validName(newName);
      if (ino.type === 'dir')
        for (let c = destDir; c; c = c.parent === null ? null : this._inode(c.parent))
          if (c.id === ino.id) throw new DFSError('EINVAL', 'não é possível mover uma pasta para dentro dela mesma');
      const exId = destDir.children.get(newName);
      let replaced = null;
      if (exId === ino.id) throw new DFSError('EINVAL', 'origem e destino são o mesmo item');
      if (exId !== undefined) {
        const ex = this._inode(exId);
        if (!opts.force) throw new DFSError('EEXIST', `${this.pathOf(ex)} já existe (use mv -f para substituir)`);
        if (ex.type === 'dir' || ino.type === 'dir') throw new DFSError('EISDIR', 'só é possível substituir arquivo por arquivo');
        replaced = ex;
      }
      const old = this.pathOf(ino);
      this._emit('info', 'cliente', `mv ${old} → ${toPath(parts)}`);
      if (replaced) {
        this._detach(replaced);
        this._purge(replaced);
        this._emit('info', 'namenode', `destino já existia: arquivo antigo substituído e seus blocos liberados`);
      }
      this._detach(ino);
      this._attach(ino, destDir, newName);
      this._emit('ok', 'namenode', `${old} → ${this.pathOf(ino)}: só metadados, ${this._countBlocks(ino)} bloco(s) não se movem`);
      this._fixCwd();
      return ino;
    }

    /** Copiar arquivo: lê os blocos e grava blocos novos (cópia de verdade, ocupa espaço). */
    cp(src, dest) {
      const s = this._file(src);
      const parts = this._parts(dest);
      const t = this._tryLookup(parts);
      let full;
      if (t && t.type === 'dir') full = (this.pathOf(t) === '/' ? '' : this.pathOf(t)) + '/' + s.name;
      else full = toPath(parts);
      this._emit('info', 'cliente', `cp ${this.pathOf(s)} → ${full}`);
      const { bytes } = this._readFile(s);
      // a cópia herda o fator de replicação da origem já na alocação dos blocos
      return this.write(full, bytes, { replication: s.replication });
    }

    setReplication(path, n) {
      const file = this._file(path);
      n = Number(n);
      if (!Number.isInteger(n) || n < 1 || n > this.nodes.length)
        throw new DFSError('EINVAL', `fator de replicação deve ser um inteiro entre 1 e ${this.nodes.length}`);
      this._assertLive(file);
      this._emit('info', 'cliente', `setrep ${n} ${this.pathOf(file)} (antes: ${file.replication})`);
      file.replication = n;
      this.recover();
      return file;
    }

    /* ---------------------------------------------------------------- nós */
    addNode() {
      const n = this._addNode(false);
      return n;
    }

    _addNode(silent) {
      if (this.nodes.length >= MAX_NODES) throw new DFSError('EINVAL', `limite de ${MAX_NODES} DataNodes`);
      const num = this.nextNode++;
      const node = {
        id: 'dn' + num,
        num,
        rack: 'rack' + (((num - 1) >> 1) + 1),
        online: true,
        dead: false,
        capacity: this.config.nodeCapacity,
        store: new Map()
      };
      this.nodes.push(node);
      if (!silent) this._emit('ok', 'namenode', `novo DataNode ${node.id} registrado (${node.rack}, ${node.capacity} blocos)`);
      return node;
    }

    _nodeOrThrow(id) {
      const n = this._node(id);
      if (!n) throw new DFSError('ENOENT', `DataNode "${id}" não existe`);
      return n;
    }

    setCapacity(cap) {
      cap = Number(cap);
      if (!Number.isInteger(cap) || cap < 4 || cap > 128) throw new DFSError('EINVAL', 'capacidade deve estar entre 4 e 128 blocos');
      for (const n of this.nodes)
        if (n.store.size > cap) throw new DFSError('EINVAL', `${n.id} já usa ${n.store.size} blocos; escolha ao menos esse valor`);
      this.config.nodeCapacity = cap;
      this.nodes.forEach((n) => (n.capacity = cap));
      this._emit('info', 'sistema', `capacidade por DataNode: ${cap} blocos`);
    }

    setBlockSize(size) {
      size = Number(size);
      if (!Number.isInteger(size) || size < 4 || size > 65536) throw new DFSError('EINVAL', 'tamanho de bloco deve estar entre 4 e 65536 bytes');
      this.config.blockSize = size;
      this._emit('info', 'sistema', `tamanho de bloco para novos arquivos: ${size} B`);
    }

    setDefaultReplication(n) {
      n = Number(n);
      if (!Number.isInteger(n) || n < 1 || n > MAX_NODES) throw new DFSError('EINVAL', `replicação padrão deve estar entre 1 e ${MAX_NODES}`);
      this.config.replication = n;
      this._emit('info', 'sistema', `replicação padrão para novos arquivos: ${n}`);
    }

    killNode(id) {
      const n = this._nodeOrThrow(id);
      if (!n.online) throw new DFSError('EINVAL', `${id} já está offline`);
      n.online = false;
      n.dead = false;
      this._emit('error', 'datanode:' + id, 'sem resposta: nó fora do ar');
      this._emit('warn', 'namenode', `heartbeat de ${id} sumiu; aguardando o timeout antes de declará-lo morto`);
      return n;
    }

    /** NameNode desiste do nó: esquece suas réplicas (o disco do nó continua com os dados). */
    declareDead(id) {
      const n = this._nodeOrThrow(id);
      if (n.online || n.dead) return false;
      n.dead = true;
      let lost = 0;
      for (const b of this.blocks.values()) if (b.replicas.delete(id)) lost++;
      this._emit('error', 'namenode', `${id} declarado MORTO: ${lost} réplica(s) removidas do mapa de blocos`);
      return true;
    }

    reviveNode(id) {
      const n = this._nodeOrThrow(id);
      if (n.online) throw new DFSError('EINVAL', `${id} já está online`);
      n.online = true;
      n.dead = false;
      this._emit('ok', 'datanode:' + id, `de volta ao cluster; envia block report com ${n.store.size} bloco(s)`);
      let orphans = 0;
      for (const bid of [...n.store.keys()]) {
        const b = this.blocks.get(bid);
        if (!b) {
          n.store.delete(bid);
          orphans++;
        } else b.replicas.add(id);
      }
      if (orphans) this._emit('info', 'namenode', `${orphans} bloco(s) órfão(s) em ${id} (arquivo já apagado): descartados`);
      this.recover();
      return n;
    }

    /** Corrompe uma réplica (simula bit rot). O checksum guardado não muda, então a verificação vai falhar. */
    corrupt(blockId, nodeId) {
      const n = this._nodeOrThrow(nodeId);
      if (!n.online) throw new DFSError('EINVAL', `${nodeId} está offline`);
      const rep = n.store.get(blockId);
      if (!rep) throw new DFSError('ENOENT', `blk_${blockId} não está em ${nodeId}`);
      rep.data[0] ^= 0xff;
      rep.tampered = true;
      this._emit('error', 'datanode:' + nodeId, `blk_${blockId}: um bit-flip silencioso corrompeu a réplica (o NameNode ainda não sabe)`);
    }

    /** Escolhe uma réplica de um arquivo para corromper (atalho para demonstração). */
    corruptFile(path) {
      const file = this._file(path);
      for (const id of file.blocks) {
        const b = this.blocks.get(id);
        const n = this._blockValidNodes(b)[0];
        if (n) {
          this.corrupt(id, n.id);
          return { blockId: id, nodeId: n.id };
        }
      }
      throw new DFSError('EIO', 'nenhuma réplica íntegra para corromper');
    }

    /* ---------------------------------------------------------------- recuperação */
    /** Re-replica blocos sub-replicados e remove réplicas em excesso. */
    recover() {
      let copied = 0;
      let trimmed = 0;
      const stuck = [];
      for (const b of this.blocks.values()) {
        const file = this.inodes.get(b.fileId);
        const target = file ? file.replication : this.config.replication;
        if (b.replicas.size < target) {
          const sources = this._blockValidNodes(b);
          if (!sources.length) {
            stuck.push(b.id);
            continue;
          }
          const need = target - b.replicas.size;
          const picks = this._chooseTargets(need, new Set(b.replicas));
          for (const n of picks) {
            const src = sources[0];
            const rep = src.store.get(b.id);
            n.store.set(b.id, { data: rep.data.slice(), crc: rep.crc });
            b.replicas.add(n.id);
            copied++;
            this._emit('ok', 'datanode:' + n.id, `re-replicação: copia blk_${b.id} de ${src.id} (agora ${b.replicas.size}/${target})`);
          }
          if (picks.length < need)
            this._emit('warn', 'namenode', `blk_${b.id} segue sub-replicado (${b.replicas.size}/${target}): faltam DataNodes com espaço`);
        } else if (b.replicas.size > target) {
          while (b.replicas.size > target) {
            const holders = [...b.replicas].map((id) => this._node(id)).sort((x, y) => y.store.size - x.store.size);
            const victim = holders[0];
            victim.store.delete(b.id);
            b.replicas.delete(victim.id);
            trimmed++;
            this._emit('info', 'datanode:' + victim.id, `réplica extra de blk_${b.id} removida (alvo ${target})`);
          }
        }
      }
      if (stuck.length)
        this._emit('error', 'namenode', `${stuck.length} bloco(s) sem nenhuma réplica íntegra online: ${stuck.slice(0, 6).map((i) => 'blk_' + i).join(', ')}${stuck.length > 6 ? '…' : ''}. Aguardando o retorno de algum DataNode`);
      return { copied, trimmed, stuck: stuck.length };
    }

    /** Equilibra o uso entre os DataNodes movendo réplicas do mais cheio para o mais vazio. */
    rebalance() {
      const online = () => this.nodes.filter((n) => n.online);
      let moves = 0;
      for (let guard = 0; guard < 2000; guard++) {
        const sorted = online().sort((a, b) => a.store.size - b.store.size);
        let done = false;
        outer: for (let hi = sorted.length - 1; hi > 0; hi--) {
          for (let lo = 0; lo < hi; lo++) {
            const H = sorted[hi];
            const L = sorted[lo];
            if (H.store.size - L.store.size <= 1) continue;
            if (L.store.size >= L.capacity) continue;
            const bid = [...H.store.keys()].find((id) => !L.store.has(id) && this.blocks.get(id) && this.blocks.get(id).replicas.has(H.id));
            if (bid === undefined) continue;
            const rep = H.store.get(bid);
            L.store.set(bid, { data: rep.data.slice(), crc: rep.crc, tampered: rep.tampered });
            H.store.delete(bid);
            const b = this.blocks.get(bid);
            b.replicas.delete(H.id);
            b.replicas.add(L.id);
            moves++;
            this._emit('info', 'datanode:' + L.id, `rebalanceamento: blk_${bid} movido de ${H.id} (${H.store.size}) para ${L.id} (${L.store.size})`);
            done = true;
            break outer;
          }
        }
        if (!done) break;
      }
      this._emit('ok', 'namenode', moves ? `rebalanceamento concluído: ${moves} réplica(s) movida(s)` : 'cluster já está equilibrado');
      return moves;
    }

    /** Verificação de integridade (como o `hdfs fsck`). Com `repair`, remove réplicas ruins e re-replica. */
    fsck(opts = {}) {
      this._emit('info', 'cliente', `fsck${opts.repair ? ' --repair' : ''}`);
      let badCount = 0;
      for (const b of [...this.blocks.values()]) {
        for (const id of [...b.replicas]) {
          const n = this._node(id);
          if (!n || !n.online) continue;
          const rep = n.store.get(b.id);
          const bad = !rep || crc32(rep.data) !== rep.crc;
          if (!bad) continue;
          badCount++;
          this._emit('warn', 'datanode:' + id, `blk_${b.id}: réplica corrompida detectada pelo checksum`);
          if (opts.repair) {
            if (this._blockValidNodes(b).length) this._dropBadReplica(b, n);
            else this._emit('error', 'namenode', `blk_${b.id}: nenhuma réplica íntegra, não há de onde reparar`);
          }
        }
      }
      if (opts.repair) this.recover();
      const h = this.health();
      this._emit(h.status === 'HEALTHY' ? 'ok' : h.status === 'DEGRADED' ? 'warn' : 'error', 'namenode', `fsck: sistema ${h.status} (${h.validReplicas}/${h.expectedReplicas} réplicas íntegras, ${h.corruptReplicas} corrompida(s), ${h.missingBlocks} bloco(s) indisponível(is))`);
      h.found = badCount;
      return h;
    }

    /* ---------------------------------------------------------------- consultas p/ a interface */
    blockHealth(b) {
      const file = this.inodes.get(b.fileId);
      const target = file ? file.replication : this.config.replication;
      let valid = 0;
      let corrupt = 0;
      for (const id of b.replicas) {
        const n = this._node(id);
        if (!n || !n.online) continue;
        const rep = n.store.get(b.id);
        if (!rep) continue;
        if (crc32(rep.data) === rep.crc) valid++;
        else corrupt++;
      }
      return { target, valid, corrupt, believed: b.replicas.size };
    }

    fileHealth(ino) {
      if (ino.type !== 'file') return null;
      let min = Infinity;
      let target = ino.replication;
      for (const id of ino.blocks) {
        const h = this.blockHealth(this.blocks.get(id));
        min = Math.min(min, h.valid);
      }
      if (min === Infinity) min = target;
      return { target, valid: min, status: min === 0 && ino.blocks.length ? 'missing' : min < target ? 'under' : 'ok' };
    }

    /** Descreve onde está cada bloco de um arquivo (para stat/tabelas). */
    blockLocations(ino) {
      return ino.blocks.map((id) => {
        const b = this.blocks.get(id);
        const reps = [...b.replicas]
          .map((nid) => this._node(nid))
          .filter(Boolean)
          .sort((x, y) => x.num - y.num)
          .map((n) => {
            const rep = n.store.get(b.id);
            const ok = !!rep && crc32(rep.data) === rep.crc;
            return { node: n.id, online: n.online, ok };
          });
        return { id, index: b.index, size: b.size, crc: b.crc, replicas: reps };
      });
    }

    health() {
      let files = 0;
      for (const ino of this.inodes.values()) if (ino.type === 'file') files++;
      let blocks = 0;
      let expected = 0;
      let valid = 0;
      let corrupt = 0;
      let missing = 0;
      let under = 0;
      let over = 0;
      for (const b of this.blocks.values()) {
        const h = this.blockHealth(b);
        blocks++;
        expected += h.target;
        valid += h.valid;
        corrupt += h.corrupt;
        if (h.valid === 0) missing++;
        if (h.valid < h.target) under++;
        if (h.believed > h.target) over++;
      }
      const online = this.nodes.filter((n) => n.online).length;
      const status = missing ? 'CORRUPT' : under || corrupt ? 'DEGRADED' : 'HEALTHY';
      return {
        files,
        blocks,
        nodes: this.nodes.length,
        onlineNodes: online,
        expectedReplicas: expected,
        validReplicas: valid,
        corruptReplicas: corrupt,
        underReplicated: under,
        overReplicated: over,
        missingBlocks: missing,
        status
      };
    }

    /* ---------------------------------------------------------------- persistência */
    serialize() {
      return {
        v: 1,
        config: this.config,
        nextInode: this.nextInode,
        nextBlock: this.nextBlock,
        nextNode: this.nextNode,
        cwd: this.cwd,
        stats: this.stats,
        rootId: this.rootId,
        trashId: this.trashId,
        inodes: [...this.inodes.values()].map((i) => (i.type === 'dir' ? Object.assign({}, i, { children: [...i.children] }) : i)),
        blocks: [...this.blocks.values()].map((b) => Object.assign({}, b, { replicas: [...b.replicas] })),
        nodes: this.nodes.map((n) => ({
          id: n.id,
          num: n.num,
          rack: n.rack,
          online: n.online,
          dead: n.dead,
          capacity: n.capacity,
          store: [...n.store].map(([id, r]) => [id, bytesToB64(r.data), r.crc, !!r.tampered])
        }))
      };
    }

    static deserialize(obj) {
      if (!obj || obj.v !== 1) throw new Error('estado salvo incompatível');
      const c = new Cluster(obj.config);
      c.events = [];
      c.nextInode = obj.nextInode;
      c.nextBlock = obj.nextBlock;
      c.nextNode = obj.nextNode;
      c.cwd = obj.cwd || '/';
      c.stats = obj.stats || { corruptDetected: 0 };
      c.rootId = obj.rootId;
      c.trashId = obj.trashId;
      c.inodes = new Map(
        obj.inodes.map((i) => [i.id, i.type === 'dir' ? Object.assign({}, i, { children: new Map(i.children) }) : Object.assign({}, i)])
      );
      c.blocks = new Map(obj.blocks.map((b) => [b.id, Object.assign({}, b, { replicas: new Set(b.replicas) })]));
      c.nodes = obj.nodes.map((n) => ({
        id: n.id,
        num: n.num,
        rack: n.rack,
        online: n.online,
        dead: n.dead,
        capacity: n.capacity,
        store: new Map(n.store.map(([id, b64, crc, tampered]) => [id, { data: b64ToBytes(b64), crc, tampered }]))
      }));
      c._fixCwd();
      return c;
    }
  }

  return { Cluster, DFSError, crc32, hex, parsePath, MAX_NODES, TRASH, DEFAULTS };
});
