/* Mini-DFS — interface. Usa o motor em js/dfs.js (window.MiniDFS). */
(function () {
  'use strict';

  const { Cluster, DFSError, crc32, hex, DEFAULTS, TRASH } = window.MiniDFS;
  const CFG = Object.assign(
    {
      sistema: 'DFS',
      disciplina: '',
      professor: '',
      turma: '',
      equipe: [],
      timeoutHeartbeat: 3500
    },
    window.MINIDFS_CONFIG || {}
  );

  /* ================================================================ utilidades */
  const $ = (sel, root) => (root || document).querySelector(sel);
  const $$ = (sel, root) => Array.from((root || document).querySelectorAll(sel));
  const esc = (s) =>
    String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const fmtBytes = (n) => (n < 1024 ? n + ' B' : (n / 1024).toFixed(n < 10240 ? 2 : 1) + ' KB');
  const fmtTime = (ts) => new Date(ts).toLocaleTimeString('pt-BR');
  const joinPath = (dir, name) => (dir === '/' ? '' : dir) + '/' + name;

  const PALETTE = ['#3b6cff', '#8b5cf6', '#e8478c', '#12a594', '#f0721d', '#0aa5d6', '#7cb518', '#b04fe0', '#2f9e44', '#d6336c'];
  const colorOf = (id) => PALETTE[id % PALETTE.length];

  const ICON = {
    folder:
      '<svg class="ico" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linejoin="round"><path d="M2.5 5.5a1.5 1.5 0 0 1 1.5-1.5h3.6l1.7 2H16a1.5 1.5 0 0 1 1.5 1.5v7A1.5 1.5 0 0 1 16 16H4a1.5 1.5 0 0 1-1.5-1.5z"/></svg>',
    trash:
      '<svg class="ico" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M4 6h12M8 6V4h4v2M6 6l.7 10h6.6L14 6M8.5 9v4M11.5 9v4"/></svg>'
  };

  /* ================================================================ estado */
  const KEY = 'minidfs.estado.v1';
  let fresh = false;
  let cluster = restore();
  if (!cluster) {
    cluster = new Cluster();
    fresh = true;
  }
  window.cluster = cluster; // útil para explorar no DevTools

  const ui = {
    selected: null,
    expanded: new Set([cluster.rootId]),
    tab: 'console',
    pending: new Map(), // nodeId -> {timer, t0}
    auto: true,
    prevSlots: new Set(),
    demo: null,
    history: [],
    hIdx: 0
  };

  function restore() {
    try {
      const raw = localStorage.getItem(KEY);
      if (!raw) return null;
      return Cluster.deserialize(JSON.parse(raw));
    } catch (e) {
      return null;
    }
  }
  function save() {
    try {
      localStorage.setItem(KEY, JSON.stringify(cluster.serialize()));
    } catch (e) {
      /* armazenamento indisponível: segue sem persistir */
    }
  }

  /* ================================================================ eventos do cluster */
  let capture = null;
  cluster.subscribe((ev) => {
    appendLog(ev);
    if (capture) capture.push(ev);
  });

  function actorInfo(actor) {
    if (actor.startsWith('datanode:')) return { kind: 'datanode', label: actor.slice(9) };
    if (actor === 'namenode') return { kind: 'namenode', label: 'NameNode' };
    if (actor === 'cliente') return { kind: 'cliente', label: 'cliente' };
    return { kind: 'sistema', label: 'sistema' };
  }

  function appendLog(ev) {
    const out = $('#log-out');
    if (!out) return;
    const a = actorInfo(ev.actor);
    const near = out.scrollHeight - out.scrollTop - out.clientHeight < 60;
    const row = document.createElement('div');
    row.className = 'lg lv-' + ev.level;
    row.dataset.kind = a.kind;
    row.innerHTML = `<time>${fmtTime(ev.time)}</time><span class="tag a-${a.kind}">${esc(a.label)}</span><span class="msg">${esc(ev.msg)}</span>`;
    out.appendChild(row);
    while (out.children.length > 500) out.removeChild(out.firstChild);
    if (near) out.scrollTop = out.scrollHeight;
  }

  /* ================================================================ saúde / timers de falha */
  function clearPending(id) {
    const p = ui.pending.get(id);
    if (p) clearTimeout(p.timer);
    ui.pending.delete(id);
  }
  function clearAllPending() {
    for (const id of [...ui.pending.keys()]) clearPending(id);
  }

  function scheduleDeath(id) {
    clearPending(id);
    if (!ui.auto) return;
    const t0 = Date.now();
    const timer = setTimeout(() => {
      ui.pending.delete(id);
      const evs = [];
      capture = evs;
      try {
        if (cluster.declareDead(id)) cluster.recover();
      } catch (e) {
        /* nó pode ter sido revivido */
      } finally {
        capture = null;
      }
      if (evs.length) conEvents(evs);
      afterChange();
    }, CFG.timeoutHeartbeat);
    ui.pending.set(id, { timer, t0 });
  }

  function doKill(id) {
    cluster.killNode(id);
    scheduleDeath(id);
  }
  function doRevive(id) {
    clearPending(id);
    cluster.reviveNode(id);
  }
  function forceRecovery() {
    let any = false;
    for (const n of cluster.nodes) {
      if (!n.online) {
        clearPending(n.id);
        if (cluster.declareDead(n.id)) any = true;
      }
    }
    const r = cluster.recover();
    return { any, r };
  }

  /* ================================================================ renderização */
  function afterChange() {
    if (ui.selected && !cluster.inodes.has(ui.selected)) ui.selected = null;
    save();
    renderAll();
  }

  function renderAll() {
    renderStats();
    renderExplorer();
    renderDetail();
    renderCluster();
    if (ui.tab === 'blocks') renderBlocksTab();
    updatePrompt();
  }

  function renderHeader() {
    $('#subtitle').textContent = `Simulador de ${CFG.sistema}: NameNode, DataNodes, blocos, replicação e recuperação de falhas.`;
    const meta = [CFG.disciplina, CFG.professor, CFG.turma].filter(Boolean).join(', ');
    $('#team').innerHTML =
      `<span class="lbl">${esc(meta)}. Equipe:</span>` + CFG.equipe.map((m) => `<span class="member">${esc(m)}</span>`).join('');
  }

  /* ---------------------------------------------------------------- estatísticas */
  function renderStats() {
    const h = cluster.health();
    const label = { HEALTHY: 'Saudável', DEGRADED: 'Degradado', CORRUPT: 'Dados indisponíveis' }[h.status];
    const detail = {
      HEALTHY: 'todas as réplicas no alvo',
      DEGRADED: 'faltam réplicas, mas os dados estão acessíveis',
      CORRUPT: 'algum bloco ficou sem réplica íntegra'
    }[h.status];
    const stat = (n, l, cls) => `<div class="stat ${cls || ''}"><b>${n}</b><span>${l}</span></div>`;
    $('#stats').innerHTML =
      `<div class="health ${h.status}"><i class="hdot"></i><div><strong>${label}</strong><span>${detail}</span></div></div>` +
      stat(`${h.onlineNodes}/${h.nodes}`, 'DataNodes online', h.onlineNodes < h.nodes ? 'warn' : '') +
      stat(h.files, 'arquivos') +
      stat(h.blocks, 'blocos') +
      stat(`${h.validReplicas}/${h.expectedReplicas}`, 'réplicas íntegras', h.validReplicas < h.expectedReplicas ? 'warn' : '') +
      stat(h.underReplicated, 'blocos sub-replicados', h.underReplicated ? 'warn' : '') +
      stat(h.corruptReplicas, 'réplicas corrompidas', h.corruptReplicas ? 'bad' : '');
  }

  /* ---------------------------------------------------------------- explorador */
  function targetDir() {
    const sel = ui.selected && cluster.inodes.get(ui.selected);
    let dir = sel ? (sel.type === 'dir' ? sel : cluster.inodes.get(sel.parent)) : null;
    if (!dir || cluster._inTrash(dir)) return '/';
    return cluster.pathOf(dir);
  }

  function rowHtml(ino, depth) {
    const isDir = ino.type === 'dir';
    const open = ui.expanded.has(ino.id);
    const isTrash = ino.id === cluster.trashId;
    const hl = isDir ? null : cluster.fileHealth(ino);
    const badge = hl
      ? `<span class="hb ${hl.status}" title="réplicas íntegras (mínimo entre os blocos) / alvo de replicação">${hl.valid}/${hl.target}</span>`
      : '';
    const meta = isDir ? (ino.children.size ? ino.children.size + (ino.children.size === 1 ? ' item' : ' itens') : '') : fmtBytes(ino.size);
    const icon = isTrash ? ICON.trash : isDir ? ICON.folder : `<i class="dot" style="background:${colorOf(ino.id)}"></i>`;
    return (
      `<div class="row${ui.selected === ino.id ? ' sel' : ''}${isTrash ? ' trash' : ''}" role="treeitem" tabindex="0" data-action="select" data-id="${ino.id}" style="--d:${depth}"` +
      `${isDir ? ` aria-expanded="${open}"` : ''}>` +
      `<span class="caret${isDir && ino.children.size ? '' : ' none'}${open ? ' open' : ''}" data-action="toggle" data-id="${ino.id}"></span>` +
      `${icon}<span class="nm">${esc(ino.name)}</span><span class="meta">${meta}</span>${badge}</div>`
    );
  }

  function renderExplorer() {
    const root = cluster.inodes.get(cluster.rootId);
    const rows = [];
    rows.push(
      `<div class="row${ui.selected === root.id ? ' sel' : ''}" role="treeitem" tabindex="0" data-action="select" data-id="${root.id}" style="--d:0">` +
        `<span class="caret none"></span>${ICON.folder}<span class="nm">/ (raiz)</span></div>`
    );
    let files = 0;
    const walk = (dir, depth) => {
      for (const k of cluster.children(dir)) {
        rows.push(rowHtml(k, depth));
        if (k.type === 'file') files++;
        if (k.type === 'dir' && ui.expanded.has(k.id)) walk(k, depth + 1);
      }
    };
    walk(root, 1);
    const trash = cluster.inodes.get(cluster.trashId);
    const nothing = [...cluster.inodes.values()].filter((i) => i.type === 'file').length === 0;
    $('#explorer').innerHTML =
      `<div class="panel-head"><h2>Namespace</h2><span class="sub">metadados no NameNode</span></div>` +
      `<div class="toolbar"><button class="btn primary sm" data-action="new-file">Novo arquivo</button>` +
      `<button class="btn sm" data-action="new-dir">Nova pasta</button>` +
      `<span class="crumb">pasta atual <code>${esc(cluster.cwd)}</code></span></div>` +
      `<div class="scroll tree" role="tree" aria-label="Árvore de pastas e arquivos">${rows.join('')}` +
      (nothing && !trash.children.size
        ? `<p class="empty">Nenhum arquivo ainda. Crie um com “Novo arquivo” ou rode a demo guiada.</p>`
        : '') +
      `</div>`;
  }

  /* ---------------------------------------------------------------- detalhes */
  const btn = (action, label, cls) => `<button class="btn sm ${cls || ''}" data-action="${action}">${label}</button>`;

  function repChips(list) {
    return list
      .map((r) => {
        const cls = !r.online ? 'off' : r.ok ? '' : 'bad';
        const tip = !r.online ? 'DataNode offline' : r.ok ? 'réplica íntegra' : 'checksum não confere';
        return `<span class="rep ${cls}" title="${tip}">${r.node}</span>`;
      })
      .join('');
  }

  function renderDetail() {
    const el = $('#detail');
    const ino = ui.selected && cluster.inodes.get(ui.selected);
    if (!ino) {
      el.innerHTML =
        `<div class="panel-head"><h2>Detalhes</h2></div>` +
        `<p class="empty">Selecione um arquivo ou pasta para ver metadados, blocos, réplicas e as ações disponíveis. Os blocos do arquivo selecionado acendem nos DataNodes.</p>`;
      return;
    }
    const path = cluster.pathOf(ino);
    const isRoot = ino.id === cluster.rootId;
    const isTrashDir = ino.id === cluster.trashId;
    const inTrash = !isTrashDir && cluster._inTrash(ino);
    let body = '';

    if (ino.type === 'file') {
      const locs = cluster.blockLocations(ino);
      const repSel = inTrash
        ? ino.replication
        : `<select data-change="setrep" aria-label="Fator de replicação">${Array.from({ length: cluster.nodes.length }, (_, i) => i + 1)
            .map((n) => `<option${n === ino.replication ? ' selected' : ''}>${n}</option>`)
            .join('')}</select>`;
      body +=
        `<div class="title"><i class="dot big" style="background:${colorOf(ino.id)}"></i><div><strong>${esc(ino.name)}</strong><div class="path">${esc(path)}</div></div></div>` +
        `<dl class="kv"><dt>Tamanho</dt><dd>${fmtBytes(ino.size)} (${ino.size} bytes)</dd>` +
        `<dt>Versão</dt><dd>v${ino.version}</dd>` +
        `<dt>Réplicas por bloco</dt><dd>${repSel}</dd>` +
        `<dt>Criado</dt><dd>${fmtTime(ino.ctime)}</dd><dt>Modificado</dt><dd>${fmtTime(ino.mtime)}</dd></dl>`;
      if (locs.length) {
        body +=
          `<table class="blk"><thead><tr><th>Bloco</th><th>Id</th><th>Tam.</th><th>CRC32</th><th>Réplicas</th></tr></thead><tbody>` +
          locs
            .map(
              (b) =>
                `<tr><td>${b.index}</td><td class="mono">blk_${b.id}</td><td>${b.size} B</td><td class="mono">${hex(b.crc)}</td><td>${repChips(b.replicas) || '<span class="rep bad">nenhuma</span>'}</td></tr>`
            )
            .join('') +
          `</tbody></table>`;
      } else body += `<p class="note">Arquivo vazio: o NameNode guarda só o inode, nenhum bloco foi alocado.</p>`;
      body += '<div class="btns">';
      if (inTrash) body += btn('read', 'Ler') + btn('restore', 'Restaurar', 'primary') + btn('purge', 'Excluir definitivamente', 'danger');
      else
        body +=
          btn('read', 'Ler', 'primary') +
          btn('overwrite', 'Sobrescrever') +
          btn('replace', 'Substituir') +
          btn('rename', 'Renomear') +
          btn('move', 'Mover') +
          btn('copy', 'Copiar') +
          btn('delete', 'Apagar', 'danger');
      body += '</div>';
    } else {
      const n = ino.children.size;
      body +=
        `<div class="title">${isTrashDir ? ICON.trash : ICON.folder}<div><strong>${isRoot ? '/ (raiz)' : esc(ino.name)}</strong><div class="path">${esc(path)}</div></div></div>` +
        `<dl class="kv"><dt>Itens</dt><dd>${n}</dd><dt>Blocos dentro</dt><dd>${cluster._countBlocks(ino)}</dd>` +
        `<dt>Modificada</dt><dd>${fmtTime(ino.mtime)}</dd></dl>`;
      if (isTrashDir) body += `<p class="note">Itens apagados ficam aqui só como metadados. Os blocos continuam nos DataNodes até a exclusão definitiva.</p>`;
      body += '<div class="btns">';
      if (isTrashDir) body += btn('empty-trash', 'Esvaziar lixeira', 'danger');
      else if (inTrash) body += btn('restore', 'Restaurar', 'primary') + btn('purge', 'Excluir definitivamente', 'danger');
      else {
        body += btn('new-file', 'Novo arquivo aqui', 'primary') + btn('new-dir', 'Nova pasta aqui');
        if (!isRoot) body += btn('rename', 'Renomear') + btn('move', 'Mover') + btn('delete', 'Apagar', 'danger');
      }
      body += '</div>';
    }
    el.innerHTML = `<div class="panel-head"><h2>Detalhes</h2><span class="sub">${ino.type === 'file' ? 'arquivo' : 'pasta'}</span></div><div class="detail-body">${body}</div>`;
  }

  /* ---------------------------------------------------------------- cluster */
  function renderCluster() {
    const sel = ui.selected && cluster.inodes.get(ui.selected);
    const selFile = sel && sel.type === 'file' ? sel.id : null;
    const h = cluster.health();
    const cur = new Set();
    const anyWaiting = cluster.nodes.some((n) => !n.online && !n.dead);

    const nodes = cluster.nodes
      .map((n) => {
        const used = n.store.size;
        const slots = [];
        for (const bid of [...n.store.keys()].sort((a, b) => a - b)) {
          const rep = n.store.get(bid);
          const b = cluster.blocks.get(bid);
          const fid = b ? b.fileId : 0;
          const file = b ? cluster.inodes.get(fid) : null;
          const bad = crc32(rep.data) !== rep.crc;
          const key = n.id + ':' + bid;
          cur.add(key);
          const cls = ['slot'];
          if (bad) cls.push('bad');
          if (selFile) cls.push(fid === selFile ? 'sel' : 'dim');
          if (ui.prevSlots.size && !ui.prevSlots.has(key)) cls.push('pop');
          const tip = `blk_${bid} · ${file ? cluster.pathOf(file) : 'órfão'} · bloco ${b ? b.index : '?'} · ${rep.data.length} B · crc ${hex(rep.crc)}${bad ? ' · CORROMPIDO' : ''}`;
          slots.push(
            `<div class="${cls.join(' ')}" style="--c:${colorOf(fid)}" data-file="${fid}" data-tip="${esc(tip)}">${bad ? '<span>!</span>' : b ? b.index : ''}</div>`
          );
        }
        for (let i = used; i < n.capacity; i++) slots.push('<div class="slot free"></div>');
        const state = n.online ? 'online' : n.dead ? 'dead' : 'down';
        const stateTxt = n.online ? 'online' : n.dead ? 'declarado morto' : 'sem resposta';
        const pend = ui.pending.get(n.id);
        let foot;
        if (n.online) foot = `<button class="btn sm danger" data-action="kill" data-node="${n.id}">Derrubar</button>`;
        else {
          foot = `<button class="btn sm primary" data-action="revive" data-node="${n.id}">Reviver</button>`;
          if (!n.dead) foot += `<span class="note">${pend ? 'detectando falha…' : 'aguardando detecção'}</span>`;
          else foot += `<span class="note">réplicas já re-criadas</span>`;
        }
        const wait =
          pend && !n.online && !n.dead
            ? `<div class="hb-wait" title="timeout de heartbeat"><i style="animation-duration:${CFG.timeoutHeartbeat}ms;animation-delay:-${Date.now() - pend.t0}ms"></i></div>`
            : '';
        return (
          `<div class="dn ${state}"><div class="dn-head"><strong>${n.id}</strong><span class="rack">${n.rack}</span><span class="state">${stateTxt}</span></div>` +
          `<div class="usage"><i style="width:${Math.min(100, (used / n.capacity) * 100)}%"></i></div>` +
          `<div class="cap"><span>${used} de ${n.capacity} blocos</span><span>${n.dead ? 'disco preservado' : ''}</span></div>` +
          `<div class="slots">${slots.join('')}</div>${wait}<div class="dn-foot">${foot}</div></div>`
        );
      })
      .join('');
    ui.prevSlots = cur;

    $('#cluster').innerHTML =
      `<div class="panel-head"><h2>Cluster de DataNodes</h2><div class="actions">` +
      `<label class="switch" title="Se desligada, use “Forçar detecção” para simular o fim do timeout"><input type="checkbox" data-change="auto"${ui.auto ? ' checked' : ''}> recuperação automática</label>` +
      (anyWaiting && !ui.auto ? btn('force', 'Forçar detecção e recuperação', 'primary') : '') +
      btn('add-node', '+ DataNode', '') +
      btn('rebalance', 'Rebalancear') +
      btn('fsck', 'Verificar (fsck)') +
      btn('fsck-repair', 'Verificar e reparar') +
      `</div></div>` +
      `<div class="cluster-body"><div class="namenode"><strong>NameNode</strong>` +
      `<span class="nn-stat"><b>${cluster.inodes.size}</b>inodes</span>` +
      `<span class="nn-stat"><b>${h.blocks}</b>blocos no mapa</span>` +
      `<span class="nn-stat"><b>${cluster.config.replication}</b>réplicas por padrão</span>` +
      `<span class="nn-stat"><b>${cluster.config.blockSize} B</b>por bloco</span>` +
      `<span class="nn-note">guarda só metadados e a localização dos blocos</span></div>` +
      `<div class="nodes">${nodes}</div></div>`;
  }

  /* ---------------------------------------------------------------- mapa de blocos */
  function renderBlocksTab() {
    const rows = [...cluster.blocks.values()].sort((a, b) => a.id - b.id);
    if (!rows.length) {
      $('#blocks-out').innerHTML = `<p class="empty">O mapa está vazio: ainda não há blocos alocados.</p>`;
      return;
    }
    const html = rows
      .slice(0, 400)
      .map((b) => {
        const file = cluster.inodes.get(b.fileId);
        const hh = cluster.blockHealth(b);
        const reps = [...b.replicas]
          .map((id) => cluster._node(id))
          .filter(Boolean)
          .sort((x, y) => x.num - y.num)
          .map((n) => {
            const r = n.store.get(b.id);
            const ok = r && crc32(r.data) === r.crc;
            return { node: n.id, online: n.online, ok };
          });
        const state = hh.valid === 0 ? 'indisponível' : hh.valid < hh.target ? `sub-replicado ${hh.valid}/${hh.target}` : 'ok';
        return `<tr><td class="mono">blk_${b.id}</td><td>${file ? esc(cluster.pathOf(file)) : '—'} <span style="opacity:.6">[${b.index}]</span></td><td>${b.size} B</td><td class="mono">${hex(b.crc)}</td><td>${repChips(reps)}</td><td>${state}</td></tr>`;
      })
      .join('');
    $('#blocks-out').innerHTML =
      `<table><thead><tr><th>Bloco</th><th>Arquivo [índice]</th><th>Tam.</th><th>CRC32</th><th>DataNodes com réplica</th><th>Estado</th></tr></thead><tbody>${html}</tbody></table>`;
  }

  /* ================================================================ abas */
  function setTab(name) {
    ui.tab = name;
    $$('.tab').forEach((t) => t.setAttribute('aria-selected', String(t.dataset.tab === name)));
    $('#pane-console').hidden = name !== 'console';
    $('#pane-log').hidden = name !== 'log';
    $('#pane-blocks').hidden = name !== 'blocks';
    if (name === 'blocks') renderBlocksTab();
    if (name === 'console') $('#con-in').focus();
    if (name === 'log') {
      const o = $('#log-out');
      o.scrollTop = o.scrollHeight;
    }
  }

  /* ================================================================ avisos, tooltip, modal */
  function toast(msg, kind) {
    const t = document.createElement('div');
    t.className = 'toast ' + (kind || '');
    t.textContent = msg;
    $('#toasts').appendChild(t);
    setTimeout(() => t.remove(), kind === 'bad' ? 6500 : 3800);
  }

  const ERR_LABEL = {
    ENOENT: 'Não encontrado',
    EEXIST: 'Já existe',
    ENOTDIR: 'Não é uma pasta',
    EISDIR: 'É uma pasta',
    ENOTEMPTY: 'Pasta não vazia',
    ENOSPC: 'Sem espaço',
    EINVAL: 'Valor inválido',
    EPERM: 'Operação não permitida',
    EIO: 'Dados indisponíveis'
  };
  function errText(e) {
    if (e instanceof DFSError) return `${ERR_LABEL[e.code] || e.code}: ${e.detail}`;
    return 'Erro inesperado: ' + (e && e.message ? e.message : e);
  }

  function openModal(title, bodyHtml, wide) {
    const m = $('#modal');
    m.innerHTML = `<div class="dlg${wide ? ' wide' : ''}" role="dialog" aria-modal="true" aria-label="${esc(title)}"><header><h2>${esc(title)}</h2><button class="x" data-action="close-modal" aria-label="Fechar">×</button></header><div class="dlg-body">${bodyHtml}</div></div>`;
    m.hidden = false;
    const first = $('input:not([type=file]):not([type=checkbox]), textarea, select, button.primary', m);
    if (first) setTimeout(() => first.focus(), 0);
  }
  function closeModal() {
    const m = $('#modal');
    m.hidden = true;
    m.innerHTML = '';
  }

  function fieldHtml(f) {
    const id = 'f_' + f.name;
    const hint = f.hint ? `<span class="hint">${esc(f.hint)}</span>` : '';
    let input;
    if (f.type === 'textarea') input = `<textarea id="${id}" name="${f.name}" rows="${f.rows || 6}" spellcheck="false">${esc(f.value || '')}</textarea>`;
    else if (f.type === 'select')
      input = `<select id="${id}" name="${f.name}">${f.options.map((o) => `<option value="${esc(o.v)}"${o.v === f.value ? ' selected' : ''}>${esc(o.l)}</option>`).join('')}</select>`;
    else if (f.type === 'file') input = `<input id="${id}" name="${f.name}" type="file">`;
    else if (f.type === 'checkbox')
      return `<div class="f"><label class="chk"><input type="checkbox" name="${f.name}"${f.value ? ' checked' : ''}> ${esc(f.label)}</label></div>`;
    else input = `<input id="${id}" name="${f.name}" type="${f.type || 'text'}" value="${esc(f.value || '')}"${f.min != null ? ` min="${f.min}"` : ''}${f.max != null ? ` max="${f.max}"` : ''} autocomplete="off">`;
    return `<div class="f"><label for="${id}">${esc(f.label)}${hint}</label>${input}</div>`;
  }

  /** Modal com formulário. onSubmit pode ser async e lançar erro: a mensagem aparece no próprio modal. */
  function formModal({ title, fields, submit, note, onSubmit, onMount }) {
    const html = `<form class="frm" novalidate>${note ? `<p class="note">${note}</p>` : ''}${fields.map(fieldHtml).join('')}<p class="err" role="alert" hidden></p><div class="btns"><button type="button" class="btn" data-action="close-modal">Cancelar</button><button type="submit" class="btn primary">${esc(submit)}</button></div></form>`;
    openModal(title, html);
    const form = $('#modal form');
    if (onMount) onMount(form);
    form.addEventListener('submit', async (ev) => {
      ev.preventDefault();
      const vals = {};
      for (const f of fields) {
        const el = form.elements[f.name];
        vals[f.name] = f.type === 'file' ? (el.files && el.files[0]) || null : f.type === 'checkbox' ? el.checked : el.value;
      }
      const err = $('.err', form);
      try {
        await onSubmit(vals);
        closeModal();
      } catch (e) {
        err.textContent = errText(e);
        err.hidden = false;
        afterChange();
      }
    });
  }

  function confirmModal({ title, text, label, danger, onConfirm }) {
    openModal(title, `<form class="frm"><p style="margin:0 0 14px">${text}</p><p class="err" role="alert" hidden></p><div class="btns"><button type="button" class="btn" data-action="close-modal">Cancelar</button><button type="submit" class="btn ${danger ? 'danger' : 'primary'}">${esc(label)}</button></div></form>`);
    const form = $('#modal form');
    form.addEventListener('submit', (ev) => {
      ev.preventDefault();
      try {
        onConfirm();
        closeModal();
      } catch (e) {
        const err = $('.err', form);
        err.textContent = errText(e);
        err.hidden = false;
        afterChange();
      }
    });
  }

  /* ---------------------------------------------------------------- listas para formulários */
  function allDirs(excludeId) {
    const out = [];
    const walk = (dir) => {
      out.push({ v: cluster.pathOf(dir), l: cluster.pathOf(dir) });
      for (const k of cluster.children(dir)) if (k.type === 'dir' && k.id !== cluster.trashId && k.id !== excludeId) walk(k);
    };
    walk(cluster.inodes.get(cluster.rootId));
    return out;
  }
  function allFiles(excludeId) {
    const out = [];
    const walk = (dir) => {
      for (const k of cluster.children(dir)) {
        if (k.id === cluster.trashId) continue;
        if (k.type === 'dir') walk(k);
        else if (k.id !== excludeId) out.push({ v: cluster.pathOf(k), l: cluster.pathOf(k) });
      }
    };
    walk(cluster.inodes.get(cluster.rootId));
    return out;
  }
  const fileBytes = async (f) => new Uint8Array(await f.arrayBuffer());

  function selectIno(id) {
    ui.selected = id;
    const ino = cluster.inodes.get(id);
    if (ino) {
      expandTo(ino);
      const dir = ino.type === 'dir' ? ino : cluster.inodes.get(ino.parent);
      cluster.cwd = cluster.pathOf(dir);
    }
  }
  function expandTo(ino) {
    for (let c = cluster.inodes.get(ino.parent); c; c = c.parent === null ? null : cluster.inodes.get(c.parent)) ui.expanded.add(c.id);
  }

  /* ================================================================ ações da interface */
  const selIno = () => (ui.selected ? cluster.inodes.get(ui.selected) : null);

  function actNewFile() {
    const dirs = allDirs();
    formModal({
      title: 'Novo arquivo',
      submit: 'Gravar no DFS',
      note: `O texto será dividido em blocos de <b>${cluster.config.blockSize} B</b> e cada bloco gravado em <b>${cluster.config.replication}</b> DataNodes.`,
      fields: [
        { name: 'dir', label: 'Pasta', type: 'select', options: dirs, value: targetDir() },
        { name: 'name', label: 'Nome do arquivo', value: 'novo.txt' },
        { name: 'content', label: 'Conteúdo', type: 'textarea', value: '', rows: 6 },
        { name: 'upload', label: 'Ou envie um arquivo do computador', type: 'file', hint: 'Se escolher um arquivo, ele é usado no lugar do texto acima.' }
      ],
      onMount(form) {
        form.elements.upload.addEventListener('change', () => {
          const f = form.elements.upload.files[0];
          if (f && form.elements.name.value === 'novo.txt') form.elements.name.value = f.name;
        });
      },
      async onSubmit(v) {
        const path = joinPath(v.dir, v.name.trim());
        const data = v.upload ? await fileBytes(v.upload) : v.content;
        const ino = cluster.write(path, data);
        selectIno(ino.id);
        afterChange();
        toast(`Gravado ${path}: ${ino.blocks.length} bloco(s) × ${ino.replication} réplicas`, 'ok');
      }
    });
  }

  function actNewDir() {
    formModal({
      title: 'Nova pasta',
      submit: 'Criar pasta',
      note: 'Criar pasta só altera o namespace no NameNode; nenhum DataNode participa.',
      fields: [
        { name: 'dir', label: 'Dentro de', type: 'select', options: allDirs(), value: targetDir() },
        { name: 'name', label: 'Nome da pasta', value: '' }
      ],
      onSubmit(v) {
        const path = joinPath(v.dir, v.name.trim());
        const ino = cluster.mkdir(path);
        selectIno(ino.id);
        afterChange();
        toast(`Pasta criada: ${path}`, 'ok');
      }
    });
  }

  function isBinary(bytes) {
    try {
      new TextDecoder('utf-8', { fatal: true }).decode(bytes);
      return bytes.includes(0);
    } catch (e) {
      return true;
    }
  }

  function actRead() {
    const ino = selIno();
    if (!ino || ino.type !== 'file') return;
    const path = cluster.pathOf(ino);
    let r;
    try {
      r = cluster.read(path);
    } catch (e) {
      afterChange();
      toast(errText(e), 'bad');
      return;
    }
    afterChange();
    let content;
    if (isBinary(r.bytes)) {
      const preview = Array.from(r.bytes.slice(0, 96)).map((b) => b.toString(16).padStart(2, '0')).join(' ');
      content = `<div class="binary">Arquivo binário (${r.bytes.length} bytes). Primeiros bytes:<br>${preview}${r.bytes.length > 96 ? ' …' : ''}</div>`;
    } else {
      const dec = new TextDecoder('utf-8');
      const parts = r.chunks.map((c, i) => dec.decode(c.bytes, { stream: i < r.chunks.length - 1 }));
      content = `<div class="chunks">${
        r.chunks.length
          ? r.chunks
              .map((c, i) => (parts[i] ? `<span class="chunk" style="--c:${colorOf(ino.id)}" title="blk_${c.blockId} lido de ${c.nodeId}">${esc(parts[i])}</span>` : ''))
              .join('')
          : '<em>(arquivo vazio)</em>'
      }</div>`;
    }
    const rows = r.chunks
      .map((c) => `<tr><td>${c.index}</td><td><code>blk_${c.blockId}</code></td><td>${c.bytes.length} B</td><td><span class="rep">${c.nodeId}</span></td></tr>`)
      .join('');
    openModal(
      'Ler ' + ino.name,
      `<div class="readview"><p class="meta">${r.bytes.length} bytes remontados a partir de ${r.chunks.length} bloco(s). Cada trecho sublinhado é um bloco; passe o mouse para ver de qual DataNode ele veio.</p>${content}` +
        (rows ? `<table><thead><tr><th>Bloco</th><th>Id</th><th>Tam.</th><th>Lido de</th></tr></thead><tbody>${rows}</tbody></table>` : '') +
        `<div class="btns" style="margin-top:14px"><button class="btn" data-action="download">Baixar arquivo</button><button class="btn primary" data-action="close-modal">Fechar</button></div></div>`,
      true
    );
    ui.lastRead = { name: ino.name, bytes: r.bytes };
  }

  function actDownload() {
    if (!ui.lastRead) return;
    const url = URL.createObjectURL(new Blob([ui.lastRead.bytes]));
    const a = document.createElement('a');
    a.href = url;
    a.download = ui.lastRead.name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  function actOverwrite() {
    const ino = selIno();
    if (!ino || ino.type !== 'file') return;
    const path = cluster.pathOf(ino);
    let current = '';
    let binary = false;
    try {
      const b = cluster.peek(path);
      binary = isBinary(b);
      current = binary ? '' : new TextDecoder().decode(b);
    } catch (e) {
      /* sem réplica disponível: começa vazio */
    }
    formModal({
      title: 'Sobrescrever ' + ino.name,
      submit: 'Sobrescrever',
      note: 'O novo conteúdo é gravado em blocos novos primeiro; só depois o NameNode troca a lista de blocos (versão +1) e libera os antigos.' + (binary ? ' O arquivo atual é binário, então o campo começa vazio.' : ''),
      fields: [
        { name: 'content', label: 'Novo conteúdo', type: 'textarea', value: current, rows: 8 },
        { name: 'upload', label: 'Ou envie um arquivo do computador', type: 'file' }
      ],
      async onSubmit(v) {
        const data = v.upload ? await fileBytes(v.upload) : v.content;
        const f = cluster.overwrite(path, data);
        afterChange();
        toast(`${path} sobrescrito: v${f.version}, ${f.blocks.length} bloco(s)`, 'ok');
      }
    });
  }

  function actReplace() {
    const ino = selIno();
    if (!ino || ino.type !== 'file') return;
    const path = cluster.pathOf(ino);
    const others = allFiles(ino.id);
    formModal({
      title: 'Substituir ' + ino.name,
      submit: 'Substituir',
      note: 'O arquivo mantém nome e posição, e passa a ter o conteúdo de outro arquivo. O de origem continua existindo.',
      fields: [
        { name: 'src', label: 'Substituir pelo conteúdo de (arquivo do DFS)', type: 'select', options: [{ v: '', l: '— escolher —' }].concat(others), value: '' },
        { name: 'upload', label: 'Ou por um arquivo do computador', type: 'file' }
      ],
      async onSubmit(v) {
        if (!v.upload && !v.src) throw new DFSError('EINVAL', 'escolha um arquivo do DFS ou do seu computador');
        const f = v.upload ? cluster.overwrite(path, await fileBytes(v.upload)) : cluster.replace(path, v.src);
        afterChange();
        toast(`${path} substituído: v${f.version}`, 'ok');
      }
    });
  }

  function actRename() {
    const ino = selIno();
    if (!ino) return;
    const path = cluster.pathOf(ino);
    formModal({
      title: 'Renomear ' + ino.name,
      submit: 'Renomear',
      note: 'Renomear só muda o nome no NameNode. Nenhum bloco é movido ou copiado.',
      fields: [{ name: 'name', label: 'Novo nome', value: ino.name }],
      onSubmit(v) {
        cluster.rename(path, v.name.trim());
        afterChange();
        toast(`Renomeado para ${v.name.trim()}`, 'ok');
      }
    });
  }

  function actMove() {
    const ino = selIno();
    if (!ino) return;
    const path = cluster.pathOf(ino);
    const dirs = allDirs(ino.type === 'dir' ? ino.id : null);
    formModal({
      title: 'Mover ' + ino.name,
      submit: 'Mover',
      note: 'Mover só troca a pasta no NameNode. Os blocos continuam nos mesmos DataNodes.',
      fields: [
        { name: 'dir', label: 'Pasta de destino', type: 'select', options: dirs, value: '/' },
        { name: 'force', label: 'Substituir se já existir um arquivo com o mesmo nome', type: 'checkbox', value: false }
      ],
      onSubmit(v) {
        cluster.mv(path, v.dir, { force: v.force });
        afterChange();
        toast(`Movido para ${v.dir}`, 'ok');
      }
    });
  }

  function actCopy() {
    const ino = selIno();
    if (!ino || ino.type !== 'file') return;
    const path = cluster.pathOf(ino);
    formModal({
      title: 'Copiar ' + ino.name,
      submit: 'Copiar',
      note: 'Diferente de mover, copiar lê os blocos e grava blocos novos: ocupa espaço de verdade.',
      fields: [
        { name: 'dir', label: 'Pasta de destino', type: 'select', options: allDirs(), value: targetDir() },
        { name: 'name', label: 'Nome da cópia', value: 'copia-' + ino.name }
      ],
      onSubmit(v) {
        const dest = joinPath(v.dir, v.name.trim());
        const c = cluster.cp(path, dest);
        selectIno(c.id);
        afterChange();
        toast(`Copiado para ${dest}`, 'ok');
      }
    });
  }

  function actDelete() {
    const ino = selIno();
    if (!ino) return;
    const path = cluster.pathOf(ino);
    confirmModal({
      title: 'Apagar ' + ino.name,
      text: `Mover <b>${esc(path)}</b> para a lixeira? Os blocos continuam armazenados e você pode restaurar depois.`,
      label: 'Mover para a lixeira',
      danger: true,
      onConfirm() {
        cluster.rm(path, { recursive: true });
        ui.selected = null;
        afterChange();
        toast('Movido para a lixeira', 'ok');
      }
    });
  }

  function actPurge() {
    const ino = selIno();
    if (!ino) return;
    const path = cluster.pathOf(ino);
    confirmModal({
      title: 'Excluir definitivamente',
      text: `Excluir <b>${esc(path)}</b> para sempre? As réplicas serão removidas dos DataNodes e isso não pode ser desfeito.`,
      label: 'Excluir definitivamente',
      danger: true,
      onConfirm() {
        cluster.rm(path, { recursive: true, permanent: true });
        ui.selected = null;
        afterChange();
        toast('Excluído definitivamente', 'ok');
      }
    });
  }

  function actRestore() {
    const ino = selIno();
    if (!ino) return;
    guard(() => {
      const r = cluster.restore(cluster.pathOf(ino));
      selectIno(r.id);
      return `Restaurado em ${cluster.pathOf(r)}`;
    });
  }

  /** Executa uma ação simples, mostra aviso de sucesso/erro e atualiza a tela. */
  function guard(fn) {
    try {
      const msg = fn();
      afterChange();
      if (msg) toast(msg, 'ok');
    } catch (e) {
      afterChange();
      toast(errText(e), 'bad');
    }
  }

  function actSettings() {
    formModal({
      title: 'Configurações do cluster',
      submit: 'Aplicar',
      note: 'Valores didáticos: no HDFS real o bloco tem 128 MB e a replicação padrão é 3. Aqui o bloco é pequeno para você enxergar cada pedaço. Tamanho de bloco e replicação valem para os próximos arquivos.',
      fields: [
        { name: 'block', label: 'Tamanho do bloco (bytes)', type: 'select', value: String(cluster.config.blockSize), options: [8, 16, 32, 64, 128, 256, 1024, 4096].map((n) => ({ v: String(n), l: n + ' B' })) },
        { name: 'rep', label: 'Replicação padrão dos novos arquivos', type: 'number', value: String(cluster.config.replication), min: 1, max: 8 },
        { name: 'cap', label: 'Capacidade de cada DataNode (blocos)', type: 'number', value: String(cluster.config.nodeCapacity), min: 4, max: 128 }
      ],
      onSubmit(v) {
        cluster.setBlockSize(v.block);
        cluster.setDefaultReplication(v.rep);
        cluster.setCapacity(v.cap);
        afterChange();
        toast('Configurações aplicadas', 'ok');
      }
    });
  }

  function resetCluster() {
    clearAllPending();
    cluster.reset();
    ui.selected = null;
    ui.expanded = new Set([cluster.rootId]);
    ui.prevSlots = new Set();
    afterChange();
  }

  function actReset() {
    confirmModal({
      title: 'Reiniciar cluster',
      text: 'Apaga todos os arquivos e pastas e volta aos DataNodes iniciais. Os tamanhos de bloco e replicação configurados são mantidos.',
      label: 'Reiniciar',
      danger: true,
      onConfirm() {
        resetCluster();
        conPrint('cluster reiniciado', 'dim');
        toast('Cluster reiniciado', 'ok');
      }
    });
  }

  function actAbout() {
    const dn = (x, y, name, blocks) =>
      `<g><rect class="box" x="${x}" y="${y}" width="118" height="74" rx="9"/><text x="${x + 59}" y="${y + 22}" text-anchor="middle" font-size="14" font-weight="700">${name}</text>` +
      blocks
        .map((c, i) => `<rect x="${x + 14 + (i % 4) * 22}" y="${y + 36 + Math.floor(i / 4) * 17}" width="18" height="13" rx="3" fill="${c}"/>`)
        .join('') +
      `</g>`;
    const svg = `<svg viewBox="0 0 680 290" role="img" aria-label="Diagrama: o cliente pergunta ao NameNode onde estão os blocos e depois lê e grava direto nos DataNodes">
      <defs>
        <marker id="ah" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="#2446e0"/></marker>
        <marker id="ah2" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="#6f7c9c"/></marker>
      </defs>
      <rect class="box" x="10" y="14" width="100" height="64" rx="10"/>
      <text x="60" y="42" text-anchor="middle" font-size="15" font-weight="700">Cliente</text>
      <text x="60" y="61" text-anchor="middle" class="sm">app ou usuário</text>

      <rect class="box nn" x="270" y="10" width="230" height="70" rx="10"/>
      <text class="nn-t" x="385" y="38" text-anchor="middle" font-size="15" font-weight="700">NameNode</text>
      <text class="nn-t" x="385" y="58" text-anchor="middle" font-size="12">árvore, inodes e mapa de blocos</text>

      <path class="arrow" d="M110 34 H 268"/>
      <text class="sm" x="189" y="27" text-anchor="middle">1. onde gravar ou ler?</text>
      <path class="arrow dash" d="M268 60 H 112"/>
      <text class="sm" x="189" y="76" text-anchor="middle">2. lista de blocos e nós</text>

      <path class="arrow" d="M60 80 V 205 H 188"/>
      <text class="sm" x="70" y="150">3. blocos vão direto</text>
      <text class="sm" x="70" y="166">para os DataNodes</text>

      ${dn(190, 168, 'dn1', ['#3b6cff', '#e8478c', '#3b6cff', '#12a594'])}
      ${dn(330, 168, 'dn2', ['#3b6cff', '#e8478c', '#12a594', '#e8478c'])}
      ${dn(470, 168, 'dn3', ['#e8478c', '#3b6cff', '#12a594', '#12a594'])}

      <path class="arrow dash" d="M249 166 L 310 82"/>
      <path class="arrow dash" d="M389 166 V 82"/>
      <path class="arrow dash" d="M529 166 L 460 82"/>
      <text class="sm" x="672" y="124" text-anchor="end">heartbeat e block report</text>

      <text class="sm" x="190" y="270">Cada cor é um arquivo; o mesmo bloco aparece em vários DataNodes (réplicas).</text>
    </svg>`;
    const ops = [
      ['Escrever', 'cria o inode e aloca os blocos', 'grava cada bloco em N DataNodes (pipeline)'],
      ['Ler', 'devolve a lista de blocos e onde estão', 'lê uma réplica íntegra por bloco; confere o CRC32; troca de réplica se falhar'],
      ['Sobrescrever', 'troca a lista de blocos do inode e sobe a versão', 'grava os blocos novos antes e só então libera os antigos (atomicidade)'],
      ['Anexar', 'acrescenta blocos ao fim do inode', 'grava só os blocos novos; os antigos são imutáveis'],
      ['Substituir', 'igual a sobrescrever, com o conteúdo de outro arquivo', 'lê a origem e grava blocos novos no destino'],
      ['Renomear', 'muda o nome no namespace', 'nada: nenhum bloco se move'],
      ['Mover', 'muda a pasta pai no namespace', 'nada: nenhum bloco se move'],
      ['Copiar', 'cria um inode novo', 'cria blocos novos (cópia real, ocupa espaço)'],
      ['Apagar', 'move o inode para /.lixeira', 'nada: blocos ficam guardados para restaurar'],
      ['Excluir de vez', 'remove o inode', 'remove as réplicas dos DataNodes']
    ];
    openModal(
      'Como o Mini-DFS funciona',
      `<div class="about">
        <h3>Arquitetura</h3>
        <p>Um sistema de arquivos distribuído espalha os dados por vários servidores. Aqui há <b>um NameNode</b>, que só guarda metadados, e vários <b>DataNodes</b>, que guardam os blocos. O cliente pergunta ao NameNode <i>onde</i> ficam os dados e depois fala direto com os DataNodes.</p>
        ${svg}
        <h3>O que cada operação faz</h3>
        <table><thead><tr><th>Operação</th><th>No NameNode (metadados)</th><th>Nos DataNodes (dados)</th></tr></thead><tbody>${ops.map((o) => `<tr><td>${o[0]}</td><td>${o[1]}</td><td>${o[2]}</td></tr>`).join('')}</tbody></table>
        <h3>Falhas e recuperação</h3>
        <ul>
          <li><b>Replicação:</b> cada bloco existe em N DataNodes, espalhados entre racks. Perder um nó não perde o arquivo.</li>
          <li><b>Heartbeat:</b> se um DataNode some, o NameNode espera o timeout, declara o nó morto e <b>re-replica</b> os blocos que ficaram abaixo do alvo.</li>
          <li><b>Checksum:</b> cada bloco guarda um CRC32. Se uma réplica estiver corrompida, a leitura usa outra e a réplica ruim é refeita. O <code>fsck</code> varre tudo.</li>
          <li><b>Block report:</b> quando um nó volta, ele lista o que tem. O NameNode descarta o que foi apagado e remove réplicas em excesso.</li>
          <li><b>Lixeira:</b> apagar é reversível até a exclusão definitiva.</li>
        </ul>
        <h3>Nomes equivalentes em outros DFS</h3>
        <table><thead><tr><th>Mini-DFS</th><th>HDFS</th><th>GFS</th><th>DFS da Microsoft</th></tr></thead><tbody>
          <tr><td>NameNode</td><td>NameNode</td><td>master</td><td>servidor de namespace (DFS Namespaces)</td></tr>
          <tr><td>DataNode</td><td>DataNode</td><td>chunkserver</td><td>servidor de destino (folder target)</td></tr>
          <tr><td>bloco</td><td>block (128 MB)</td><td>chunk (64 MB)</td><td>arquivo inteiro replicado (DFS Replication)</td></tr>
        </tbody></table>
        <h3>Limites desta simulação</h3>
        <p>Os DataNodes são estruturas em memória no navegador, sem rede real. O estado fica salvo no <code>localStorage</code> do navegador. Não há permissões, quotas, concorrência entre vários clientes nem NameNode secundário.</p>
      </div>`,
      true
    );
  }

  /* ================================================================ console */
  function conPrint(html, cls) {
    const out = $('#con-out');
    const d = document.createElement('div');
    d.className = 'cl ' + (cls || '');
    d.innerHTML = html;
    out.appendChild(d);
    while (out.children.length > 800) out.removeChild(out.firstChild);
    out.scrollTop = out.scrollHeight;
  }
  const conText = (text, cls) => conPrint(esc(text), cls);

  function conEvents(evs) {
    for (const ev of evs) {
      if (ev.actor === 'cliente' && ev.level === 'info') continue; // já aparece no eco do comando
      const a = actorInfo(ev.actor);
      conPrint(`<span class="a-${a.kind}">${esc(a.label)}</span>  ${esc(ev.msg)}`, 'ev ' + ev.level);
    }
  }

  function updatePrompt() {
    const p = $('#con-prompt');
    if (p) p.textContent = 'dfs:' + cluster.cwd + '$';
  }

  function tokenize(line) {
    const out = [];
    let cur = '';
    let q = null;
    let has = false;
    let quoted = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (q) {
        if (ch === '\\' && i + 1 < line.length) {
          const nx = line[++i];
          cur += nx === 'n' ? '\n' : nx === 't' ? '\t' : nx;
        } else if (ch === q) q = null;
        else cur += ch;
      } else if (ch === '"' || ch === "'") {
        q = ch;
        has = true;
        quoted = true;
      } else if (/\s/.test(ch)) {
        if (has) {
          out.push({ v: cur, q: quoted });
          cur = '';
          has = false;
          quoted = false;
        }
      } else {
        cur += ch;
        has = true;
      }
    }
    if (q) throw new DFSError('EINVAL', 'aspas não fechadas');
    if (has) out.push({ v: cur, q: quoted });
    return out;
  }

  function parseArgs(tokens) {
    const flags = new Set();
    const args = [];
    for (const t of tokens) {
      if (!t.q && /^-{1,2}[a-zA-Z]/.test(t.v)) {
        if (t.v.startsWith('--')) flags.add(t.v.slice(2));
        else for (const ch of t.v.slice(1)) flags.add(ch);
      } else args.push(t.v);
    }
    return { flags, args };
  }

  const HELP = [
    ['Arquivos', null],
    ['  write <caminho> <texto>', 'escrever: cria um arquivo (falha se já existir)'],
    ['  cat <caminho>', 'ler'],
    ['  overwrite <caminho> <texto>', 'sobrescrever o conteúdo'],
    ['  append <caminho> <texto>', 'anexar ao final'],
    ['  replace <destino> <origem>', 'substituir o conteúdo do destino pelo da origem'],
    ['  rename <caminho> <novo-nome>', 'renomear (só metadados)'],
    ['  mv [-f] <origem> <destino>', 'mover (-f substitui um arquivo existente)'],
    ['  cp <origem> <destino>', 'copiar'],
    ['  rm [-r] [-f] <caminho>', 'apagar: vai para a lixeira (-f exclui de vez)'],
    ['  restore <caminho>', 'restaurar um item de /.lixeira'],
    ['  emptytrash', 'esvaziar a lixeira'],
    ['Navegação', null],
    ['  ls [caminho]  tree [caminho]', 'listar'],
    ['  cd <pasta>  pwd  mkdir [-p] <pasta>', ''],
    ['  stat <caminho>', 'metadados, blocos e réplicas'],
    ['Cluster', null],
    ['  nodes', 'estado dos DataNodes'],
    ['  kill <dnX>  revive <dnX>', 'derrubar / reviver um DataNode'],
    ['  addnode  rebalance', 'adicionar DataNode / equilibrar blocos'],
    ['  setrep <n> <caminho>', 'fator de replicação de um arquivo'],
    ['  fsck [--repair]', 'verifica checksums e réplicas (e conserta)'],
    ['  corrupt <caminho>', 'corrompe uma réplica (ou: corrupt blk_N dnX)'],
    ['  set block|rep|cap <n>', 'tamanho do bloco, replicação padrão, capacidade'],
    ['  reset  clear  help', '']
  ];

  function lsLine(ino) {
    const isDir = ino.type === 'dir';
    const h = isDir ? null : cluster.fileHealth(ino);
    return [
      isDir ? 'pasta' : 'arq  ',
      (isDir ? '-' : fmtBytes(ino.size)).padStart(9),
      (isDir ? '-' : String(ino.blocks.length)).padStart(6),
      (h ? h.valid + '/' + h.target : '-').padStart(8),
      (isDir ? '-' : 'v' + ino.version).padStart(5),
      fmtTime(ino.mtime),
      ino.name + (isDir ? '/' : '')
    ].join('  ');
  }

  function treeLines(ino, prefix, out, isRoot) {
    const kids = cluster.children(ino);
    kids.forEach((k, i) => {
      const last = i === kids.length - 1;
      out.push(prefix + (last ? '└── ' : '├── ') + k.name + (k.type === 'dir' ? '/' : ''));
      if (k.type === 'dir') treeLines(k, prefix + (last ? '    ' : '│   '), out);
    });
  }

  function repText(list) {
    return list.map((r) => r.node + (!r.online ? '(offline)' : !r.ok ? '(corrompida)' : '')).join(' ') || '(nenhuma)';
  }

  function holderOf(path) {
    const f = cluster.resolve(path);
    return [...cluster.blocks.get(f.blocks[0]).replicas][0];
  }

  /** Executa um comando. Devolve linhas [texto, classe]. Erros são lançados. */
  function runCommand(name, flags, args) {
    const out = [];
    const P = (t, c) => out.push([t, c || 'out']);
    const need = (n, usage) => {
      if (args.length < n) throw new DFSError('EINVAL', 'uso: ' + usage);
    };
    const rest = (from) => args.slice(from).join(' ');
    switch (name) {
      case 'help':
      case 'ajuda':
        P('Comandos disponíveis (use aspas para textos com espaços; \\n vira quebra de linha):', 'dim');
        for (const [a, b] of HELP) P(b === null ? '\n' + a : a.padEnd(38) + (b || ''), b === null ? 'ok' : 'out');
        break;
      case 'clear':
        $('#con-out').innerHTML = '';
        break;
      case 'pwd':
        P(cluster.cwd);
        break;
      case 'cd': {
        const d = cluster.cd(args[0] || '/');
        selectIno(cluster.resolve(d).id);
        break;
      }
      case 'ls': {
        const ino = cluster.resolve(args[0] || '.');
        P('tipo   tamanho  blocos  réplicas  vers.  modificado  nome', 'dim');
        if (ino.type === 'dir') cluster.children(ino).forEach((k) => P(lsLine(k)));
        else P(lsLine(ino));
        if (ino.type === 'dir' && !ino.children.size) P('(vazia)', 'dim');
        break;
      }
      case 'tree': {
        const ino = cluster.resolve(args[0] || '.');
        P(cluster.pathOf(ino) + (ino.type === 'dir' ? '/' : ''));
        if (ino.type === 'dir') {
          const lines = [];
          treeLines(ino, '', lines);
          lines.forEach((l) => P(l));
        }
        break;
      }
      case 'mkdir': {
        need(1, 'mkdir [-p] <pasta>');
        const d = cluster.mkdir(args[0], { parents: flags.has('p') });
        expandTo(d);
        break;
      }
      case 'write':
      case 'touch': {
        need(1, 'write <caminho> <texto>');
        const f = cluster.write(args[0], name === 'touch' ? '' : rest(1));
        selectIno(f.id);
        P(`gravado ${cluster.pathOf(f)}: ${f.size} B em ${f.blocks.length} bloco(s)`, 'ok');
        break;
      }
      case 'cat':
      case 'read': {
        need(1, 'cat <caminho>');
        const r = cluster.read(args[0]);
        const f = r.file;
        selectIno(f.id);
        if (isBinary(r.bytes)) P(`(arquivo binário, ${r.bytes.length} bytes)`, 'dim');
        else new TextDecoder().decode(r.bytes).split('\n').forEach((l) => P(l));
        break;
      }
      case 'overwrite':
      case 'over': {
        need(2, 'overwrite <caminho> <texto>');
        const f = cluster.overwrite(args[0], rest(1));
        selectIno(f.id);
        P(`sobrescrito ${cluster.pathOf(f)}: v${f.version}, ${f.blocks.length} bloco(s)`, 'ok');
        break;
      }
      case 'append': {
        need(2, 'append <caminho> <texto>');
        const f = cluster.append(args[0], rest(1));
        selectIno(f.id);
        P(`anexado a ${cluster.pathOf(f)}: agora ${f.size} B em ${f.blocks.length} bloco(s)`, 'ok');
        break;
      }
      case 'replace': {
        need(2, 'replace <destino> <origem>');
        const f = cluster.replace(args[0], args[1]);
        selectIno(f.id);
        P(`${cluster.pathOf(f)} substituído (v${f.version})`, 'ok');
        break;
      }
      case 'rename':
      case 'ren': {
        need(2, 'rename <caminho> <novo-nome>');
        const f = cluster.rename(args[0], args[1]);
        selectIno(f.id);
        P('renomeado: ' + cluster.pathOf(f), 'ok');
        break;
      }
      case 'mv': {
        need(2, 'mv [-f] <origem> <destino>');
        const f = cluster.mv(args[0], args[1], { force: flags.has('f') });
        selectIno(f.id);
        P('movido para ' + cluster.pathOf(f), 'ok');
        break;
      }
      case 'cp': {
        need(2, 'cp <origem> <destino>');
        const f = cluster.cp(args[0], args[1]);
        selectIno(f.id);
        P('copiado para ' + cluster.pathOf(f), 'ok');
        break;
      }
      case 'rm':
      case 'del': {
        need(1, 'rm [-r] [-f] <caminho>');
        cluster.rm(args[0], { recursive: flags.has('r'), permanent: flags.has('f') });
        ui.selected = null;
        break;
      }
      case 'restore': {
        need(1, 'restore <caminho-na-lixeira>');
        const f = cluster.restore(args[0]);
        selectIno(f.id);
        P('restaurado em ' + cluster.pathOf(f), 'ok');
        break;
      }
      case 'emptytrash':
        cluster.emptyTrash();
        ui.selected = null;
        break;
      case 'stat': {
        need(1, 'stat <caminho>');
        const ino = cluster.resolve(args[0]);
        selectIno(ino.id);
        P(cluster.pathOf(ino), 'ok');
        if (ino.type === 'dir') {
          P(`  pasta com ${ino.children.size} item(ns); criada ${fmtTime(ino.ctime)}`);
        } else {
          P(`  arquivo  ${ino.size} B  versão v${ino.version}  réplicas-alvo ${ino.replication}`);
          P(`  criado ${fmtTime(ino.ctime)}  modificado ${fmtTime(ino.mtime)}`);
          if (ino.blocks.length) {
            P('  bloco  id        tam    crc32      réplicas', 'dim');
            for (const b of cluster.blockLocations(ino))
              P(`  ${String(b.index).padEnd(5)}  ${('blk_' + b.id).padEnd(8)}  ${(b.size + ' B').padEnd(5)}  ${hex(b.crc)}   ${repText(b.replicas)}`);
          } else P('  (sem blocos: arquivo vazio)', 'dim');
        }
        break;
      }
      case 'nodes': {
        P('nó    rack   estado             uso', 'dim');
        for (const n of cluster.nodes)
          P(`${n.id.padEnd(5)}  ${n.rack.padEnd(5)}  ${(n.online ? 'online' : n.dead ? 'morto' : 'sem resposta').padEnd(17)}  ${n.store.size}/${n.capacity} blocos`);
        break;
      }
      case 'kill': {
        need(1, 'kill <dnX>');
        doKill(args[0]);
        break;
      }
      case 'revive': {
        need(1, 'revive <dnX>');
        doRevive(args[0]);
        break;
      }
      case 'addnode': {
        const n = cluster.addNode();
        P('registrado ' + n.id, 'ok');
        break;
      }
      case 'rebalance':
        cluster.rebalance();
        break;
      case 'setrep': {
        need(2, 'setrep <n> <caminho>');
        cluster.setReplication(args[1], args[0]);
        break;
      }
      case 'fsck': {
        const h = cluster.fsck({ repair: flags.has('repair') });
        P(`estado: ${h.status}  blocos ${h.blocks}  réplicas íntegras ${h.validReplicas}/${h.expectedReplicas}  corrompidas ${h.corruptReplicas}  sub-replicados ${h.underReplicated}  indisponíveis ${h.missingBlocks}`, h.status === 'HEALTHY' ? 'ok' : 'err');
        break;
      }
      case 'corrupt': {
        need(1, 'corrupt <caminho>  ou  corrupt blk_N dnX');
        if (/^blk_\d+$/.test(args[0])) {
          need(2, 'corrupt blk_N dnX');
          cluster.corrupt(Number(args[0].slice(4)), args[1]);
        } else {
          cluster.corruptFile(args[0]);
        }
        break;
      }
      case 'set': {
        need(2, 'set block|rep|cap <n>');
        const k = args[0];
        if (k === 'block') cluster.setBlockSize(args[1]);
        else if (k === 'rep') cluster.setDefaultReplication(args[1]);
        else if (k === 'cap') cluster.setCapacity(args[1]);
        else throw new DFSError('EINVAL', 'use: set block|rep|cap <n>');
        break;
      }
      case 'reset':
        resetCluster();
        P('cluster reiniciado', 'ok');
        break;
      default:
        throw new DFSError('EINVAL', `comando desconhecido: ${name}. Digite help.`);
    }
    return out;
  }

  /** Executa uma linha do console: mostra o comando, os eventos internos e o resultado. */
  function execLine(line) {
    line = line.trim();
    if (!line) return;
    conPrint(`<span class="pr">${esc('dfs:' + cluster.cwd + '$')}</span> ${esc(line)}`, 'cmd');
    const evs = [];
    capture = evs;
    let result = [];
    let error = null;
    try {
      const toks = tokenize(line);
      const name = toks.length ? toks[0].v.toLowerCase() : '';
      const { flags, args } = parseArgs(toks.slice(1));
      result = runCommand(name, flags, args);
    } catch (e) {
      error = e;
    } finally {
      capture = null;
    }
    conEvents(evs);
    for (const [t, c] of result) conText(t, c);
    if (error) conText(errText(error), 'err');
    afterChange();
  }

  /* ================================================================ demo guiada */
  const DEMO = [
    {
      title: 'O namespace é só metadado',
      text: 'Criamos pastas. Repare no log: só o NameNode age, nenhum DataNode é acionado, e os discos continuam vazios.',
      cmds: ['mkdir -p /docs/so', 'ls /docs']
    },
    {
      title: 'Escrever um arquivo',
      text: 'O texto é dividido em blocos de 32 B e cada bloco é gravado em 3 DataNodes diferentes (pipeline). Os quadrados coloridos aparecem nos discos: cada cor é um arquivo, cada número é o índice do bloco.',
      cmds: ['write /docs/so/intro.txt "Um sistema de arquivos distribuído divide cada arquivo em blocos e guarda cópias em vários servidores."']
    },
    {
      title: 'Metadados do arquivo',
      text: 'O NameNode sabe, para cada bloco, em quais DataNodes estão as réplicas. É esse mapa que permite ler o arquivo depois.',
      cmds: ['stat /docs/so/intro.txt']
    },
    {
      title: 'Ler',
      text: 'O cliente pede a lista de blocos ao NameNode, busca cada bloco em um DataNode e confere o checksum. Depois remonta o texto.',
      cmds: ['cat /docs/so/intro.txt']
    },
    {
      title: 'Sobrescrever',
      text: 'Os blocos novos são gravados primeiro; só depois o NameNode troca a lista do inode (versão 2) e libera os blocos antigos. Se faltasse espaço, o arquivo antigo seguiria intacto.',
      cmds: ['overwrite /docs/so/intro.txt "Versão 2: o conteúdo antigo foi trocado por inteiro."']
    },
    {
      title: 'Anexar',
      text: 'Blocos são imutáveis. Anexar texto cria blocos novos no fim do arquivo, sem mexer nos existentes.',
      cmds: ['append /docs/so/intro.txt " Este trecho foi anexado depois."', 'cat /docs/so/intro.txt']
    },
    {
      title: 'Renomear',
      text: 'Só o nome muda no NameNode. Compare o stat antes e depois: os blocos e réplicas são exatamente os mesmos.',
      cmds: ['rename /docs/so/intro.txt leitura.txt', 'stat /docs/so/leitura.txt']
    },
    {
      title: 'Mover',
      text: 'Mover também é só metadado: o arquivo troca de pasta e nenhum bloco se move entre DataNodes.',
      cmds: ['mv /docs/so/leitura.txt /docs', 'tree /']
    },
    {
      title: 'Substituir',
      text: 'Criamos um rascunho e o substituímos pelo conteúdo de leitura.txt. O rascunho mantém o nome e vira a versão 2; os blocos antigos são liberados.',
      cmds: ['write /docs/rascunho.txt "Rascunho antigo que será trocado."', 'replace /docs/rascunho.txt /docs/leitura.txt', 'cat /docs/rascunho.txt']
    },
    {
      title: 'Falha de um DataNode',
      text: 'Derrubamos o dn2. A leitura continua funcionando (failover para outra réplica). Depois do timeout de heartbeat o NameNode declara o nó morto e re-replica os blocos que ficaram com 2 cópias, voltando a 3.',
      cmds: ['kill dn2', 'cat /docs/leitura.txt', { wait: CFG.timeoutHeartbeat + 900 }, 'fsck']
    },
    {
      title: 'O DataNode volta',
      text: 'Ao voltar, o dn2 manda um block report. Como já existem cópias suficientes, o NameNode remove as réplicas em excesso.',
      cmds: ['revive dn2', 'nodes']
    },
    {
      title: 'Corrupção silenciosa',
      text: 'Corrompemos uma réplica sem avisar ninguém (bit rot). O fsck compara o CRC32 e acusa o problema; com --repair a réplica ruim é descartada e refeita a partir de uma boa.',
      cmds: ['corrupt /docs/leitura.txt', 'fsck', 'fsck --repair']
    },
    {
      title: 'Apagar e restaurar',
      text: 'Apagar move o item para a lixeira: só metadados. Os blocos continuam nos discos, então dá para restaurar.',
      cmds: ['rm /docs/rascunho.txt', 'ls /.lixeira', 'restore /.lixeira/rascunho.txt']
    },
    {
      title: 'Excluir de vez',
      text: 'Com rm -f o inode some e as réplicas são removidas de todos os DataNodes. Os quadrados do arquivo desaparecem dos discos.',
      cmds: ['rm -f /docs/rascunho.txt', 'fsck']
    },
    {
      title: 'Sem réplicas, sem dados',
      text: 'Com apenas 1 réplica por bloco, derrubar o nó que guarda um bloco deixa o arquivo ilegível (erro EIO). Quando o nó volta, a leitura funciona de novo. Por isso o padrão é 3.',
      cmds: ['setrep 1 /docs/leitura.txt', () => 'kill ' + holderOf('/docs/leitura.txt'), 'cat /docs/leitura.txt', () => 'revive ' + [...cluster.nodes].find((n) => !n.online).id, 'cat /docs/leitura.txt']
    }
  ];

  /* Resultado real de cada passo (mostrado depois de executar, junto com o estado dos discos). */
  const RESULTADOS = [
    'Só o NameNode agiu (criou /docs e /docs/so). Nenhum DataNode foi acionado, então os discos continuam vazios.',
    '105 B viraram 4 blocos (32 + 32 + 32 + 9 B), cada um copiado em 3 DataNodes: 4 × 3 = 12 cópias nos discos.',
    'Os discos não mudaram. O stat listou, para cada bloco, os 3 DataNodes que guardam as cópias.',
    'Os discos não mudaram. O cliente leu cada bloco de um DataNode, conferiu o checksum e remontou o texto.',
    'O texto novo tem 54 B e coube em 2 blocos × 3 = 6 cópias. As 12 cópias antigas foram removidas e o arquivo virou v2.',
    'Entrou 1 bloco novo de 32 B (3 cópias); os blocos antigos não foram tocados. O arquivo agora tem 86 B em 3 blocos (9 cópias).',
    'Os discos não mudaram: só o nome mudou no NameNode (intro.txt → leitura.txt). Os 3 blocos ficaram onde estavam.',
    'Os discos não mudaram: só o NameNode anotou a nova pasta (/docs). Nenhum bloco se moveu entre DataNodes.',
    'rascunho.txt foi criado e depois substituído por 3 blocos novos com o conteúdo de leitura.txt; os blocos do rascunho antigo foram liberados. Agora são 6 blocos (18 cópias).',
    'O dn2 caiu e a leitura continuou (failover). Passado o timeout, o NameNode o declarou morto e refez as cópias que ele tinha nos outros nós: todas as réplicas voltaram ao alvo.',
    'O dn2 voltou e mandou seu block report. Como já havia 3 cópias de cada bloco, as cópias em excesso foram removidas.',
    'O fsck achou 1 réplica corrompida (estado Degradado) e, com --repair, ela foi refeita a partir de uma cópia boa. O cluster voltou a Saudável.',
    'O arquivo foi para a lixeira e já foi restaurado neste mesmo passo. Só metadados mudaram: os blocos não saíram dos discos.',
    'rascunho.txt foi apagado de vez e as cópias dele saíram de todos os DataNodes. Sobraram só os 3 blocos de leitura.txt (9 cópias).',
    'Com 1 cópia por bloco, derrubar um nó deixou o arquivo ilegível (o erro vermelho é de propósito). Quando o nó voltou, a leitura funcionou de novo.'
  ];

  /* Quantos blocos cada DataNode guarda neste momento. */
  function demoDisks() {
    return cluster.nodes.map((n) => `${n.id} ${n.online ? n.store.size : 'fora do ar'}`).join(' · ');
  }

  function renderDemo() {
    const bar = $('#demo-bar');
    const d = ui.demo;
    if (!d) {
      bar.hidden = true;
      return;
    }
    bar.hidden = false;
    const prev = !d.busy && d.last ? d.last : null;
    const resultado = prev
      ? `<p class="demo-text" style="padding:6px 10px;margin-bottom:6px;background:#e3f4ec;border-left:3px solid #12a38a;border-radius:6px"><b>Resultado do passo ${prev.i + 1} (${esc(DEMO[prev.i].title)}):</b> ${esc(RESULTADOS[prev.i])} <b>Discos agora: ${esc(prev.disks)}.</b></p>`
      : '';
    if (d.i >= DEMO.length) {
      bar.innerHTML = `<div style="grid-column:1;min-width:0">${resultado}<div class="demo-title">Demo concluída</div><p class="demo-text">Você viu escrever, ler, sobrescrever, anexar, renomear, mover, substituir, apagar e restaurar, além de falha de nó, corrupção e recuperação. Pode continuar explorando pelo console ou pela interface.</p></div><div class="demo-btns"><button class="btn" data-action="demo">Recomeçar</button><button class="btn primary" data-action="demo-exit">Fechar demo</button></div>`;
      return;
    }
    const s = DEMO[d.i];
    bar.innerHTML = `<div style="grid-column:1;min-width:0">${resultado}<div class="demo-title"><small>${prev ? 'Próximo: ' : ''}Passo ${d.i + 1} de ${DEMO.length}</small>${esc(s.title)}</div><p class="demo-text">${esc(s.text)}</p></div><div class="demo-btns"><button class="btn primary" data-action="demo-next"${d.busy ? ' disabled' : ''}>${d.busy ? 'Executando…' : 'Executar passo ' + (d.i + 1)}</button><button class="btn" data-action="demo-exit">Fechar demo</button></div>`;
  }

  function demoStart() {
    clearAllPending();
    ui.auto = true;
    cluster.config.blockSize = DEFAULTS.blockSize;
    cluster.config.replication = DEFAULTS.replication;
    cluster.config.nodeCapacity = DEFAULTS.nodeCapacity;
    resetCluster();
    ui.demo = { i: 0, busy: false, last: null };
    setTab('console');
    conPrint('Demo guiada iniciada: cluster reiniciado com 4 DataNodes, bloco de 32 B e replicação 3.', 'step');
    renderDemo();
  }

  async function demoNext() {
    const d = ui.demo;
    if (!d || d.busy || d.i >= DEMO.length) return;
    d.busy = true;
    renderDemo();
    setTab('console');
    const step = DEMO[d.i];
    conPrint(`Passo ${d.i + 1}: ${esc(step.title)}`, 'step');
    for (const c of step.cmds) {
      if (typeof c === 'object') await sleep(c.wait);
      else {
        let line;
        try {
          line = typeof c === 'function' ? c() : c;
        } catch (e) {
          conText(errText(e), 'err');
          continue;
        }
        execLine(line);
        await sleep(700);
      }
    }
    d.last = { i: d.i, disks: demoDisks() };
    d.i++;
    d.busy = false;
    renderDemo();
  }

  /* ================================================================ eventos da página */
  document.addEventListener('click', (ev) => {
    const el = ev.target.closest('[data-action]');
    if (!el) return;
    const a = el.dataset.action;
    switch (a) {
      case 'select': {
        selectIno(Number(el.dataset.id));
        renderExplorer();
        renderDetail();
        renderCluster();
        updatePrompt();
        break;
      }
      case 'toggle': {
        const id = Number(el.dataset.id);
        if (ui.expanded.has(id)) ui.expanded.delete(id);
        else ui.expanded.add(id);
        renderExplorer();
        ev.stopPropagation();
        break;
      }
      case 'tab':
        setTab(el.dataset.tab);
        break;
      case 'log-filter':
        $('#log-out').dataset.filter = el.dataset.filter;
        $$('.log-bar .chip[data-filter]').forEach((c) => c.classList.toggle('on', c === el));
        break;
      case 'log-clear':
        $('#log-out').innerHTML = '';
        break;
      case 'close-modal':
        closeModal();
        break;
      case 'new-file': actNewFile(); break;
      case 'new-dir': actNewDir(); break;
      case 'read': actRead(); break;
      case 'download': actDownload(); break;
      case 'overwrite': actOverwrite(); break;
      case 'replace': actReplace(); break;
      case 'rename': actRename(); break;
      case 'move': actMove(); break;
      case 'copy': actCopy(); break;
      case 'delete': actDelete(); break;
      case 'purge': actPurge(); break;
      case 'restore': actRestore(); break;
      case 'empty-trash':
        confirmModal({
          title: 'Esvaziar lixeira',
          text: 'Excluir definitivamente tudo que está na lixeira? As réplicas serão removidas dos DataNodes.',
          label: 'Esvaziar',
          danger: true,
          onConfirm() {
            cluster.emptyTrash();
            ui.selected = null;
            afterChange();
            toast('Lixeira esvaziada', 'ok');
          }
        });
        break;
      case 'kill':
        guard(() => {
          doKill(el.dataset.node);
          return null;
        });
        break;
      case 'revive':
        guard(() => {
          doRevive(el.dataset.node);
          return `${el.dataset.node} voltou ao cluster`;
        });
        break;
      case 'add-node':
        guard(() => `${cluster.addNode().id} adicionado (use Rebalancear para distribuir blocos nele)`);
        break;
      case 'rebalance':
        guard(() => {
          const m = cluster.rebalance();
          return m ? `${m} réplica(s) movida(s)` : 'O cluster já está equilibrado';
        });
        break;
      case 'fsck':
        guard(() => `fsck: ${cluster.fsck().status}`);
        break;
      case 'fsck-repair':
        guard(() => `fsck --repair: ${cluster.fsck({ repair: true }).status}`);
        break;
      case 'force':
        guard(() => {
          const { r } = forceRecovery();
          return `Recuperação concluída: ${r.copied} réplica(s) recriada(s)`;
        });
        break;
      case 'demo': demoStart(); break;
      case 'demo-next': demoNext(); break;
      case 'demo-exit':
        ui.demo = null;
        renderDemo();
        break;
      case 'about': actAbout(); break;
      case 'settings': actSettings(); break;
      case 'reset': actReset(); break;
    }
  });

  document.addEventListener('change', (ev) => {
    const el = ev.target.closest('[data-change]');
    if (!el) return;
    if (el.dataset.change === 'setrep') {
      const ino = selIno();
      if (ino) guard(() => (cluster.setReplication(cluster.pathOf(ino), el.value), `Replicação de ${ino.name}: ${el.value}`));
    } else if (el.dataset.change === 'auto') {
      ui.auto = el.checked;
      if (ui.auto) {
        for (const n of cluster.nodes) if (!n.online && !n.dead && !ui.pending.has(n.id)) scheduleDeath(n.id);
      } else {
        clearAllPending();
      }
      renderCluster();
    }
  });

  document.addEventListener('dblclick', (ev) => {
    const row = ev.target.closest('.row[data-id]');
    if (!row) return;
    const ino = cluster.inodes.get(Number(row.dataset.id));
    if (ino && ino.type === 'dir' && ino.children.size) {
      if (ui.expanded.has(ino.id)) ui.expanded.delete(ino.id);
      else ui.expanded.add(ino.id);
      renderExplorer();
    }
  });

  document.addEventListener('keydown', (ev) => {
    if (ev.key === 'Escape' && !$('#modal').hidden) closeModal();
    if ((ev.key === 'Enter' || ev.key === ' ') && ev.target.matches && ev.target.matches('.row[data-id]')) {
      ev.preventDefault();
      ev.target.click();
    }
  });

  $('#modal').addEventListener('mousedown', (ev) => {
    if (ev.target === $('#modal')) closeModal();
  });

  // clicar num bloco seleciona o arquivo dono dele
  $('#cluster').addEventListener('click', (ev) => {
    const slot = ev.target.closest('.slot[data-file]');
    if (!slot) return;
    const id = Number(slot.dataset.file);
    if (cluster.inodes.has(id)) {
      selectIno(id);
      renderExplorer();
      renderDetail();
      renderCluster();
      updatePrompt();
    }
  });

  // tooltip
  const tip = $('#tip');
  document.addEventListener('mouseover', (ev) => {
    const t = ev.target.closest('[data-tip]');
    if (!t) return;
    tip.textContent = t.dataset.tip;
    tip.hidden = false;
  });
  document.addEventListener('mousemove', (ev) => {
    if (tip.hidden) return;
    const w = tip.offsetWidth;
    tip.style.left = Math.min(ev.clientX + 14, window.innerWidth - w - 8) + 'px';
    tip.style.top = ev.clientY + 16 + 'px';
  });
  document.addEventListener('mouseout', (ev) => {
    if (ev.target.closest && ev.target.closest('[data-tip]')) tip.hidden = true;
  });

  // console
  $('#con-form').addEventListener('submit', (ev) => {
    ev.preventDefault();
    const input = $('#con-in');
    const line = input.value;
    input.value = '';
    if (line.trim()) {
      ui.history.push(line);
      ui.hIdx = ui.history.length;
    }
    execLine(line);
  });
  $('#con-in').addEventListener('keydown', (ev) => {
    const input = ev.target;
    if (ev.key === 'ArrowUp' && ui.history.length) {
      ev.preventDefault();
      ui.hIdx = Math.max(0, ui.hIdx - 1);
      input.value = ui.history[ui.hIdx] || '';
    } else if (ev.key === 'ArrowDown') {
      ev.preventDefault();
      ui.hIdx = Math.min(ui.history.length, ui.hIdx + 1);
      input.value = ui.history[ui.hIdx] || '';
    }
  });
  $('#pane-console').addEventListener('click', (ev) => {
    if (!window.getSelection().toString() && !ev.target.closest('button')) $('#con-in').focus();
  });

  /* ================================================================ início */
  renderHeader();
  cluster.events.forEach(appendLog);

  if (fresh) {
    try {
      cluster.write(
        '/bem-vindo.txt',
        'Olá! Este arquivo foi dividido em blocos e replicado em vários DataNodes. Clique nele para ver onde cada bloco está.'
      );
      ui.selected = cluster.resolve('/bem-vindo.txt').id;
    } catch (e) {
      /* ignora */
    }
  } else {
    cluster._emit('info', 'sistema', 'estado anterior restaurado do navegador');
  }

  for (const n of cluster.nodes) if (!n.online && !n.dead) scheduleDeath(n.id);

  conPrint('Mini-DFS pronto. Digite <b>help</b> para ver os comandos ou clique em <b>Demo guiada</b>.', 'dim');
  conPrint('Dica: clique num arquivo para acender seus blocos nos DataNodes.', 'dim');
  afterChange();
  setTab('console');
})();
