# Mini-DFS

Simulador didático de um **sistema de arquivos distribuído (DFS)** que roda inteiro no navegador, sem servidor e sem dependências. Feito para o trabalho de Sistemas de Arquivos (Sistemas Operacionais T303): o "mini sistema de arquivos" com **ler, escrever, sobrescrever, apagar, substituir, renomear e mover**, mais replicação, falha de nós e recuperação.

## Antes de apresentar: coloque os nomes da equipe

Abra `js/config.js` e troque os integrantes:

```js
equipe: ['Maria Souza', 'João Lima', 'Ana Costa', 'Pedro Alves'],
```

Os nomes aparecem no topo da página (critério "Identificação dos membros da equipe").

## Publicar no GitHub Pages

1. Crie um repositório no GitHub (por exemplo `mini-dfs`).
2. Envie **o conteúdo desta pasta** para ele, com o `index.html` na raiz:
   ```bash
   git init
   git add .
   git commit -m "Mini-DFS"
   git branch -M main
   git remote add origin https://github.com/SEU-USUARIO/mini-dfs.git
   git push -u origin main
   ```
3. No repositório: **Settings → Pages → Build and deployment → Source: Deploy from a branch → Branch: `main` / `(root)` → Save**.
4. Em cerca de um minuto o site fica em `https://SEU-USUARIO.github.io/mini-dfs/`.

O arquivo `.nojekyll` já está na pasta (ele evita que o GitHub processe o site com Jekyll). Para testar localmente basta abrir o `index.html` no navegador.

## Como usar na apresentação

Roteiro de 4 a 5 minutos:

1. Abra **Como funciona** e explique o diagrama: o NameNode só guarda metadados, os DataNodes guardam os blocos.
2. Clique em **Demo guiada**. Cada clique em *Executar passo* roda os comandos no console, mostra o que o NameNode e os DataNodes fizeram e acende os blocos nos discos. São 15 passos: escrever, ler, sobrescrever, anexar, renomear, mover, substituir, falha de nó, retorno do nó, corrupção, lixeira, exclusão definitiva e o limite da replicação 1.
3. Deixe o professor mandar comandos no console (`help` lista todos) ou clicar nos botões da interface.

Dica: clicar num arquivo acende só os blocos dele nos DataNodes. Passar o mouse sobre um bloco mostra id, arquivo, tamanho e CRC32.

## Requisitos do trabalho e onde estão

| Operação pedida | Na interface | No console |
|---|---|---|
| Escrever | Novo arquivo | `write /pasta/a.txt "texto"` |
| Ler | Ler (mostra de qual DataNode veio cada bloco) | `cat /pasta/a.txt` |
| Sobrescrever | Sobrescrever | `overwrite /pasta/a.txt "novo"` |
| Substituir | Substituir (por outro arquivo do DFS ou do computador) | `replace /destino.txt /origem.txt` |
| Renomear | Renomear | `rename /pasta/a.txt b.txt` |
| Mover | Mover | `mv /pasta/a.txt /outra` |
| Apagar | Apagar (vai para `/.lixeira`) e Excluir definitivamente | `rm`, `rm -f`, `restore`, `emptytrash` |

Extras: anexar (`append`), copiar (`cp`), replicação por arquivo (`setrep`), `fsck` (com `--repair`), corromper uma réplica (`corrupt`), derrubar e reviver DataNodes (`kill`, `revive`), adicionar nó e rebalancear.

## O que é simulado

- **NameNode:** árvore de pastas, inodes e mapa bloco → DataNodes. Nunca guarda conteúdo.
- **DataNodes:** guardam réplicas de blocos com CRC32, organizados em racks.
- **Escrita:** o arquivo é dividido em blocos, cada bloco vai para N DataNodes (padrão 3), espalhados entre racks e equilibrados por uso.
- **Sobrescrever é atômico:** grava os blocos novos, troca a lista de blocos do inode (versão +1) e só então libera os antigos. Se faltar espaço, o arquivo antigo continua intacto.
- **Renomear e mover são só metadados:** nenhum bloco se move.
- **Falha de nó:** o NameNode espera o timeout de heartbeat, declara o nó morto e re-replica os blocos abaixo do alvo. A leitura continua funcionando por failover entre réplicas.
- **Retorno do nó:** o nó envia um block report; blocos de arquivos já apagados são descartados e réplicas em excesso removidas.
- **Corrupção:** a leitura e o `fsck` comparam o CRC32; réplicas ruins são descartadas e refeitas a partir de uma boa.

Para dar para enxergar os blocos, o tamanho padrão é **32 bytes** (ajustável em *Configurações*). No HDFS real o bloco tem 128 MB, e o NameNode espera cerca de 10 minutos sem heartbeat antes de declarar um nó morto; aqui são 3,5 s (`timeoutHeartbeat` em `js/config.js`).

## Perguntas que o professor pode fazer

- **Por que replicar?** Para tolerar falhas: com 3 cópias em racks diferentes, perder um nó (ou um rack) não perde dados. O custo é espaço (3×).
- **E se o NameNode cair?** É um ponto único de falha. O HDFS real usa NameNode em standby com alta disponibilidade e um log de edições (EditLog + FsImage). O Mini-DFS não implementa isso.
- **Por que blocos grandes no HDFS?** Para reduzir o volume de metadados no NameNode e favorecer leituras sequenciais longas.
- **Renomear um arquivo de 1 GB é lento?** Não: só o nome muda no NameNode. Copiar é que recria os blocos.
- **O que impede um arquivo de ser lido corrompido?** O CRC32 de cada bloco, verificado na leitura. Se falhar, o cliente usa outra réplica.
- **Isso é o DFS da Microsoft?** Os conceitos de namespace, servidores de destino e replicação são parecidos (veja a tabela de equivalências em *Como funciona*), mas o simulador segue o modelo HDFS/GFS, com blocos e NameNode.

## Estrutura

```
index.html          página
css/style.css       estilos
js/config.js        nomes da equipe e timeout (edite)
js/dfs.js           motor do DFS (sem DOM; roda no navegador e no Node)
js/app.js           interface, console e demo guiada
tests/engine.test.js  40 testes do motor
```

Para rodar os testes (precisa de Node.js): `node tests/engine.test.js`

## Limites

Os DataNodes são estruturas em memória, sem rede real, e o estado fica salvo no `localStorage` do navegador (use *Reiniciar cluster* para limpar). Não há permissões, quotas, vários clientes concorrentes nem NameNode secundário.
