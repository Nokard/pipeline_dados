# Modelagem

Como cada camada foi desenhada e por quê.

```
raw/                        CSVs de CDC, como a origem entregou
  └─ bronze/{fonte}         tipado e particionado por dia — 3 tabelas separadas
       └─ silver/eventos_unificados    as 3 fontes numa linha do tempo, sem reenvios
            └─ silver/purchase_diario  estado consolidado da compra em cada dia
                 └─ gold/purchase_historico   validade SCD2 — dataset final
```

---

## O que torna esse problema difícil

Três fontes descrevem a mesma compra (`purchase_id` é a chave), mas:

1. **Chegam em dias diferentes.** O `purchase_id = 56` recebe `product_item` e
   `extra_info` em 25/01, e `purchase` só em 26/01 — o "filho" antes do "pai".
2. **A ordem é desconhecida.** O `71` recebe `extra_info` (10/03) →
   `product_item` (15/03) → `purchase` (20/03), ordem totalmente invertida.
3. **Corrigem o passado.** O `55` teve o valor e a data de pagamento alterados
   em julho, seis meses depois da compra.
4. **Podem reenviar.** O enunciado avisa que falhas de envio geram reenvio do
   mesmo evento.
5. **Podem cancelar.** O `72` foi pago em 04/04 e teve o `release_date` zerado
   em 10/05.

E o requisito que amarra tudo: **o passado não pode mudar**, nem com
reprocessamento full.

---

## Dados de entrada

Os prints do enunciado trazem **13 eventos** de três compras (`55`, `56`, `69`).
Eles foram transcritos **sem nenhuma alteração** para `data/events/` — dá para
conferir linha a linha contra o PDF.

Além deles, **foram criados dados fictícios** com sete compras a mais (`70` a
`76`), totalizando **39 eventos e 10 compras**:

| Fonte | Eventos |
|---|---|
| `purchase` | 16 |
| `product_item` | 12 |
| `purchase_extra_info` | 11 |

O motivo não foi volume: os 13 eventos do enunciado deixam algumas situações
sem representação, e sem dado que as exercite não há como provar que a
modelagem as trata. Cada compra fictícia cobre um caso específico:

| Compra | Cenário | O que exercita |
|---|---|---|
| `70` | 3 fontes no mesmo dia; valor corrigido em ago; subsidiária muda em set | caso feliz + duas correções tardias |
| `71` | `extra_info` (10/03) → `product_item` (15/03) → `purchase` (20/03) | ordem totalmente invertida |
| `72` | pago em 04/04, `release_date` volta a `NULL` em 10/05 | **cancelamento** — quebra o forward fill ingênuo |
| `73` | `extra_info` nunca chega | compra que nunca completa |
| `74` | linha idêntica duplicada | reenvio de evento |
| `75` | 2 eventos de `purchase` no mesmo dia, horas diferentes | último do dia vence |
| `76` | 2 eventos de `purchase` no **mesmo timestamp** | desempate determinístico |

As duas mais valiosas são a `72` e a `76`. A `72` é o único dado que distingue
"a fonte não falou" de "a fonte falou vazio" — sem ela, o bug do forward fill
passaria despercebido. A `76` é o único empate real de `transaction_datetime`,
que é o que obriga a ordenação a ser total.

O volume continua pequeno de propósito: **39 eventos são conferíveis à mão**, o
que permite validar cada regra contra o resultado esperado em vez de confiar em
agregados.

> Uma diferença de forma: o PDF exibe valores como `50,00` (formatação pt-BR),
> e os CSVs usam `50.00`. Vírgula decimal dentro de arquivo separado por vírgula
> seria ruído desnecessário; o tratamento de precisão continua garantido pelo
> `DECIMAL(18,2)` do bronze.

---

## raw

Os CSVs crus, exatamente como a origem entregou. O `seed_data.py` só faz upload
— nenhuma transformação.

**Por que existe:** se você transforma na chegada, perde a capacidade de
reprocessar do zero quando descobrir um bug na transformação. O `raw` é a única
coisa que o pipeline nunca reescreve.

---

## bronze

Uma tabela por fonte, tipada e particionada por `transaction_date`. **Nada é
unido aqui.**

**Schema explícito, nunca `inferSchema`.** A inferência olha uma amostra dos
dados: o mesmo arquivo pode virar tipos diferentes entre execuções. O
`release_date` vazio do `purchase_id = 56` é o caso clássico.

**`purchase_value` como `DECIMAL(18,2)`.** É dinheiro e entra em `SUM()`.
`float` acumula erro de arredondamento e o GMV não fecha na conferência.

**`transaction_date` derivada, não confiada.** O CSV traz a coluna pronta, mas
ela é redundante com o `transaction_datetime`. Se a origem mandar as duas
divergentes, a partição vai pro dia errado — e como a partição é o que congela
o passado, a imutabilidade quebra sem ninguém perceber.

### Por que TimestampNTZ

`transaction_datetime` é **`TimestampNTZType`**, não `TimestampType`.

`TimestampType` guarda um *instante*: o Spark interpreta a string no fuso da
sessão ao escrever e reconverte ao ler. Sem fuso fixo, o container (`Etc/UTC`) e
a máquina local (`America/Sao_Paulo`) produzem horários diferentes para o mesmo
dado — e um evento às 00:05 deslocado em 3h **muda de dia**, logo de partição.

Isso aconteceu de verdade: o evento de `2023-01-23 00:05` aparecia como
`2023-01-22 21:05` numa partição `2023-01-23`. A linha ficava internamente
contraditória, porque o `datetime` (instante) se movia e o `date` (dia de
calendário, sem fuso) não.

`NTZ` guarda o relógio de parede literal — entra `2023-01-23 00:05:00`, sai
igual, em qualquer fuso. Semanticamente também é o tipo certo: o timestamp vem
do log de CDC da origem, é hora local daquele sistema, nunca foi um instante
global.

*Verificado: leitura da gold em UTC, São Paulo e Tóquio devolve valores
idênticos.*

---

## silver/eventos_unificados

As três fontes empilhadas numa linha do tempo única.
**Grão:** 1 linha por (`purchase_id`, `origem_evento`, `transaction_date`).

### UNION, não JOIN

O JOIN assumiria que as três fontes existem ao mesmo tempo — e não existem. Para
o `71`, um INNER JOIN devolveria zero linhas até 20/03; um LEFT JOIN ancorado em
`purchase` não teria nem a linha-âncora. O JOIN também **colapsa a linha do
tempo**, perdendo *quando* cada informação chegou — que é a rastreabilidade
exigida.

O union empilha os eventos e deixa cada fonte preencher só as colunas de que ela
é dona; o resto vem `NULL`. Resultado: **a ordem de chegada deixa de importar**
— não existe "quem cria a linha", e o problema das seis permutações some por
construção.

### A coluna `origem_evento`

Parece só rastreabilidade, mas é **estrutural**. Ela desambigua dois `NULL`
incompatíveis na mesma coluna:

- *"essa fonte não fala sobre esse campo"* → deve herdar o valor anterior
- *"essa fonte falou, e o valor é vazio"* → **não** deve herdar

Sem ela, o cancelamento do `72` seria indistinguível de silêncio.

### Deduplicação e ordenação total

**Reenvio:** dois eventos byte-idênticos são o mesmo evento. `dropDuplicates`
sobre um `hash_evento` (sha2 do conteúdo) resolve.

**Ordenação total:** `transaction_datetime` **não é único** — o `76` tem dois
eventos de `purchase` às 12:00:00 de 25/06. Sem critério de desempate, o Spark
escolhe arbitrariamente, e a escolha muda entre execuções. O desempate por
`hash_evento` torna a ordem total: qual linha vence é arbitrário, mas vence
**sempre a mesma**.

> Determinismo não é sobre *qual* linha você escolhe no empate. É sobre escolher
> sempre a mesma. Num CDC real o critério correto seria o offset do log
> (LSN/binlog), que aqui não existe.

---

## silver/purchase_diario

O estado consolidado da compra em cada dia.
**Grão:** 1 linha por (`purchase_id`, `transaction_date`), com todos os campos
ativos preenchidos.

Atende ao requisito: *"se uma tabela sofreu atualização e as demais não, os
dados ativos das demais deverão ser repetidos"*.

### Forward fill por bloco de fonte

O jeito ingênuo — `last(coluna, ignorenulls=True)` — **está errado aqui**. O
`ignorenulls` pula todos os `NULL`, inclusive os que são afirmação. No `72`, o
cancelamento de 10/05 seria ignorado e a compra contaria GMV para sempre.

A correção é agrupar as colunas de cada fonte num `struct` que só existe quando
o evento veio daquela fonte:

```python
bloco = F.when(F.col("origem_evento") == fonte, F.struct(*colunas))
F.last(bloco, ignorenulls=True).over(linha_do_tempo)
```

O struct inteiro é não-nulo sempre que a fonte falou — mesmo que todos os campos
dentro dele sejam `NULL`. Assim o `ignorenulls` pula apenas eventos de *outras*
fontes, nunca um evento real.

### A janela olha só para trás

```python
.rowsBetween(Window.unboundedPreceding, Window.currentRow)
```

O estado de 20/01 é calculado só com eventos até 20/01 — a correção de julho não
existe do ponto de vista daquela linha. **Isso não é uma proteção adicional; é
uma propriedade da janela**, e é o que congela os valores de negócio.

O colapso para o grão diário pega a última linha de cada dia. Ela serve porque o
fill é cumulativo: já herdou tudo que veio antes, inclusive as linhas mais cedo
do mesmo dia. A fusão de fontes dentro do dia sai de graça.

---

## gold

**`gold/purchase_historico`** — o dataset final. As mesmas 22 linhas do estado
diário, mais três colunas de vigência:

| Coluna | Significado |
|---|---|
| `dt_inicio_validade` | a partir de quando essa versão vale |
| `dt_fim_validade` | até quando valeu (`NULL` = vigente) |
| `is_current` | atalho para `dt_fim_validade IS NULL` |

`dt_fim_validade` é a `transaction_date` da versão seguinte menos um dia. Os
intervalos cobrem todos os dias **sem buraco nem sobreposição**: entre 24/01 e
04/02 o `55` não teve evento algum, mas a versão de 23/01 continua vigente. É
isso que faz o `BETWEEN` responder "como estava em qualquer data".

### Schema

```
purchase_id           BIGINT          transaction_datetime  TIMESTAMP_NTZ
buyer_id              BIGINT          prod_item_id          BIGINT
order_date            DATE            release_date          DATE
producer_id           BIGINT          product_id            BIGINT
item_quantity         INT             purchase_value        DECIMAL(18,2)
subsidiary            STRING          fontes_no_dia         ARRAY<STRING>
dt_inicio_validade    DATE            dt_fim_validade       DATE
is_current            BOOLEAN
PARTITIONED BY        transaction_date DATE
```

### Uma tensão consciente

As colunas de negócio (`buyer_id`, `purchase_value`, `subsidiary`) foram
calculadas com uma janela que olha **para trás** — nunca mudam.

As colunas de vigência fazem o oposto: para saber quando uma versão deixou de
valer é preciso olhar o evento **seguinte**. "Ser o registro corrente" não é uma
propriedade da linha, e sim dela em relação às que vieram depois — logo muda
quando chega dado novo, por definição.

Isso não quebra a imutabilidade: a consulta de GMV filtra e soma valores sem
tocar em `is_current`. O que se atualiza é metadado de vigência, não valor de
negócio.

### Por que não uma gold agregada

Uma tabela de deltas (`+50`, `−50`, `+5`…) chegou a ser construída e foi
**descartada**: o SCD2 já resolve a viagem no tempo, com SQL mais simples e uma
tabela a menos. O enunciado pede *"um select em cima da sua tabela"*, não uma
segunda tabela agregada.

---

## A consulta do GMV

```sql
SELECT release_date                           AS dia,
       COALESCE(subsidiary, 'não informada')  AS subsidiaria,
       SUM(purchase_value)                    AS gmv
FROM purchase_historico
WHERE is_current
  AND release_date IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;
```

| Trecho | Por quê |
|---|---|
| `is_current` | a tabela tem 22 versões, só 10 vigentes; sem isso o GMV infla |
| `release_date IS NOT NULL` | a definição de GMV — só pagamento confirmado. Exclui o `56` (nunca pago) e o `72` (cancelado) |
| `COALESCE(subsidiary, …)` | o `73` nunca recebeu `extra_info`; descartar esconderia R$ 640 de receita real |
| `release_date` como dia | o dia do GMV é o do pagamento, não o dia em que o dado chegou |

**Viagem no tempo** — troca-se apenas o filtro de vigência:

```sql
WHERE date'2023-03-31' BETWEEN dt_inicio_validade
                           AND COALESCE(dt_fim_validade, date'9999-12-31')
```

```
2023-01-20  nacional  →  hoje: ausente   |  em 31/03/2023: R$ 50,00
```

O pagamento do `55` migrou para 01/03 numa correção de julho. As duas respostas
convivem na mesma tabela, e cada uma devolve sempre o mesmo resultado — porque
nenhuma partição passada é reescrita.

---

## Premissas assumidas

- **Cancelamento** = `release_date` voltando a `NULL`. O enunciado cita "não
  cancelado" mas não há coluna de status nos exemplos.
- **Subsidiária ausente** vira `"não informada"` em vez de descartada, para não
  esconder receita real do relatório.
- **Reprocessamento full** a cada rodada, em vez de incremental por partição. O
  resultado é idêntico porque o pipeline é determinístico; a otimização
  (`{{ ds }}` nos jobs) vale quando o volume crescer.
