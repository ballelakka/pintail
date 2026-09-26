import mysql from 'mysql2/promise'
import { until, type Context, type Scenario } from '../harness'

/** docs/limitations.md:327 and supervisor.rs:48: CDC purge recovery: once per invocation, from a fresh source snapshot. */
export async function purgeWhileDown(ctx: Context) {
  await ctx.stop()
  const old = ctx.durable('before-purge').checkpoints[0]
  const before = ctx.commits
  await until('committed writes beyond captured position', async () => ctx.commits >= before + 10)
  await ctx.sql('FLUSH BINARY LOGS; FLUSH BINARY LOGS')
  const file = String((await ctx.rows('SHOW BINARY LOG STATUS'))[0][0])
  await ctx.sql(`PURGE BINARY LOGS TO '${file}'`)
  const retained = await ctx.rows('SHOW BINARY LOGS')
  ctx.check('purge:required-file-is-gone', !retained.some(row => row[0] === old.binlog_file))
}
export function partialIsNotHealthy(ctx: Context, label: string, scope: 'database' | 'table' = 'database') {
  const state = ctx.durable(label)
  const incomplete = new Set(state.snapshot_chunks.filter(c => c.status !== 'completed').map(c => c.table_name))
  for (const table of state.tables) if (!table.copy_complete) incomplete.add(table.name)
  ctx.check(`partial-copy:not-healthy:${label}`, state.tables.every(t => !incomplete.has(t.name) || !['streaming','polling'].includes(t.state)))
  // A one-table repair leaves the database live for its other tables.
  if (incomplete.size && scope === 'database') ctx.check(`partial-database:not-healthy:${label}`, !['streaming','polling'].includes(state.databases[0]?.state))
}
async function recover(ctx: Context, faults: string[]) {
  await purgeWhileDown(ctx)
  for (const [index, fault] of faults.entries()) {
    await ctx.start(fault)
    if (index === 0) await ctx.diagnostic(/cdc\.resnapshot .*unavailable source position/)
    await ctx.fired(fault.split('@')[0])
    partialIsNotHealthy(ctx, `after-${fault.replaceAll('@', '-')}`)
  }
  await ctx.start()
  if (!faults.length) await ctx.diagnostic(/cdc\.resnapshot .*unavailable source position/)
  await until('purge recovery returns to streaming', async () => (await ctx.status()).state === 'streaming')
}
/** A source rebuilt by an upgrade or a restore renumbers its binlogs but keeps every transaction; the GTID set alone says where to resume. */
async function renumberedBinlogs(ctx: Context) {
  await ctx.sql('FLUSH BINARY LOGS; FLUSH BINARY LOGS')
  const before = ctx.commits
  await until('the stream checkpoints past the flushed files', async () => ctx.commits >= before + 20)
  // Caught up first, as a source taken down for an upgrade leaves it: a
  // transaction the stream had not read when the old files went is truly
  // lost, and 1236 is then the right answer.
  await ctx.stopChurn()
  await ctx.converge('before-renumber')
  await ctx.stop()
  const old = ctx.durable('before-renumber').checkpoints[0]
  const executed = String((await ctx.rows('SELECT @@GLOBAL.gtid_executed'))[0][0])
  await ctx.sql(`RESET BINARY LOGS AND GTIDS; SET GLOBAL gtid_purged = ${mysql.escape(executed)}`)
  const retained = (await ctx.rows('SHOW BINARY LOGS')).map(row => String(row[0]))
  ctx.check('renumber:checkpoint-file-is-gone', !retained.includes(old.binlog_file), `${old.binlog_file} -> ${retained.join(',')}`)
  const history = String((await ctx.rows('SELECT @@GLOBAL.gtid_executed'))[0][0])
  ctx.check('renumber:history-is-kept', history === executed, history)
  await ctx.startChurn()
  await ctx.start()
  const resumed = ctx.commits
  await until('writes after the renumbering', async () => ctx.commits >= resumed + 20)
  await ctx.stopChurn()
  await ctx.converge('after-renumber')
  ctx.noDiagnostic(/cdc\.resnapshot/)
  await ctx.startChurn()
}
/** A source rebuilt under a new server identity numbers from one; a change versioned below the stored rows would be applied and lost, so the stream recopies. */
async function newIdentity(ctx: Context) {
  const rebuilt = 'e1d2c3b4-a5f6-4789-8abc-0000000009a1'
  await ctx.sql(`SET gtid_next = '${rebuilt}:1'; START TRANSACTION; UPDATE accounts SET owner = 'rebuilt-source' WHERE id = 2; COMMIT; SET gtid_next = 'AUTOMATIC'`)
  await ctx.diagnostic(/cdc\.resnapshot .*at or below the \d+ already stored/)
  await until('recopy returns to streaming', async () => (await ctx.status()).state === 'streaming')
}
export const purgeScenarios: Scenario[] = [
  { slug: 'renumbered-binlogs-resume', area: 'purge', promise: 'crates/pintail-cdc/src/lib.rs: a GTID resume names no file', run: renumberedBinlogs },
  { slug: 'new-source-identity-recopies', area: 'purge', promise: 'crates/pintail-cdc/src/lib.rs: a transaction versioned below the stored rows is refused', run: newIdentity },
  { slug: 'purge-auto-resnapshot', area: 'purge', promise: 'docs/limitations.md: automatic purge recovery', run: ctx => recover(ctx, []) },
  { slug: 'purge-resnapshot-abort-once', area: 'purge', promise: 'crates/pintail-api/src/supervisor.rs: interrupted copy recovery', run: ctx => recover(ctx, ['snapshot.chunk.after_ingest@2']) },
  { slug: 'purge-resnapshot-abort-twice', area: 'purge', promise: 'docs/limitations.md: purge recovery once per runner invocation', run: ctx => recover(ctx, ['snapshot.chunk.after_ingest@2', 'snapshot.table.before_complete']) },
  { slug: 'purge-resnapshot-position-abort', area: 'purge', promise: 'crates/pintail-cdc/src/lib.rs: durable resnapshot handoff', run: ctx => recover(ctx, ['cdc.resnapshot.after_targets']) },
]
