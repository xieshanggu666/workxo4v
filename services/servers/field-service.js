#!/usr/bin/env node
// 现场同步服务：移动端现场协同闭环的服务端门户
//  - 队伍注册 / 离线动作入队：按 推演 / 分支 / 队伍 三维隔离（本地 JSONL 持久化）
//  - 联网补传：动作按入队时钉住的分支分组，固定补传回原分支；回执落账（acked / conflict）
//  - 切换分支期间的在途补传：单队伍单在途，重复触发复用同一批；在途期间新入队的
//    动作不被裹挟进当前批次，落地后再补一轮
//  - 离线包代理：按分支给出预警 + 在途任务缓存（离线接收预警）
//  - 崩溃恢复：重启后队列状态从 JSONL 流水重建，未确认动作不丢；旧版无分支层队列自动迁移
import { createApp, ok, fail, listen, call, waitFor } from '../lib/http.js'
import { PORTS, DATA_DIR } from '../lib/config.js'
import { FieldOutbox } from '../lib/fieldOutbox.js'

const outbox = new FieldOutbox(`${DATA_DIR}/field`)
const inflight = new Map() // `${simId}__${teamId}` -> Promise（单队伍单在途补传）

const GW = (method, p, body) => call(method, PORTS.gateway, p, body)

const app = createApp([
  ['GET', '/healthz', async (req, res) => ok(res, { service: 'field' })],

  // 队伍注册（幂等）：登记即入队一条 registerTeam 动作并立即尝试同步
  ['POST', '/sims/:simId/teams/:teamId/register', async (req, res, { simId, teamId }, query, body) => {
    const r = outbox.enqueue(simId, teamId, {
      clientActionId: body.clientActionId || `reg-${teamId}`,
      kind: 'registerTeam', teamId,
      name: body.name, capabilities: body.capabilities || [],
      branchId: body.branchId,
      at: body.at
    })
    const sync = await syncTeam(simId, teamId)
    ok(res, { registered: true, duplicated: r.duplicated, branchId: r.action.branchId, sync })
  }],

  // 离线动作入队（断网时现场端本地也持有同样队列；联网后调用 /sync 补传）。
  // 每条动作钉住 branchId（动作级 > 请求级 > 默认 main；同 id 重提缺省钉住原分支）
  ['POST', '/sims/:simId/teams/:teamId/actions', async (req, res, { simId, teamId }, query, body) => {
    const list = Array.isArray(body.actions) ? body.actions : [body.action || body]
    if (!list.length || list.some((a) => !a?.kind)) {
      return fail(res, 400, 'bad-actions', 'actions 需为带 kind 的对象或数组')
    }
    const accepted = []
    for (const a of list) {
      try {
        const r = outbox.enqueue(simId, teamId, {
          ...a,
          teamId: a.teamId || teamId,
          branchId: a.branchId || body.branchId || undefined
        })
        accepted.push({
          clientActionId: r.action.clientActionId, branchId: r.action.branchId,
          status: r.action.status, duplicated: r.duplicated
        })
      } catch (e) {
        return fail(res, e.status || 400, e.code || 'bad-action', e.message)
      }
    }
    ok(res, { accepted, queued: outbox.pending(simId, teamId).length })
  }],

  // 队列/回执查询：可按分支过滤（默认全分支汇总，按 HLC 因果序）
  ['GET', '/sims/:simId/teams/:teamId/outbox', async (req, res, { simId, teamId }, { status, branch }) => {
    ok(res, { actions: outbox.list(simId, teamId, { status: status || undefined, branchId: branch || undefined }) })
  }],

  ['GET', '/sims/:simId/field-teams', async (req, res, { simId }) => {
    ok(res, { teams: outbox.listTeams(simId) })
  }],

  // 联网补传：待发动作按钉住分支分组，逐组固定补传回原分支；
  // body.branchId 可限定只补某一分支（其余分支动作继续留队）
  ['POST', '/sims/:simId/teams/:teamId/sync', async (req, res, { simId, teamId }, query, body) => {
    const r = await syncTeam(simId, teamId, { branchId: body?.branchId })
    ok(res, r)
  }],

  // 离线包（代理网关）：按分支拉取，断网前缓存 → 离线期间可查看预警/任务并继续入队动作
  ['GET', '/sims/:simId/teams/:teamId/bundle', async (req, res, { simId, teamId }, { branch }) => {
    const r = await GW('GET', `/sims/${encodeURIComponent(simId)}/field/bundle?branch=${encodeURIComponent(branch || 'main')}&teamId=${encodeURIComponent(teamId)}`)
    if (r.status !== 200) return fail(res, r.status, r.body?.code || 'bundle-failed', r.body?.msg || '离线包获取失败')
    ok(res, r.body)
  }]
])

// 同步一支队伍的待发动作：按动作入队时钉住的分支分组，逐组送网关（补传固定回原分支）。
// 单队伍单在途：切换分支 / 重复触发不并发补传；在途期间新入队的动作不进当前批次，
// 本轮落地后再补一轮（有界）。网关不可达时保留队列稍后重试（不丢动作）。
async function syncTeam(simId, teamId, { branchId } = {}) {
  const key = `${simId}__${teamId}`
  if (inflight.has(key)) return inflight.get(key)
  const run = (async () => {
    const totals = { synced: 0, results: [], branches: {} }
    for (let round = 0; round < 3; round++) {
      const groups = outbox.pendingByBranch(simId, teamId, { branchId })
      if (!groups.size) break
      let deferred = false
      for (const [br, actions] of groups) {
        // eslint-disable-next-line no-await-in-loop
        const r = await pushGroup(simId, teamId, br, actions)
        if (r.deferred) { deferred = true; totals.deferred = true; totals.reason = r.reason; continue }
        totals.synced += r.synced
        totals.results.push(...r.results)
        totals.branches[br] = {
          synced: r.synced,
          remaining: outbox.pending(simId, teamId, { branchId: br }).length,
          results: r.results
        }
      }
      // 网关不可达直接收兵；无剩余待发（含在途期间新入队的）则收兵，否则再补一轮
      if (deferred || !outbox.pending(simId, teamId, { branchId }).length) break
    }
    outbox.prune(simId, teamId)
    // remaining 报告全队待补传总量（含未在本次限定范围内的其它分支），
    // 让调用方知道切换分支后仍有 backlog；各分支明细见 branches
    totals.remaining = outbox.pending(simId, teamId).length
    return totals
  })()
  inflight.set(key, run)
  try { return await run } finally { inflight.delete(key) }
}

// 单分支分组补传：整批送网关（逐条应用），成功/冲突都落账
async function pushGroup(simId, teamId, branchId, pending) {
  const r = await GW('POST', `/sims/${encodeURIComponent(simId)}/field/actions?branch=${encodeURIComponent(branchId)}`, {
    clientId: teamId,
    actions: pending.map((a) => ({ ...a, teamId: a.teamId || teamId }))
  }).catch((e) => ({ status: 502, body: { code: 'gateway-down', msg: e.message } }))
  if (r.status !== 200) {
    pending.forEach((a) => outbox.mark(simId, teamId, a.clientActionId, { attempts: (a.attempts || 0) + 1 }))
    return { synced: 0, deferred: true, reason: r.body?.msg || `gateway ${r.status}`, results: [] }
  }
  const byId = new Map((r.body.results || []).map((x) => [x.clientActionId, x]))
  let synced = 0
  const results = []
  for (const a of pending) {
    const res = byId.get(a.clientActionId)
    if (!res) continue
    if (res.ok && res.applied !== false) {
      outbox.mark(simId, teamId, a.clientActionId, { status: 'acked', result: res, attempts: (a.attempts || 0) + 1 })
      synced++
    } else {
      // 事件入账但业务前置未满足（因果竞态）或快速失败：记冲突，待现场/指挥员处置，不自动重试
      outbox.mark(simId, teamId, a.clientActionId, { status: 'conflict', result: res, attempts: (a.attempts || 0) + 1 })
    }
    results.push({ clientActionId: a.clientActionId, branchId, kind: a.kind, ...pickResult(res) })
  }
  return { synced, results }
}

function pickResult(res) {
  return {
    ok: !!res.ok, applied: res.applied !== false,
    msg: res.msg || (res.advisory || []).join('；') || '',
    advisory: res.advisory || [], meta: res.meta || {}
  }
}

waitFor(PORTS.gateway, '/healthz', 100, 200).catch(() => null)
  .then(() => listen(app, PORTS.field, 'field'))
  .catch((e) => { console.error(e); process.exit(1) })
