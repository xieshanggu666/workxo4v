#!/usr/bin/env node
// 现场同步服务：移动端现场协同闭环的服务端门户
//  - 队伍注册 / 离线动作入队（本地持久化，断网期间现场端也可由网关直推）
//  - 联网补传：按 HLC 因果序逐条推送网关应用，回执落账（acked / conflict）
//  - 离线包代理：预警 + 在途任务 + 位置，供移动端断网前缓存（离线接收预警）
//  - 分支协同：动作按 simId/teamId/branchId 三级隔离，补传固定回动作产生时的
//    原分支；切换分支只切换"门户视角"，在途同步的回执按动作自带分支落账
//  - 崩溃恢复：重启后队列状态从 JSONL 流水重建，未确认动作不丢
import { createApp, ok, fail, listen, call, waitFor } from '../lib/http.js'
import { PORTS, DATA_DIR } from '../lib/config.js'
import { FieldOutbox, DEFAULT_BRANCH } from '../lib/fieldOutbox.js'

const outbox = new FieldOutbox(`${DATA_DIR}/field`)

// 每支队伍一把同步锁：切换分支 / 手动补传 / 注册触发的并发同步串行化，
// 避免同一批动作被两个在途请求重复提交（幂等能兜底，但省一次往返）
const syncLocks = new Map()
function withTeamLock(simId, teamId, fn) {
  const key = `${simId}__${teamId}`
  const prev = syncLocks.get(key) || Promise.resolve()
  const next = prev.then(fn, fn)
  syncLocks.set(key, next.finally(() => { if (syncLocks.get(key) === next) syncLocks.delete(key) }))
  return next
}

const GW = (method, p, body) => call(method, PORTS.gateway, p, body)

const app = createApp([
  ['GET', '/healthz', async (req, res) => ok(res, { service: 'field' })],

  // 队伍注册（幂等）：登记即入队一条 registerTeam 动作并立即尝试同步
  ['POST', '/sims/:simId/teams/:teamId/register', async (req, res, { simId, teamId }, query, body) => {
    const branchId = body.branchId || query.branch || DEFAULT_BRANCH
    const r = outbox.enqueue(simId, teamId, {
      clientActionId: body.clientActionId || `reg-${teamId}-${branchId}`,
      kind: 'registerTeam', teamId,
      name: body.name, capabilities: body.capabilities || [],
      at: body.at
    }, { branchId })
    const sync = await withTeamLock(simId, teamId, () => syncTeam(simId, teamId, { onlyBranchId: branchId }))
    ok(res, { registered: true, duplicated: r.duplicated, branchId, sync })
  }],

  // 离线动作入队（断网时现场端本地也持有同样队列；联网后调用 /sync 补传）。
  // branch 解析顺序：动作自带 branchId（客户端固定）> query.branch > main；
  // 同 id 重提改分支会被 outbox 以 branch-mismatch 拒绝（补传固定回原分支）
  ['POST', '/sims/:simId/teams/:teamId/actions', async (req, res, { simId, teamId }, query, body) => {
    const list = Array.isArray(body.actions) ? body.actions : [body.action || body]
    if (!list.length || list.some((a) => !a?.kind)) {
      return fail(res, 400, 'bad-actions', 'actions 需为带 kind 的对象或数组')
    }
    const accepted = []
    for (const a of list) {
      try {
        const branchId = a.branchId || query.branch || body.branchId || DEFAULT_BRANCH
        const r = outbox.enqueue(simId, teamId, { ...a, teamId: a.teamId || teamId }, { branchId })
        accepted.push({ clientActionId: r.action.clientActionId, status: r.action.status, branchId: r.action.branchId, duplicated: r.duplicated })
      } catch (e) {
        return fail(res, e.status || 400, e.code || 'bad-action', e.message, { actual: e.actual || null, expected: e.expected || null })
      }
    }
    const branchId = accepted[0]?.branchId || DEFAULT_BRANCH
    ok(res, { accepted, queued: outbox.pending(simId, teamId, branchId).length })
  }],

  // 队列查询：?branch= 单分支（默认 main）；?all=1 返回全分支（按分支分组）
  ['GET', '/sims/:simId/teams/:teamId/outbox', async (req, res, { simId, teamId }, { status, branch, all }) => {
    if (all != null) {
      const groups = outbox.pendingAcrossBranches(simId, teamId)
        .map((g) => ({ branchId: g.branchId, actions: status ? g.actions.filter((a) => a.status === status) : g.actions }))
      // pendingAcrossBranches 只返回有待发的分支；查看全部（含冲突/已闭环）时补齐
      const branches = new Set(outbox.listBranches(simId, teamId).map((b) => b.branchId))
      for (const b of branches) {
        if (!groups.some((g) => g.branchId === b)) {
          const actions = outbox.list(simId, teamId, b, status ? { status } : {})
          if (actions.length) groups.push({ branchId: b, actions })
        }
      }
      return ok(res, { groups, branches: outbox.listBranches(simId, teamId) })
    }
    const branchId = branch || DEFAULT_BRANCH
    ok(res, { branchId, actions: outbox.list(simId, teamId, branchId, status ? { status } : {}) })
  }],

  // 现场队伍当前在哪些分支上留有离线队列（切换页 / 门户视角用）
  ['GET', '/sims/:simId/teams/:teamId/branches', async (req, res, { simId, teamId }) => {
    ok(res, { branches: outbox.listBranches(simId, teamId) })
  }],

  // 切换现场队伍的"推演分支视角"：
  //  - 不搬运任何动作（动作固定在原分支，补传时各自回各自的分支）
  //  - 先排空旧分支视角下在途待发（尽力而为，网关不可达则保留待自动补传）
  //  - 返回新分支离线包，移动端立刻以新分支态势开展离线作业
  ['POST', '/sims/:simId/teams/:teamId/switch-branch', async (req, res, { simId, teamId }, query, body) => {
    const targetBranch = body.branchId || query.branch
    if (!targetBranch) return fail(res, 400, 'need-branch', '缺少 branchId')
    const r = await withTeamLock(simId, teamId, async () => {
      // 切换前把各分支待发动作补传一轮（固定回原分支）；失败不阻塞切换
      const drained = await syncTeam(simId, teamId, {}).catch((e) => ({ deferred: true, reason: e.message }))
      const bundle = await GW('GET',
        `/sims/${encodeURIComponent(simId)}/field/bundle?branch=${encodeURIComponent(targetBranch)}&teamId=${encodeURIComponent(teamId)}`)
      return { drained, bundleStatus: bundle.status, bundle: bundle.body, branches: outbox.listBranches(simId, teamId) }
    })
    if (r.bundleStatus !== 200) {
      // 分支不存在 / 网关不可达：允许离线切换（现场仍可在本地队列作业，稍后再取包）
      return ok(res, { switched: true, branchId: targetBranch, bundle: null, bundleError: r.bundle?.body?.msg || `bundle ${r.bundleStatus}`, drained: r.drained, branches: r.branches })
    }
    ok(res, { switched: true, branchId: targetBranch, ...r.bundle, drained: r.drained, branches: r.branches })
  }],

  ['GET', '/sims/:simId/field-teams', async (req, res, { simId }) => {
    ok(res, { teams: outbox.listTeams(simId) })
  }],

  // 联网补传：把 queued 动作按因果序（HLC）分组送各原分支网关，回执落账。
  // ?branch= 只补某分支；默认遍历该队全部分支（补传固定回原分支）
  ['POST', '/sims/:simId/teams/:teamId/sync', async (req, res, { simId, teamId }, query, body) => {
    const onlyBranchId = query.branch || body?.branchId || null
    const r = await withTeamLock(simId, teamId, () => syncTeam(simId, teamId, { onlyBranchId }))
    ok(res, r)
  }],

  // 离线包（代理网关）：断网前拉取缓存 → 离线期间可查看预警/任务并继续入队动作
  ['GET', '/sims/:simId/teams/:teamId/bundle', async (req, res, { simId, teamId }, { branch }) => {
    const branchId = branch || DEFAULT_BRANCH
    const r = await GW('GET', `/sims/${encodeURIComponent(simId)}/field/bundle?branch=${encodeURIComponent(branchId)}&teamId=${encodeURIComponent(teamId)}`)
    if (r.status !== 200) return fail(res, r.status, r.body?.code || 'bundle-failed', r.body?.msg || '离线包获取失败')
    ok(res, r.body)
  }]
])

// 同步一支队伍的待发动作：按动作产生时的分支分组，每组一批推网关对应分支
// （网关按最新态势校验 + 稳定 id 幂等），成功/冲突都落账；
// 网关不可达时保留队列稍后重试（不丢动作）。
// 在途同步期间即使现场切换了分支视角，回执也按动作自带 branchId 写回原分支。
async function syncTeam(simId, teamId, { onlyBranchId = null } = {}) {
  const groups = outbox.pendingAcrossBranches(simId, teamId, onlyBranchId)
  if (!groups.length) return { synced: 0, remaining: countPending(simId, teamId), results: [], branches: [] }
  const results = []
  const branchSummaries = []
  let synced = 0
  for (const group of groups) {
    const { branchId, actions: pending } = group
    // 批量提交（网关逐条应用），单批失败按连接故障处理：该分支整体保留重试
    // eslint-disable-next-line no-await-in-loop
    const r = await GW('POST', `/sims/${encodeURIComponent(simId)}/field/actions?branch=${encodeURIComponent(branchId)}`, {
      clientId: teamId,
      actions: pending.map((a) => ({ ...a, teamId: a.teamId || teamId, branchId }))
    }).catch((e) => ({ status: 502, body: { code: 'gateway-down', msg: e.message } }))
    if (r.status !== 200) {
      pending.forEach((a) => outbox.mark(simId, teamId, a.clientActionId, { attempts: (a.attempts || 0) + 1 }))
      branchSummaries.push({ branchId, synced: 0, remaining: pending.length, deferred: true, reason: r.body?.msg || `gateway ${r.status}` })
      continue
    }
    const byId = new Map((r.body.results || []).map((x) => [x.clientActionId, x]))
    let branchSynced = 0
    for (const a of pending) {
      const res = byId.get(a.clientActionId)
      if (!res) continue
      if (res.ok && res.applied !== false) {
        outbox.mark(simId, teamId, a.clientActionId, { status: 'acked', result: res, attempts: (a.attempts || 0) + 1 })
        branchSynced++; synced++
      } else if (res.ok && res.applied === false) {
        // 事件入账但业务前置未满足（因果竞态）：记冲突，待现场/指挥员处置，不自动重试
        outbox.mark(simId, teamId, a.clientActionId, { status: 'conflict', result: res, attempts: (a.attempts || 0) + 1 })
      } else {
        outbox.mark(simId, teamId, a.clientActionId, { status: 'conflict', result: res, attempts: (a.attempts || 0) + 1 })
      }
      results.push({ clientActionId: a.clientActionId, kind: a.kind, branchId, ...pickResult(res) })
    }
    outbox.prune(simId, teamId, branchId)
    branchSummaries.push({ branchId, synced: branchSynced, remaining: outbox.pending(simId, teamId, branchId).length })
  }
  return { synced, remaining: countPending(simId, teamId), results, branches: branchSummaries }
}

function countPending(simId, teamId) {
  return outbox.listBranches(simId, teamId).reduce((n, b) => n + b.queued, 0)
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
