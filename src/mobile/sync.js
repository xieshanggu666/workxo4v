/* =========================================================================
 * 现场移动端同步引擎（零依赖，localStorage 持久化）
 *
 *  - 分支协同：队伍可切换推演分支开展离线作业；离线包 / 动作 / 回执按
 *    「推演 + 分支 + 队伍」隔离。动作入队即钉住当时分支，补传固定回原分支，
 *    切换分支不裹挟在途与待发动作
 *  - 离线包：按分支缓存「生效预警 + 在途签收单 + 抢修工单 + 阻断/队伍位置」，
 *    断网期间可正常查看（离线接收预警）
 *  - 离线动作：每个动作生成稳定 clientActionId + 客户端 HLC，先落本地队列
 *    （queued），联网后按分支分组、组内按 HLC 因果顺序补传；每条动作独立回执
 *  - 切换期间在途请求：切换分支前等待在途补传落地，回执按 clientActionId 归账，
 *    不会错记到新分支动作上；单在途去重，重复触发复用同一批
 *  - 冲突处理：前置校验拒绝 / reducer 因果竞态 → conflict，不阻塞后续动作；
 *    修正后重提仍钉住原分支（冲突成因在原分支态势上），可查看原因或丢弃
 *  - 补传幂等：同 clientActionId 重试由网关生成确定性事件 id，历史服务去重
 *  - 旧队列迁移：field-sync-v1（无分支维度）自动升级，历史动作归入 main 分支
 * ========================================================================= */

const LS_KEY = 'field-sync-v2'
const LEGACY_KEY = 'field-sync-v1'
const MAIN = 'main'

// HLC（与服务端 services/lib/causal.js 同构，字符串字典序即因果序）
export function hlcEncode(h) {
  return h.ts.toString(16).padStart(12, '0') + '-' + h.l.toString(16).padStart(6, '0')
}
export function hlcDecode(s) {
  if (!s || typeof s !== 'string' || !s.includes('-')) return null
  const [ts, l] = s.split('-')
  const n = { ts: parseInt(ts, 16), l: parseInt(l, 16) }
  return Number.isFinite(n.ts) && Number.isFinite(n.l) ? n : null
}
function hlcReceive(local, remote) {
  const now = Date.now()
  const ts = Math.max(local?.ts || 0, remote?.ts || 0, now)
  let l = 0
  if (ts === local?.ts && ts === remote?.ts) l = Math.max(local.l, remote.l) + 1
  else if (ts === local?.ts) l = local.l + 1
  else if (ts === remote?.ts) l = remote.l + 1
  return { ts, l }
}

function blankStore() {
  return {
    simId: '', teamId: '', teamName: '', branchId: MAIN,
    clock: { ts: 0, l: 0 },
    queue: [],      // 每条动作钉住 branchId（入队时所在分支）
    bundles: {},    // branchId -> { bundle, bundleAt }（离线包按分支缓存）
    lastSyncAt: null
  }
}

// 旧版（v1，无分支维度）队列迁移：历史动作全部归入 main，离线包挂到 main 缓存
function migrateV1(legacy) {
  return {
    ...blankStore(),
    simId: legacy.simId || '', teamId: legacy.teamId || '', teamName: legacy.teamName || '',
    clock: legacy.clock || { ts: 0, l: 0 },
    queue: (Array.isArray(legacy.queue) ? legacy.queue : []).map((a) => ({ ...a, branchId: a.branchId || MAIN })),
    bundles: legacy.bundle ? { [MAIN]: { bundle: legacy.bundle, bundleAt: legacy.bundleAt || null } } : {},
    lastSyncAt: legacy.lastSyncAt || null
  }
}

function loadStore() {
  let raw = null
  try { raw = JSON.parse(localStorage.getItem(LS_KEY) || 'null') } catch { raw = null }
  if (!raw) {
    let legacy = null
    try { legacy = JSON.parse(localStorage.getItem(LEGACY_KEY) || 'null') } catch { legacy = null }
    if (legacy) {
      raw = migrateV1(legacy)
      try { localStorage.removeItem(LEGACY_KEY) } catch { /* 忽略 */ }
    }
  }
  const s = { ...blankStore(), ...(raw || {}) }
  // 归一化：动作缺分支归 main；bundles 必须是对象
  s.queue = (Array.isArray(s.queue) ? s.queue : []).map((a) => ({ ...a, branchId: a.branchId || MAIN }))
  s.bundles = s.bundles && typeof s.bundles === 'object' && !Array.isArray(s.bundles) ? s.bundles : {}
  s.branchId = s.branchId || MAIN
  return s
}

export class FieldClient {
  constructor(onChange) {
    this.online = navigator.onLine
    this.s = loadStore()
    this.syncing = false
    this._inflight = null // 单在途补传：切换分支 / 重复触发复用同一批
    this.onChange = onChange || (() => {})
    window.addEventListener('online', () => { this.online = true; this.sync(); this.fetchBundle() })
    window.addEventListener('offline', () => { this.online = false; this._emit() })
  }

  _persist() { localStorage.setItem(LS_KEY, JSON.stringify(this.s)) }
  _emit() { this.onChange() }

  configure({ simId, teamId, teamName, branchId }) {
    this.s.simId = simId; this.s.teamId = teamId; this.s.teamName = teamName || ''
    this.s.branchId = branchId || MAIN
    this._persist(); this._emit()
  }
  get configured() { return !!(this.s.simId && this.s.teamId) }

  // 切换作业分支：在途补传先落地（回执按 id 归账到原分支动作，不错记）；
  // 已入队动作仍钉住原分支、联网后固定补传回去；此后新动作进入新分支
  async switchBranch(branchId) {
    const next = String(branchId || '').trim() || MAIN
    if (next === this.s.branchId) return { switched: false, branchId: next }
    if (this._inflight) await this._inflight.catch(() => {})
    this.s.branchId = next
    this._persist(); this._emit()
    if (this.online && this.configured) await this.fetchBundle().catch(() => {})
    return { switched: true, branchId: next }
  }

  _tick(remoteHlc = null) {
    const now = Date.now()
    if (remoteHlc) this.s.clock = hlcReceive(this.s.clock, hlcDecode(remoteHlc) || { ts: 0, l: 0 })
    else this.s.clock = now > this.s.clock.ts ? { ts: now, l: 0 } : { ts: this.s.clock.ts, l: this.s.clock.l + 1 }
    return hlcEncode(this.s.clock)
  }

  nowHHMM(d = new Date()) {
    return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`
  }

  // 入队一条离线动作（立即可用，无网时只落本地）；动作钉住当前分支
  enqueue(kind, payload = {}) {
    const id = payload.clientActionId ||
      `${kind}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`
    const action = {
      ...payload,
      clientActionId: id,
      kind,
      teamId: this.s.teamId,
      branchId: payload.branchId || this.s.branchId || MAIN,
      hlc: this._tick(),
      at: payload.at || this.nowHHMM(),
      status: 'queued',
      queuedAt: new Date().toISOString(),
      attempts: 0,
      result: null
    }
    // 同 id 重提（修正冲突后重发）：保留原 HLC，只清状态
    const idx = this.s.queue.findIndex((a) => a.clientActionId === id)
    if (idx >= 0) this.s.queue[idx] = action
    else this.s.queue.push(action)
    this._persist(); this._emit()
    if (this.online && this.configured) this.sync()
    return action
  }

  // 放弃一条冲突/待发动作（现场确认无法处理）
  discard(id) {
    this.s.queue = this.s.queue.filter((a) => a.clientActionId !== id)
    this._persist(); this._emit()
  }

  // 修正后重提：用同一业务参数生成新动作（新 clientActionId），原冲突项丢弃。
  // rest 继承原动作的 branchId —— 冲突重提固定回原分支（冲突成因在原分支态势上）
  retryWith(id, patch) {
    const old = this.s.queue.find((a) => a.clientActionId === id)
    if (!old) return
    this.discard(id)
    const { clientActionId, hlc, queuedAt, status, attempts, result, ...rest } = old
    return this.enqueue(old.kind, { ...rest, ...patch })
  }

  pending() { return this.s.queue.filter((a) => a.status === 'queued') }
  conflicts() { return this.s.queue.filter((a) => a.status === 'conflict') }
  acked() { return this.s.queue.filter((a) => a.status === 'acked') }

  // 当前分支的离线包缓存（切分支后各分支缓存互不覆盖）
  get bundle() { return this.s.bundles[this.s.branchId]?.bundle || null }
  get bundleAt() { return this.s.bundles[this.s.branchId]?.bundleAt || null }

  async _api(path, options) {
    const resp = await fetch(path, options)
    const body = await resp.json().catch(() => ({}))
    if (!resp.ok || body.ok === false) {
      const err = new Error(body.msg || `HTTP ${resp.status}`)
      err.status = resp.status; err.body = body
      throw err
    }
    return body
  }

  // 拉取指定分支（默认当前分支）离线包并缓存；服务端事件 hlc 带回，保证因果不回退
  async fetchBundle(branchId = this.s.branchId) {
    if (!this.configured) return
    const br = branchId || MAIN
    const r = await this._api(`/sims/${encodeURIComponent(this.s.simId)}/field/bundle?branch=${encodeURIComponent(br)}&teamId=${encodeURIComponent(this.s.teamId)}`)
    if (r.bundle) {
      this.s.bundles = { ...this.s.bundles, [br]: { bundle: r.bundle, bundleAt: r.bundle.serverAt } }
      this._persist(); this._emit()
    }
    return r
  }

  // 分支列表（切换分支时供选择）
  async listBranches() {
    if (!this.configured) return []
    const r = await this._api(`/sims/${encodeURIComponent(this.s.simId)}/branches`)
    return r.branches || []
  }

  // 联网补传：单在途去重；按钉住分支分组，逐组固定补传回原分支
  async sync() {
    if (!this.configured) return { skipped: true }
    if (this._inflight) return this._inflight
    this._inflight = this._doSync().finally(() => { this._inflight = null })
    return this._inflight
  }

  async _doSync() {
    const totals = { synced: 0, results: [] }
    if (!this.s.queue.some((a) => a.status === 'queued')) { this._emit(); return totals }
    this.syncing = true; this._emit()
    try {
      for (let round = 0; round < 3; round++) {
        // 按入队时钉住的分支分组：每组固定补传回原分支，组内 HLC 因果序
        const groups = new Map()
        for (const a of this.s.queue.filter((x) => x.status === 'queued')) {
          const br = a.branchId || MAIN
          if (!groups.has(br)) groups.set(br, [])
          groups.get(br).push(a)
        }
        if (!groups.size) break
        let hitError = false
        for (const [br, actions] of groups) {
          actions.sort((x, y) => ((x.hlc || '') + '|' + x.clientActionId < (y.hlc || '') + '|' + y.clientActionId ? -1 : 1))
          // eslint-disable-next-line no-await-in-loop
          const r = await this._pushGroup(br, actions).catch((e) => ({ error: e.message }))
          if (r.error) { hitError = true; totals.error = r.error; continue } // 网络故障：动作保留 queued，联网恢复后自动重补
          totals.synced += r.synced
          totals.results.push(...r.results)
        }
        this._persist()
        // 在途期间新入队的动作（含切分支后）再补一轮；出错或有界收兵
        if (hitError || !this.s.queue.some((a) => a.status === 'queued')) break
      }
      this.s.lastSyncAt = new Date().toISOString()
      this._persist()
      await this.fetchBundle().catch(() => {})
      this._emit()
      return totals
    } finally {
      this.syncing = false; this._emit()
    }
  }

  // 单分支分组补传：逐条回执（冲突不阻塞后续）
  async _pushGroup(branchId, queued) {
    const r = await this._api(`/sims/${encodeURIComponent(this.s.simId)}/field/actions?branch=${encodeURIComponent(branchId)}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ clientId: this.s.teamId, actions: queued })
    })
    const byId = new Map((r.results || []).map((x) => [x.clientActionId, x]))
    let synced = 0
    for (const a of queued) {
      const res = byId.get(a.clientActionId)
      const item = this.s.queue.find((x) => x.clientActionId === a.clientActionId)
      if (!item || !res) continue
      item.attempts += 1
      if (res.ok && res.applied !== false) { item.status = 'acked'; synced++ }
      else item.status = 'conflict'
      item.result = { code: res.code, msg: res.msg, advisory: res.advisory || [], meta: res.meta || {} }
    }
    return { synced, results: r.results || [] }
  }
}
