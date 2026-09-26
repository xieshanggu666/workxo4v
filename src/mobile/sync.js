/* =========================================================================
 * 现场移动端同步引擎（零依赖，localStorage 持久化）
 *
 *  - 分支协同：现场队伍可切换推演分支开展离线作业。离线包 / 离线动作 / 补传
 *    回执按「推演 × 分支 × 队伍」三级隔离；动作入队时固定分支，补传永远
 *    回到产生动作的原分支（切换分支不搬运动作）
 *  - 离线包：从网关拉取「生效预警 + 在途签收单 + 抢修工单 + 阻断/队伍位置」，
 *    断网前缓存，断网期间可正常查看（离线接收预警）
 *  - 离线动作：每个动作生成稳定 clientActionId + 客户端 HLC，先落本地队列
 *    （queued），联网后按 HLC 因果顺序批量补传；每条动作独立回执
 *  - 在途切换：同步按分支快照执行，切换分支期间未完成的请求其回执仍写回
 *    原分支队列；切换后新分支另起一轮同步，两不串账
 *  - 冲突处理：前置校验拒绝 / reducer 因果竞态 → conflict，不阻塞后续动作；
 *    修正重提固定回原分支，现场确认改投时才可显式移动到当前分支
 *  - 补传幂等：同 clientActionId 重试由网关生成确定性事件 id，历史服务去重
 *  - 旧版（无分支隔离）本地队列首次加载时迁移到 main 分支
 * ========================================================================= */

const LS_KEY = 'field-sync-v2'
const LEGACY_LS_KEY = 'field-sync-v1'
const DEFAULT_BRANCH = 'main'

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

function freshStore() {
  return {
    v: 2,
    simId: '', teamId: '', teamName: '',
    currentBranch: DEFAULT_BRANCH,
    clock: { ts: 0, l: 0 },
    branches: {} // branchId -> { queue, bundle, bundleAt, lastSyncAt }
  }
}

function branchState(s, branchId) {
  if (!s.branches[branchId]) {
    s.branches[branchId] = { queue: [], bundle: null, bundleAt: null, lastSyncAt: null }
  }
  return s.branches[branchId]
}

function loadStore() {
  let raw = null
  try { raw = JSON.parse(localStorage.getItem(LS_KEY) || 'null') } catch { raw = null }
  if (raw && raw.v === 2) {
    const s = { ...freshStore(), ...raw }
    if (!s.branches || typeof s.branches !== 'object') s.branches = {}
    branchState(s, s.currentBranch || DEFAULT_BRANCH)
    return s
  }
  // 迁移旧版（v1：无分支隔离，单队列/单离线包）→ 全部归入 main
  const s = freshStore()
  let legacy = raw
  if (!legacy) {
    try { legacy = JSON.parse(localStorage.getItem(LEGACY_LS_KEY) || 'null') } catch { legacy = null }
  }
  if (legacy && legacy.simId) {
    s.simId = legacy.simId || ''
    s.teamId = legacy.teamId || ''
    s.teamName = legacy.teamName || ''
    s.clock = legacy.clock || { ts: 0, l: 0 }
    const main = branchState(s, DEFAULT_BRANCH)
    main.queue = Array.isArray(legacy.queue) ? legacy.queue.map((a) => ({ ...a, branchId: DEFAULT_BRANCH })) : []
    main.bundle = legacy.bundle || null
    main.bundleAt = legacy.bundleAt || null
    main.lastSyncAt = legacy.lastSyncAt || null
  }
  return s
}

export class FieldClient {
  constructor(onChange) {
    this.online = typeof navigator !== 'undefined' ? navigator.onLine : true
    this.s = loadStore()
    // 每个分支一把在途锁：同分支并发 sync 复用同一 Promise（切换期间的
    // 在途请求不被取消，回执按开同步时快照的分支写回原队列）
    this.inFlight = new Map() // branchId -> Promise
    this.onChange = onChange || (() => {})
    if (typeof window !== 'undefined') {
      window.addEventListener('online', () => { this.online = true; this.syncAll(); this.fetchBundle() })
      window.addEventListener('offline', () => { this.online = false; this._emit() })
    }
    this._persist()
  }

  _persist() { try { localStorage.setItem(LS_KEY, JSON.stringify(this.s)) } catch { /* 配额满时保留内存态 */ } }
  _emit() { this.onChange() }

  configure({ simId, teamId, teamName }) {
    this.s.simId = simId; this.s.teamId = teamId; this.s.teamName = teamName || ''
    if (!this.s.currentBranch) this.s.currentBranch = DEFAULT_BRANCH
    branchState(this.s, this.s.currentBranch)
    this._persist(); this._emit()
  }
  get configured() { return !!(this.s.simId && this.s.teamId) }
  get branchId() { return this.s.currentBranch || DEFAULT_BRANCH }
  _bs(branchId = this.branchId) { return branchState(this.s, branchId) }

  // 本地留有队列 / 离线包的分支（切换页用）
  knownBranches() { return Object.keys(this.s.branches) }
  branchSummary(branchId) {
    const bs = this._bs(branchId)
    return {
      branchId,
      queued: bs.queue.filter((a) => a.status === 'queued').length,
      conflict: bs.queue.filter((a) => a.status === 'conflict').length,
      acked: bs.queue.filter((a) => a.status === 'acked').length,
      bundleAt: bs.bundleAt
    }
  }
  allBranchSummaries() { return this.knownBranches().map((b) => this.branchSummary(b)) }

  _tick(remoteHlc = null) {
    const now = Date.now()
    if (remoteHlc) this.s.clock = hlcReceive(this.s.clock, hlcDecode(remoteHlc) || { ts: 0, l: 0 })
    else this.s.clock = now > this.s.clock.ts ? { ts: now, l: 0 } : { ts: this.s.clock.ts, l: this.s.clock.l + 1 }
    return hlcEncode(this.s.clock)
  }

  nowHHMM(d = new Date()) {
    return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`
  }

  // 入队一条离线动作（立即可用，无网时只落本地）。
  // 动作固定在"入队时的当前分支"；同 id 重提不允许换分支（防误投）。
  enqueue(kind, payload = {}, branchId = this.branchId) {
    const id = payload.clientActionId ||
      `${kind}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`
    const existing = this._find(id)
    if (existing && existing.action.branchId !== branchId) {
      const err = new Error(`动作固定在分支 ${existing.action.branchId}，不能改提到 ${branchId}；如需改投请用 requeueForBranch`)
      err.code = 'branch-mismatch'; err.actual = existing.action.branchId; err.expected = branchId
      throw err
    }
    const action = {
      ...payload,
      clientActionId: id,
      kind,
      teamId: this.s.teamId,
      branchId,
      hlc: existing?.action.hlc || this._tick(),
      at: payload.at || this.nowHHMM(),
      status: 'queued',
      queuedAt: new Date().toISOString(),
      attempts: 0,
      result: null
    }
    // 同 id 重提（修正冲突后重发）：保留原 HLC / 原分支，只清状态
    const bs = this._bs(branchId)
    const idx = bs.queue.findIndex((a) => a.clientActionId === id)
    if (idx >= 0) bs.queue[idx] = action
    else bs.queue.push(action)
    this._persist(); this._emit()
    if (this.online && this.configured) this.sync(branchId)
    return action
  }

  // 放弃一条冲突/待发动作（现场确认无法处理）
  discard(id) {
    const hit = this._find(id)
    if (!hit) return
    const bs = this._bs(hit.action.branchId)
    bs.queue = bs.queue.filter((a) => a.clientActionId !== id)
    this._persist(); this._emit()
  }

  // 修正后重提：用同一业务参数生成新动作（新 clientActionId），原冲突项丢弃。
  // 默认固定回原分支（冲突重提不换分支）；显式传 branchId 才改投。
  retryWith(id, patch, branchId) {
    const hit = this._find(id)
    if (!hit) return
    const target = branchId || hit.action.branchId
    const old = hit.action
    this.discard(id)
    const { clientActionId, hlc, queuedAt, status, attempts, result, ...rest } = old
    return this.enqueue(old.kind, { ...rest, ...patch }, target)
  }

  // 现场确认动作本应在另一分支执行：显式改投（留痕 movedFrom）
  requeueForBranch(id, newBranchId, patch = {}) {
    const hit = this._find(id)
    if (!hit || hit.action.branchId === newBranchId) return
    return this.retryWith(id, { movedFrom: { clientActionId: id, branchId: hit.action.branchId, at: new Date().toISOString() }, ...patch }, newBranchId)
  }

  _find(id) {
    for (const branchId of this.knownBranches()) {
      const bs = this._bs(branchId)
      const idx = bs.queue.findIndex((a) => a.clientActionId === id)
      if (idx >= 0) return { action: bs.queue[idx], idx, branchId, bs }
    }
    return null
  }

  pending(branchId) { return this._filter('queued', branchId) }
  conflicts(branchId) { return this._filter('conflict', branchId) }
  acked(branchId) { return this._filter('acked', branchId) }
  _filter(status, branchId) {
    const branches = branchId ? [branchId] : this.knownBranches()
    return branches.flatMap((b) => this._bs(b).queue.filter((a) => a.status === status))
  }

  get syncing() { return this.inFlight.size > 0 }
  isSyncing(branchId) { return this.inFlight.has(branchId) }

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

  // 切换现场队伍的推演分支视角：
  //  - 不等待、不取消旧分支的在途同步（其回执按快照写回旧分支）
  //  - 离线也能切（先以本地缓存作业，联网后自动补传/取包）
  //  - 切换后新分支另起一轮：先补传该分支待发，再拉新分支离线包
  async switchBranch(branchId, { kick = true } = {}) {
    if (!branchId) throw new Error('缺少 branchId')
    if (branchId === this.branchId) return { same: true }
    this.s.currentBranch = branchId
    branchState(this.s, branchId)
    this._persist(); this._emit()
    if (kick && this.online && this.configured) {
      // 不 await：UI 立即响应；后台同步失败仅落账
      this.sync(branchId).catch(() => {})
      this.fetchBundle(branchId).catch(() => {})
    }
    return { switched: true, branchId }
  }

  // 拉取离线包并推进本地 HLC（服务端事件 hlc 带回，保证因果不回退）
  async fetchBundle(branchId = this.branchId) {
    if (!this.configured) return
    const r = await this._api(`/sims/${encodeURIComponent(this.s.simId)}/field/bundle?branch=${encodeURIComponent(branchId)}&teamId=${encodeURIComponent(this.s.teamId)}`)
    if (r.bundle) {
      const bs = this._bs(branchId)
      bs.bundle = r.bundle
      bs.bundleAt = r.bundle.serverAt
      this._persist(); this._emit()
    }
    return r
  }

  // 联网补传单个分支：按 HLC 因果序批量送网关；逐条回执（冲突不阻塞后续）。
  // 关键：开头快照分支与待发列表——切换分支期间本请求仍在途时，回执写回
  // 快照分支，绝不落到切换后的当前分支。
  sync(branchId = this.branchId) {
    const running = this.inFlight.get(branchId)
    if (running) return running
    const p = this._syncBranch(branchId)
      .finally(() => { if (this.inFlight.get(branchId) === p) this.inFlight.delete(branchId); this._emit() })
    this.inFlight.set(branchId, p)
    this._emit()
    return p
  }

  async _syncBranch(branchId) {
    if (!this.configured) return { skipped: true }
    const bs = this._bs(branchId) // 快照分支对象：在途切换不影响回执归属
    const queued = bs.queue
      .filter((a) => a.status === 'queued')
      .sort((a, b) => ((a.hlc || '') + '|' + a.clientActionId < (b.hlc || '') + '|' + b.clientActionId ? -1 : 1))
    if (!queued.length) { this._emit(); return { synced: 0, branchId } }
    try {
      const r = await this._api(`/sims/${encodeURIComponent(this.s.simId)}/field/actions?branch=${encodeURIComponent(branchId)}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ clientId: this.s.teamId, branchId, actions: queued })
      })
      const byId = new Map((r.results || []).map((x) => [x.clientActionId, x]))
      for (const a of queued) {
        const res = byId.get(a.clientActionId)
        const item = bs.queue.find((x) => x.clientActionId === a.clientActionId)
        if (!item || !res) continue
        item.attempts += 1
        if (res.ok && res.applied !== false) item.status = 'acked'
        else item.status = 'conflict'
        item.result = { code: res.code, msg: res.msg, advisory: res.advisory || [], meta: res.meta || {} }
      }
      bs.lastSyncAt = new Date().toISOString()
      this._persist()
      await this.fetchBundle(branchId).catch(() => {})
      this._emit()
      return { synced: (r.results || []).filter((x) => x.ok && x.applied !== false).length, results: r.results, branchId }
    } catch (e) {
      // 网络故障：动作保留 queued，联网恢复后自动重补（仍补向原分支）
      this._persist(); this._emit()
      return { error: e.message, branchId }
    }
  }

  // 联网恢复 / 手动一键补传：逐分支补传，每个分支固定回各自原分支
  async syncAll() {
    if (!this.configured || this._syncingAll) return { skipped: true }
    this._syncingAll = true
    try {
      const out = []
      for (const branchId of this.knownBranches()) {
        if (this.pending(branchId).length) {
          // eslint-disable-next-line no-await-in-loop
          out.push(await this.sync(branchId))
        }
      }
      return { branches: out }
    } finally {
      this._syncingAll = false
    }
  }
}
