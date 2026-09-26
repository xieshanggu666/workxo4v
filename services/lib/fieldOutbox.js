import fs from 'node:fs'
import path from 'node:path'
import { ensureDir } from './storage.js'
import { hlcNow, hlcEncode, hlcDecode, hlcReceive } from './causal.js'

/* =========================================================================
 * FieldOutbox —— 现场端离线动作队列（也可作为现场同步服务的服务端持久层）
 *
 * 文件布局（按 simId / teamId / branchId 三级隔离——切换推演分支开展离线
 * 作业时，离线动作、补传回执、本地 HLC 全部落在分支目录内互不污染）：
 *   <root>/<simId>/<teamId>/<branchId>/queue.jsonl   动作流水（queued → sent → acked / conflict）
 *   <root>/<simId>/<teamId>/<branchId>/clock.json    分支本地 HLC（离线也能单调推进）
 *
 * 设计要点：
 *  - 客户端生成稳定 clientActionId：联网重试 / 进程重启补传单条动作只处理一次
 *  - 动作在入队时即盖本地 HLC 并固定 branchId：现场切换分支不改变动作去向，
 *    补传永远回到动作产生时的原分支（"补传固定回原分支"）
 *  - 冲突重提沿用同 id 时不允许改分支（branch-mismatch），防止 A 分支产生的
 *    动作被误提到 B 分支；需要换分支处理请走 requeueForBranch（显式改投，留痕）
 *  - 旧版布局 <root>/<simId>/<teamId>/{queue,clock}.json* 首次扫描时迁移到 main/
 *  - 仅追加 JSONL + 状态索引；崩溃后从流水重建内存表
 * ========================================================================= */

export const DEFAULT_BRANCH = 'main'

export function newClientActionId(kind = 'act') {
  return `${kind}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`
}

export class FieldOutbox {
  constructor(rootDir) {
    this.root = ensureDir(rootDir)
    this.queues = new Map() // key -> Map<actionId, action>
    this.clocks = new Map() // key -> { ts, l }
    this._migrateLegacy()
    this._recover()
  }

  _key(simId, teamId, branchId) { return `${simId}__${teamId}__${branchId}` }
  _dir(simId, teamId, branchId) { return ensureDir(path.join(this.root, simId, teamId, branchId)) }
  _queueFile(simId, teamId, branchId) { return path.join(this._dir(simId, teamId, branchId), 'queue.jsonl') }
  _clockFile(simId, teamId, branchId) { return path.join(this._dir(simId, teamId, branchId), 'clock.json') }

  // 旧版（无分支隔离）队列迁移：队目录下直接存在的 queue.jsonl / clock.json
  // 整体迁入 main/，动作补盖 branchId='main'。幂等：main/ 已有同名文件时跳过。
  _migrateLegacy() {
    for (const simId of this._subdirs(this.root)) {
      const simDir = path.join(this.root, simId)
      for (const teamId of this._subdirs(simDir)) {
        const teamDir = path.join(simDir, teamId)
        const legacyQueue = path.join(teamDir, 'queue.jsonl')
        const legacyClock = path.join(teamDir, 'clock.json')
        if (!fs.existsSync(legacyQueue)) continue
        const mainDir = ensureDir(path.join(teamDir, DEFAULT_BRANCH))
        const targetQueue = path.join(mainDir, 'queue.jsonl')
        if (fs.existsSync(targetQueue)) {
          fs.rmSync(legacyQueue, { force: true })
          fs.rmSync(legacyClock, { force: true })
          continue
        }
        // 流水行内动作补盖 branchId，保持原有 HLC / 状态 / 时间戳
        let text = ''
        try { text = fs.readFileSync(legacyQueue, 'utf8') } catch { text = '' }
        const rows = []
        for (const line of text.split('\n')) {
          const t = line.trim()
          if (!t) continue
          try {
            const row = JSON.parse(t)
            if (row.op === 'upsert' && row.action) row.action.branchId = DEFAULT_BRANCH
            rows.push(JSON.stringify(row))
          } catch { /* 崩溃尾部半行跳过 */ }
        }
        fs.writeFileSync(targetQueue, rows.length ? rows.join('\n') + '\n' : '')
        if (fs.existsSync(legacyClock)) fs.renameSync(legacyClock, path.join(mainDir, 'clock.json'))
        fs.rmSync(legacyQueue, { force: true })
      }
    }
  }

  _subdirs(dir) {
    try {
      return fs.readdirSync(dir, { withFileTypes: true }).filter((d) => d.isDirectory()).map((d) => d.name)
    } catch { return [] }
  }

  _recover() {
    for (const simId of this._subdirs(this.root)) {
      const simDir = path.join(this.root, simId)
      for (const teamId of this._subdirs(simDir)) {
        const teamDir = path.join(simDir, teamId)
        for (const branchId of this._subdirs(teamDir)) {
          const key = this._key(simId, teamId, branchId)
          const map = new Map()
          for (const line of this._readLines(this._queueFile(simId, teamId, branchId))) {
            if (line.op === 'upsert') {
              // 旧流水兜底：缺 branchId 的动作归入其所在分支
              line.action.branchId = line.action.branchId || branchId
              map.set(line.action.clientActionId, line.action)
            } else if (line.op === 'remove') map.delete(line.actionId)
          }
          this.queues.set(key, map)
          try { this.clocks.set(key, JSON.parse(fs.readFileSync(this._clockFile(simId, teamId, branchId), 'utf8'))) } catch { /* 无时钟文件从 0 开始 */ }
        }
      }
    }
  }

  *_readLines(file) {
    let text
    try { text = fs.readFileSync(file, 'utf8') } catch { return }
    for (const line of text.split('\n')) {
      const t = line.trim()
      if (!t) continue
      try { yield JSON.parse(t) } catch { /* 崩溃尾部半行跳过 */ }
    }
  }

  _append(simId, teamId, branchId, row) {
    fs.appendFileSync(this._queueFile(simId, teamId, branchId), JSON.stringify(row) + '\n')
  }

  _map(simId, teamId, branchId) {
    const key = this._key(simId, teamId, branchId)
    if (!this.queues.has(key)) this.queues.set(key, new Map())
    return this.queues.get(key)
  }

  _clock(simId, teamId, branchId) {
    const key = this._key(simId, teamId, branchId)
    if (!this.clocks.has(key)) this.clocks.set(key, { ts: 0, l: 0 })
    return this.clocks.get(key)
  }
  _saveClock(simId, teamId, branchId) {
    fs.writeFileSync(this._clockFile(simId, teamId, branchId), JSON.stringify(this.clocks.get(this._key(simId, teamId, branchId))))
  }

  // 推进分支本地 HLC：离线动作入队 / 收到服务器事件回执时调用
  tick(simId, teamId, branchId = DEFAULT_BRANCH, remoteHlc = null) {
    const c = this._clock(simId, teamId, branchId)
    const next = remoteHlc ? hlcReceive(c, hlcDecode(remoteHlc) || { ts: 0, l: 0 }) : hlcNow(c)
    this.clocks.set(this._key(simId, teamId, branchId), next)
    this._saveClock(simId, teamId, branchId)
    return hlcEncode(next)
  }

  // 入队一条离线动作（幂等：同 clientActionId 不重复入队，已终态的不可改）。
  // 动作在入队时固定分支：同 id 重提（冲突修正）必须回到原分支，否则拒绝。
  enqueue(simId, teamId, action, { remoteHlc = null, branchId = DEFAULT_BRANCH } = {}) {
    const id = action.clientActionId
    if (!id) throw Object.assign(new Error('缺少 clientActionId'), { status: 400, code: 'missing-action-id' })
    const wanted = action.branchId || branchId || DEFAULT_BRANCH
    const existed = this.find(simId, teamId, id)
    if (existed) {
      if (existed.branchId !== wanted) {
        throw Object.assign(new Error(`动作 ${id} 固定在分支 ${existed.branchId}，不能改提到 ${wanted}`), {
          status: 409, code: 'branch-mismatch', actual: existed.branchId, expected: wanted
        })
      }
      if (existed.status !== 'queued' && existed.status !== 'conflict') {
        return { action: existed, duplicated: true }
      }
    }
    const map = this._map(simId, teamId, wanted)
    // 冲突的动作允许现场修正后以同 id 重提（仍在原分支）
    const hlc = action.hlc || existed?.hlc || this.tick(simId, teamId, wanted, remoteHlc)
    const rec = {
      ...action,
      clientActionId: id,
      teamId: action.teamId || teamId,
      simId,
      branchId: wanted,
      hlc,
      status: existed?.status === 'conflict' ? 'queued' : (existed?.status || 'queued'),
      attempts: existed?.attempts || 0,
      queuedAt: existed?.queuedAt || new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      result: existed?.status === 'conflict' ? null : (existed?.result || null)
    }
    map.set(id, rec)
    this._append(simId, teamId, wanted, { op: 'upsert', action: rec })
    return { action: rec, duplicated: false }
  }

  list(simId, teamId, branchId, { status } = {}) {
    const map = this._map(simId, teamId, branchId)
    let out = [...map.values()]
    if (status) out = out.filter((a) => a.status === status)
    // 因果序：HLC 升序（同批 after 由网关拓扑保证）
    return out.sort((a, b) => ((a.hlc || '') + '|' + a.clientActionId < (b.hlc || '') + '|' + b.clientActionId ? -1 : 1))
  }

  pending(simId, teamId, branchId) { return this.list(simId, teamId, branchId, { status: 'queued' }) }

  get(simId, teamId, branchId, id) { return this._map(simId, teamId, branchId).get(id) || null }

  // 跨分支按 id 查找（回执落账 / 分支固定校验用）
  find(simId, teamId, id) {
    for (const map of this.queues.values()) {
      const hit = map.get(id)
      if (hit && hit.simId === simId && (hit.teamId === teamId || teamId == null)) return hit
    }
    return null
  }

  // 列出队伍的全部分支（切换页用）
  listBranches(simId, teamId) {
    const out = []
    for (const [key, map] of this.queues) {
      if (!key.startsWith(`${simId}__${teamId}__`)) continue
      const all = [...map.values()]
      out.push({
        branchId: key.split('__')[2],
        total: all.length,
        queued: all.filter((a) => a.status === 'queued').length,
        conflict: all.filter((a) => a.status === 'conflict').length,
        acked: all.filter((a) => a.status === 'acked').length
      })
    }
    // 目录存在但内存无键（空队列）的分支也要出现
    const teamDir = path.join(this.root, simId, teamId)
    for (const branchId of this._subdirs(teamDir)) {
      if (!out.some((b) => b.branchId === branchId)) out.push({ branchId, total: 0, queued: 0, conflict: 0, acked: 0 })
    }
    return out.sort((a, b) => (a.branchId === DEFAULT_BRANCH ? -1 : a.branchId.localeCompare(b.branchId)))
  }

  // 一支队伍在所有分支上的待发动作（补传固定回各自原分支，分组提交）
  pendingAcrossBranches(simId, teamId, onlyBranchId = null) {
    const branches = this.listBranches(simId, teamId)
      .filter((b) => !onlyBranchId || b.branchId === onlyBranchId)
    return branches.map((b) => ({ branchId: b.branchId, actions: this.pending(simId, teamId, b.branchId) }))
      .filter((g) => g.actions.length)
  }

  // 补传处理结果回写（按动作记录自带的 branchId 落账——在途同步期间切换分支不串账）
  mark(simId, teamId, id, patch) {
    const rec = this.find(simId, teamId, id)
    if (!rec) return null
    const { branchId } = rec
    const map = this._map(simId, teamId, branchId)
    Object.assign(rec, patch, { updatedAt: new Date().toISOString() })
    map.set(id, rec)
    this._append(simId, teamId, branchId, { op: 'upsert', action: rec })
    return rec
  }

  // 已完成动作清理（保留最近 N 条备查），冲突项不自动清理
  prune(simId, teamId, branchId, keep = 50) {
    const map = this._map(simId, teamId, branchId)
    const done = [...map.values()].filter((a) => a.status === 'acked').sort((a, b) => a.updatedAt.localeCompare(b.updatedAt))
    const remove = done.slice(0, Math.max(0, done.length - keep))
    remove.forEach((a) => {
      map.delete(a.clientActionId)
      this._append(simId, teamId, branchId, { op: 'remove', actionId: a.clientActionId })
    })
    return remove.length
  }

  // 显式改投：把一条冲突/待发动作以新 clientActionId 复制到另一分支（原动作丢弃并留痕）。
  // 这是"现场确认动作应在新分支执行"的唯一通道；返回新动作记录。
  requeueForBranch(simId, teamId, id, newBranchId, patch = {}) {
    const rec = this.find(simId, teamId, id)
    if (!rec) return null
    const { clientActionId, hlc, queuedAt, status, attempts, result, updatedAt, ...rest } = rec
    const moved = {
      ...rest,
      ...patch,
      clientActionId: newClientActionId(rec.kind),
      branchId: newBranchId,
      movedFrom: { clientActionId, branchId: rec.branchId, at: new Date().toISOString() }
    }
    this.remove(simId, teamId, id)
    const r = this.enqueue(simId, teamId, moved, { branchId: newBranchId })
    return r.action
  }

  remove(simId, teamId, id) {
    const rec = this.find(simId, teamId, id)
    if (!rec) return false
    this._map(simId, teamId, rec.branchId).delete(id)
    this._append(simId, teamId, rec.branchId, { op: 'remove', actionId: id })
    return true
  }

  listTeams(simId) {
    const out = []
    const simDir = path.join(this.root, simId)
    for (const teamId of this._subdirs(simDir)) {
      const branches = this.listBranches(simId, teamId)
      const sum = (k) => branches.reduce((n, b) => n + b[k], 0)
      out.push({
        teamId,
        total: sum('total'), queued: sum('queued'), conflict: sum('conflict'), acked: sum('acked'),
        branches
      })
    }
    return out
  }
}
