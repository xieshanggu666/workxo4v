import fs from 'node:fs'
import path from 'node:path'
import { ensureDir } from './storage.js'
import { hlcNow, hlcEncode, hlcDecode, hlcReceive } from './causal.js'

/* =========================================================================
 * FieldOutbox —— 现场端离线动作队列（也可作为现场同步服务的服务端持久层）
 *
 * 文件布局（按 推演 / 分支 / 队伍 三维隔离）：
 *   <root>/<simId>/<branchId>/<teamId>/queue.jsonl   动作流水（queued → acked / conflict）
 *   <root>/<simId>/<teamId>.clock.json               本地 HLC（一机一钟，切分支不回退）
 *
 * 设计要点：
 *  - 动作入队即钉住当时所在分支 branchId：补传固定回原分支，切换分支不裹挟在途动作
 *  - 客户端生成稳定 clientActionId：联网重试 / 进程重启补传单条动作只处理一次
 *  - 动作在入队时即盖本地 HLC（设备时钟 + 逻辑计数），补传时由采集端据此与
 *    其它节点合并因果序；after[] 让"签收 after 派发到达"即便父事件晚到也拓扑归位
 *  - 仅追加 JSONL + 状态索引；崩溃后从流水重建内存表
 *  - 旧版布局 <root>/<simId>/<teamId>/（无分支层）启动时自动迁移：无 branchId 的
 *    历史动作归入 main 分支，时钟文件平移为 <teamId>.clock.json，迁移幂等可重入
 * ========================================================================= */

const MAIN = 'main'

export class FieldOutbox {
  constructor(rootDir) {
    this.root = ensureDir(rootDir)
    this.queues = new Map() // `${simId}__${branchId}__${teamId}` -> Map<actionId, action>
    this.clocks = new Map() // `${simId}__${teamId}` -> { ts, l }
    this._migrateLegacy()
    this._recover()
  }

  _qKey(simId, branchId, teamId) { return `${simId}__${branchId}__${teamId}` }
  _cKey(simId, teamId) { return `${simId}__${teamId}` }
  _dir(simId, branchId, teamId) { return ensureDir(path.join(this.root, simId, branchId, teamId)) }
  _queueFile(simId, branchId, teamId) { return path.join(this._dir(simId, branchId, teamId), 'queue.jsonl') }
  _clockFile(simId, teamId) { return path.join(ensureDir(path.join(this.root, simId)), `${teamId}.clock.json`) }

  _dirs(dir) {
    try {
      return fs.readdirSync(dir, { withFileTypes: true }).filter((d) => d.isDirectory()).map((d) => d.name)
    } catch { return [] }
  }

  /* ---------------- 旧队列迁移（无分支层 → 按分支隔离） ---------------- */

  _migrateLegacy() {
    for (const simId of this._dirs(this.root)) {
      const simDir = path.join(this.root, simId)
      for (const name of this._dirs(simDir)) {
        const legacyDir = path.join(simDir, name)
        const legacyQueue = path.join(legacyDir, 'queue.jsonl')
        if (!fs.existsSync(legacyQueue)) continue // 已是分支层目录
        const teamId = name
        const rows = [...this._readLines(legacyQueue)]
        // 先收集各动作出现过的分支（upsert 自带 branchId，缺省 main），
        // remove 行需广播到对应动作所在的每个分支，避免迁移后残留
        const branchOf = new Map()
        for (const row of rows) {
          if (row?.op === 'upsert' && row.action?.clientActionId) {
            const id = row.action.clientActionId
            if (!branchOf.has(id)) branchOf.set(id, new Set())
            branchOf.get(id).add(row.action.branchId || MAIN)
          }
        }
        const byBranch = new Map()
        const push = (branchId, row) => {
          if (!byBranch.has(branchId)) byBranch.set(branchId, [])
          byBranch.get(branchId).push(row)
        }
        for (const row of rows) {
          if (row?.op === 'remove') {
            const targets = branchOf.get(row.actionId) || new Set([MAIN])
            for (const br of targets) push(br, row)
          } else if (row?.op === 'upsert' && row.action) {
            push(row.action.branchId || MAIN, { ...row, action: { ...row.action, branchId: row.action.branchId || MAIN } })
          }
        }
        for (const [branchId, list] of byBranch) {
          const file = this._queueFile(simId, branchId, teamId)
          for (const row of list) fs.appendFileSync(file, JSON.stringify(row) + '\n')
        }
        // 时钟平移：一机一钟，不随分支复制
        try { fs.renameSync(path.join(legacyDir, 'clock.json'), this._clockFile(simId, teamId)) } catch { /* 无时钟文件 */ }
        fs.rmSync(legacyDir, { recursive: true, force: true })
      }
    }
  }

  _recover() {
    for (const simId of this._dirs(this.root)) {
      const simDir = path.join(this.root, simId)
      // 队伍时钟（<simId>/<teamId>.clock.json，与分支目录平级）
      for (const f of fs.readdirSync(simDir)) {
        const m = f.match(/^(.+)\.clock\.json$/)
        if (!m) continue
        try { this.clocks.set(this._cKey(simId, m[1]), JSON.parse(fs.readFileSync(path.join(simDir, f), 'utf8'))) } catch { /* 损坏时钟从 0 开始 */ }
      }
      for (const branchId of this._dirs(simDir)) {
        for (const teamId of this._dirs(path.join(simDir, branchId))) {
          const map = new Map()
          for (const line of this._readLines(this._queueFile(simId, branchId, teamId))) {
            if (line.op === 'upsert') map.set(line.action.clientActionId, { ...line.action, branchId })
            else if (line.op === 'remove') map.delete(line.actionId)
          }
          this.queues.set(this._qKey(simId, branchId, teamId), map)
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

  _append(simId, branchId, teamId, row) {
    fs.appendFileSync(this._queueFile(simId, branchId, teamId), JSON.stringify(row) + '\n')
  }

  _map(simId, branchId, teamId) {
    const key = this._qKey(simId, branchId, teamId)
    if (!this.queues.has(key)) this.queues.set(key, new Map())
    return this.queues.get(key)
  }

  // 枚举一支队伍的全部分支队列：[branchId, Map]
  _teamQueues(simId, teamId) {
    const out = []
    const prefix = `${simId}__`
    const suffix = `__${teamId}`
    for (const [key, map] of this.queues) {
      if (key.startsWith(prefix) && key.endsWith(suffix)) {
        out.push([key.slice(prefix.length, -suffix.length), map])
      }
    }
    return out
  }

  // 推进本地 HLC：离线动作入队 / 收到服务器事件回执时调用（一机一钟，跨分支单调）
  tick(simId, teamId, remoteHlc = null) {
    const key = this._cKey(simId, teamId)
    const c = this.clocks.get(key) || { ts: 0, l: 0 }
    const next = remoteHlc ? hlcReceive(c, hlcDecode(remoteHlc) || { ts: 0, l: 0 }) : hlcNow(c)
    this.clocks.set(key, next)
    fs.writeFileSync(this._clockFile(simId, teamId), JSON.stringify(next))
    return hlcEncode(next)
  }

  // 入队一条离线动作（幂等：同 clientActionId 不重复入队，已终态的不可改；
  // 冲突的动作允许现场修正后以同 id 重提。重提未显式指定分支时钉住原分支——补传固定回原分支）
  enqueue(simId, teamId, action, { remoteHlc = null } = {}) {
    const id = action.clientActionId
    if (!id) throw Object.assign(new Error('缺少 clientActionId'), { status: 400, code: 'missing-action-id' })
    // 全队范围查重：同 id 可能因切换分支后重提而落在其它分支队列
    let existed = null
    let existedBranch = null
    for (const [br, map] of this._teamQueues(simId, teamId)) {
      const rec = map.get(id)
      if (rec) { existed = rec; existedBranch = br; break }
    }
    if (existed && existed.status !== 'queued' && existed.status !== 'conflict') {
      return { action: existed, duplicated: true }
    }
    const branchId = action.branchId || existed?.branchId || MAIN
    // 显式改派到别的分支：移动而非复制（同 id 全队唯一）
    if (existed && existedBranch !== branchId) {
      this._map(simId, existedBranch, teamId).delete(id)
      this._append(simId, existedBranch, teamId, { op: 'remove', actionId: id })
    }
    const hlc = action.hlc || (existed?.hlc) || this.tick(simId, teamId, remoteHlc)
    const rec = {
      ...action,
      clientActionId: id,
      teamId: action.teamId || teamId,
      simId,
      branchId,
      hlc,
      status: existed?.status === 'conflict' ? 'queued' : (existed?.status || 'queued'),
      attempts: existed?.attempts || 0,
      queuedAt: existed?.queuedAt || new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      result: existed?.status === 'conflict' ? null : (existed?.result || null)
    }
    this._map(simId, branchId, teamId).set(id, rec)
    this._append(simId, branchId, teamId, { op: 'upsert', action: rec })
    return { action: rec, duplicated: false }
  }

  list(simId, teamId, { status, branchId } = {}) {
    let out = []
    for (const [br, map] of this._teamQueues(simId, teamId)) {
      if (branchId && br !== branchId) continue
      out.push(...map.values())
    }
    if (status) out = out.filter((a) => a.status === status)
    // 因果序：HLC 升序（同批 after 由网关拓扑保证）
    return out.sort((a, b) => ((a.hlc || '') + '|' + a.clientActionId < (b.hlc || '') + '|' + b.clientActionId ? -1 : 1))
  }

  pending(simId, teamId, { branchId } = {}) { return this.list(simId, teamId, { status: 'queued', branchId }) }

  // 待发动作按入队时钉住的分支分组：补传固定回原分支的核心
  pendingByBranch(simId, teamId, { branchId } = {}) {
    const groups = new Map()
    for (const a of this.pending(simId, teamId, { branchId })) {
      const br = a.branchId || MAIN
      if (!groups.has(br)) groups.set(br, [])
      groups.get(br).push(a)
    }
    return groups
  }

  get(simId, teamId, id) {
    for (const [, map] of this._teamQueues(simId, teamId)) {
      const rec = map.get(id)
      if (rec) return rec
    }
    return null
  }

  // 补传处理结果回写（按 id 定位到其所在分支队列）
  mark(simId, teamId, id, patch) {
    for (const [br, map] of this._teamQueues(simId, teamId)) {
      const rec = map.get(id)
      if (!rec) continue
      Object.assign(rec, patch, { updatedAt: new Date().toISOString() })
      map.set(id, rec)
      this._append(simId, br, teamId, { op: 'upsert', action: rec })
      return rec
    }
    return null
  }

  // 已完成动作清理（各分支队列分别保留最近 N 条备查），冲突项不自动清理
  prune(simId, teamId, keep = 50) {
    let removed = 0
    for (const [br, map] of this._teamQueues(simId, teamId)) {
      const done = [...map.values()].filter((a) => a.status === 'acked').sort((a, b) => a.updatedAt.localeCompare(b.updatedAt))
      const remove = done.slice(0, Math.max(0, done.length - keep))
      remove.forEach((a) => {
        map.delete(a.clientActionId)
        this._append(simId, br, teamId, { op: 'remove', actionId: a.clientActionId })
      })
      removed += remove.length
    }
    return removed
  }

  listTeams(simId) {
    const prefix = `${simId}__`
    const teamIds = new Set()
    for (const key of this.queues.keys()) {
      if (key.startsWith(prefix)) teamIds.add(key.slice(key.lastIndexOf('__') + 2))
    }
    const stat = (list) => ({
      total: list.length,
      queued: list.filter((a) => a.status === 'queued').length,
      conflict: list.filter((a) => a.status === 'conflict').length,
      acked: list.filter((a) => a.status === 'acked').length
    })
    const out = []
    for (const teamId of teamIds) {
      const all = this.list(simId, teamId)
      const branches = this._teamQueues(simId, teamId)
        .map(([br]) => br).sort()
        .map((br) => ({ branchId: br, ...stat(this.list(simId, teamId, { branchId: br })) }))
      out.push({ teamId, ...stat(all), branches })
    }
    return out
  }
}
