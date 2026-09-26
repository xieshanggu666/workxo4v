// FieldOutbox 单元测试：分支隔离 / 补传固定回原分支 / 冲突重提 branch-mismatch /
// 显式改投留痕 / 旧版无分支队列迁移到 main / 崩溃流水恢复
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { FieldOutbox, DEFAULT_BRANCH } from '../lib/fieldOutbox.js'

let failed = 0
let passed = 0
const assert = (cond, msg) => {
  if (cond) { passed++; console.log('  ✓', msg) }
  else { failed++; console.error('  ✗ FAIL:', msg) }
}
const assertEq = (a, b, msg) => assert(a === b, `${msg}（期望 ${b}，实际 ${a}）`)

function tmpRoot() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'dc-outbox-'))
}

const SIM = 'sim-1'
const TEAM = 'team-1'
const BR = 'br-a'

function hlc(ts, l = 0) { return ts.toString(16).padStart(12, '0') + '-' + l.toString(16).padStart(6, '0') }

console.log('— 1. 入队即固定分支；查询按分支隔离 —')
{
  const ob = new FieldOutbox(tmpRoot())
  ob.enqueue(SIM, TEAM, { clientActionId: 'a1', kind: 'reportPosition', lat: 1, hlc: hlc(1000) }, { branchId: 'main' })
  ob.enqueue(SIM, TEAM, { clientActionId: 'a2', kind: 'reportPosition', lat: 2, hlc: hlc(2000) }, { branchId: BR })
  assertEq(ob.pending(SIM, TEAM, 'main').length, 1, '主干 1 条')
  assertEq(ob.pending(SIM, TEAM, BR).length, 1, 'B 分支 1 条')
  assertEq(ob.pending(SIM, TEAM, 'main')[0].branchId, 'main', '记录盖 main')
  assertEq(ob.pending(SIM, TEAM, BR)[0].branchId, BR, '记录盖 br-a')
  // HLC 按分支独立推进（离线时钟互不干扰）
  ob.tick(SIM, TEAM, 'main')
  ob.tick(SIM, TEAM, BR)
  const branches = ob.listBranches(SIM, TEAM).map((b) => b.branchId)
  assert(branches.includes('main') && branches.includes(BR), '分支索引含两个分支')
  // 队伍隔离
  assertEq(ob.pending(SIM, 'other', 'main').length, 0, '别的队伍为空')
}

console.log('— 2. 补传固定回原分支：mark/find 跨分支落账 —')
{
  const ob = new FieldOutbox(tmpRoot())
  ob.enqueue(SIM, TEAM, { clientActionId: 'a1', kind: 'reportPosition', hlc: hlc(1000) }, { branchId: BR })
  // 不传分支也能按 id 找到并落回原分支（模拟在途同步期间切换了视角）
  assertEq(ob.find(SIM, TEAM, 'a1').branchId, BR, '跨分支按 id 找到')
  ob.mark(SIM, TEAM, 'a1', { status: 'acked', result: { ok: true } })
  assertEq(ob.pending(SIM, TEAM, BR).length, 0, '回执写回原分支，B 无待发')
  assertEq(ob.list(SIM, TEAM, BR)[0].status, 'acked', 'B 分支状态 acked')
  assertEq(ob.list(SIM, TEAM, 'main').length, 0, '主干仍为空（未串账）')
}

console.log('— 3. 冲突重提不允许换分支：branch-mismatch —')
{
  const ob = new FieldOutbox(tmpRoot())
  ob.enqueue(SIM, TEAM, { clientActionId: 'c1', kind: 'ackWarning', hlc: hlc(1000) }, { branchId: BR })
  ob.mark(SIM, TEAM, 'c1', { status: 'conflict', result: { ok: false, msg: '重复签收' } })
  let threw = null
  try {
    ob.enqueue(SIM, TEAM, { clientActionId: 'c1', kind: 'ackWarning' }, { branchId: 'main' })
  } catch (e) { threw = e }
  assert(threw && threw.status === 409 && threw.code === 'branch-mismatch', '改分支重提抛 branch-mismatch')
  assertEq(threw?.actual, BR, '错误带实际固定分支')
  // 回原分支重提允许
  const r = ob.enqueue(SIM, TEAM, { clientActionId: 'c1', kind: 'ackWarning', role: 'commander' }, { branchId: BR })
  assert(!r.duplicated && r.action.status === 'queued', '回原分支重提转 queued')
  assertEq(r.action.result, null, '重提清空冲突回执')
  // 已终态（acked）的动作任何分支都不可改
  ob.mark(SIM, TEAM, 'c1', { status: 'acked' })
  const dup = ob.enqueue(SIM, TEAM, { clientActionId: 'c1', kind: 'ackWarning' }, { branchId: BR })
  assert(dup.duplicated, '已闭环动作同 id 幂等返回')
}

console.log('— 4. 显式改投：新 id、留痕 movedFrom、原动作移除 —')
{
  const ob = new FieldOutbox(tmpRoot())
  ob.enqueue(SIM, TEAM, { clientActionId: 'm1', kind: 'reportPosition', lat: 9, hlc: hlc(1000) }, { branchId: 'main' })
  ob.mark(SIM, TEAM, 'm1', { status: 'conflict' })
  const moved = ob.requeueForBranch(SIM, TEAM, 'm1', BR, { lat: 10 })
  assertEq(moved.branchId, BR, '新动作落在 B 分支')
  assert(moved.clientActionId !== 'm1', '生成新 clientActionId')
  assertEq(moved.movedFrom.branchId, 'main', '留痕来源分支')
  assertEq(moved.lat, 10, 'patch 生效')
  assertEq(ob.list(SIM, TEAM, 'main').length, 0, '原分支动作已移除')
  assertEq(ob.pending(SIM, TEAM, BR).length, 1, '新分支待发 1 条')
}

console.log('— 5. pendingAcrossBranches：补传按原分支分组 —')
{
  const ob = new FieldOutbox(tmpRoot())
  ob.enqueue(SIM, TEAM, { clientActionId: 'g1', kind: 'reportPosition', hlc: hlc(1000) }, { branchId: 'main' })
  ob.enqueue(SIM, TEAM, { clientActionId: 'g2', kind: 'reportPosition', hlc: hlc(2000) }, { branchId: BR })
  ob.enqueue(SIM, TEAM, { clientActionId: 'g3', kind: 'reportPosition', hlc: hlc(3000) }, { branchId: BR })
  ob.mark(SIM, TEAM, 'g3', { status: 'conflict' })
  const all = ob.pendingAcrossBranches(SIM, TEAM)
  assertEq(all.length, 2, '两个分支有待发组')
  const only = ob.pendingAcrossBranches(SIM, TEAM, BR)
  assertEq(only.length, 1, '只看 B 分支时 1 组')
  assertEq(only[0].actions.length, 1, '冲突项不算 pending（g3 不在）')
}

console.log('— 6. 旧版队列迁移：<team>/queue.jsonl → <team>/main/queue.jsonl —')
{
  const root = tmpRoot()
  const teamDir = path.join(root, SIM, TEAM)
  fs.mkdirSync(teamDir, { recursive: true })
  const row = {
    op: 'upsert',
    action: {
      clientActionId: 'old-1', kind: 'registerTeam', simId: SIM, teamId: TEAM,
      hlc: hlc(500), at: '08:00', status: 'queued', attempts: 0,
      queuedAt: '2026-01-01T00:00:00.000Z', updatedAt: '2026-01-01T00:00:00.000Z', result: null
    }
  }
  fs.writeFileSync(path.join(teamDir, 'queue.jsonl'), JSON.stringify(row) + '\n')
  fs.writeFileSync(path.join(teamDir, 'clock.json'), JSON.stringify({ ts: 123, l: 7 }))
  const ob = new FieldOutbox(root)
  assertEq(ob.pending(SIM, TEAM, DEFAULT_BRANCH).length, 1, '旧动作迁入 main')
  assertEq(ob.pending(SIM, TEAM, DEFAULT_BRANCH)[0].branchId, 'main', '旧动作补盖 branchId')
  assert(!fs.existsSync(path.join(teamDir, 'queue.jsonl')), '旧 queue.jsonl 已移走')
  assert(fs.existsSync(path.join(teamDir, 'main', 'queue.jsonl')), '新位置存在')
  assert(fs.existsSync(path.join(teamDir, 'main', 'clock.json')), '时钟迁入 main')
  // 幂等：再来一次（新 main 已存在）不报错、不重复
  const ob2 = new FieldOutbox(root)
  assertEq(ob2.pending(SIM, TEAM, 'main').length, 1, '二次启动无重复迁移')
}

console.log('— 7. 崩溃恢复：分支流水重建内存索引（含 remove） —')
{
  const root = tmpRoot()
  let ob = new FieldOutbox(root)
  ob.enqueue(SIM, TEAM, { clientActionId: 'r1', kind: 'reportPosition', hlc: hlc(1000) }, { branchId: BR })
  ob.enqueue(SIM, TEAM, { clientActionId: 'r2', kind: 'reportPosition', hlc: hlc(2000) }, { branchId: BR })
  ob.mark(SIM, TEAM, 'r1', { status: 'acked' })
  ob.remove(SIM, TEAM, 'r1')
  ob = new FieldOutbox(root)
  assertEq(ob.list(SIM, TEAM, BR).length, 1, '重启后只剩 r2（upsert/remove 流水重建）')
  assertEq(ob.list(SIM, TEAM, BR)[0].clientActionId, 'r2', '保留正确动作')
}

console.log('— 8. listTeams 汇总带分支分组 —')
{
  const ob = new FieldOutbox(tmpRoot())
  ob.enqueue(SIM, 't1', { clientActionId: 't1a', kind: 'reportPosition', hlc: hlc(1) }, { branchId: 'main' })
  ob.enqueue(SIM, 't1', { clientActionId: 't1b', kind: 'reportPosition', hlc: hlc(2) }, { branchId: BR })
  ob.enqueue(SIM, 't2', { clientActionId: 't2a', kind: 'reportPosition', hlc: hlc(3) }, { branchId: 'main' })
  const teams = ob.listTeams(SIM)
  const t1 = teams.find((t) => t.teamId === 't1')
  assertEq(t1.total, 2, 't1 总计 2')
  assertEq(t1.branches.length, 2, 't1 有两个分支分组')
  assertEq(teams.find((t) => t.teamId === 't2').total, 1, 't2 总计 1')
}

console.log(`\n结果：${passed} 通过，${failed} 失败`)
if (failed) process.exit(1)
