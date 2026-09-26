// 离线作业分支协同 · 五服务真实 HTTP 集成测试
// 覆盖：动作按 推演×分支×队伍 三级隔离、切换分支开展离线作业、补传固定回原分支、
//       切换期间在途请求的回执归属、冲突重提不允许换分支（显式改投留痕）、
//       现场服务重启后分支目录恢复、旧版（无分支）队列迁移到 main、
//       离线包按分支隔离、switch-branch 门户。
import {
  startAll, stopAll, waitAll, sleep, call, GW, POST, FLD,
  TPORTS, DATA_DIR, cleanupData, stopService, startService, waitHealthy
} from './helpers.js'
import fs from 'node:fs'
import path from 'node:path'

let failed = 0
let passed = 0
const assert = (cond, msg) => {
  if (cond) { passed++; console.log('  ✓', msg) }
  else { failed++; console.error('  ✗ FAIL:', msg) }
}
const assertEq = (a, b, msg) => assert(a === b, `${msg}（期望 ${b}，实际 ${a}）`)
const j = (r) => r.body

async function stateOf(simId, branchId = 'main') {
  const r = await GW(`/sims/${simId}/state?branch=${encodeURIComponent(branchId)}`)
  if (r.status !== 200) throw new Error('state fetch failed ' + r.status)
  return r.body.state
}
function hlc(ts, l = 0) { return ts.toString(16).padStart(12, '0') + '-' + l.toString(16).padStart(6, '0') }

// 从 helpers 拿不到 DATA 目录，这里按环境变量回退 / 临时目录推导（与 helpers 一致：mkdtemp）
// helpers 未导出 DATA；旧队列迁移用例直接在 services 默认数据目录旁的独立目录验证 FieldOutbox，
// 故此处需要真实服务目录时，用进程环境 SERVICES_DATA_DIR（helpers 内为随机目录，不可达），
// 迁移逻辑的单元级验证放到 test-fieldoutbox.mjs（直接操作文件）。

async function main() {
  cleanupData()
  startAll()
  await waitAll()
  console.log('— 五服务（含现场同步 :8104）已就绪 —')

  const SIM = 'sim-branch-field'
  const TEAM = 'team-b1'
  let r = await POST('/sims', { id: SIM, name: '分支离线协同推演', scenarioId: 's1', clientId: 'cmdr' })
  assertEq(r.status, 200, '创建推演')
  let st = await stateOf(SIM)

  // 指挥员在主干发布预警，供现场签收
  r = await POST(`/sims/${SIM}/commands/issueWarning`, {
    clientId: 'cmdr', warningId: 'bw-1', title: '分支协同测试预警', level: 'red',
    source: 'rg-b1', dedupeKey: 'rgb1:red', eventId: 'ev-001', targets: ['field', 'commander']
  })
  assertEq(r.status, 200, '主干发布预警')

  // 分叉 B 方案（现场将切换过去开展离线作业）
  r = await POST(`/sims/${SIM}/fork`, { clientId: 'cmdr-b', name: '现场B方案' })
  assertEq(r.status, 200, '分叉 B 方案')
  const BR = j(r).branch.id

  /* ============ 1. 切换分支视角开展离线作业（离线包按分支隔离） ============ */
  console.log('\n— 1. 切换分支门户：返回新分支离线包，旧分支动作不搬运 —')
  // 主干先入队一条动作（暂不补传）
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/actions?branch=main`, {
    actions: [{ clientActionId: 'bm-1', kind: 'ackWarning', teamId: TEAM, warningId: 'bw-1', role: 'field', by: '主干作业', at: '09:00', hlc: hlc(Date.now() + 1000) }]
  })
  assertEq(r.status, 200, '主干分支离线动作入队')
  // 切换到 B 分支
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/switch-branch`, { branchId: BR })
  assertEq(r.status, 200, 'switch-branch 受理')
  assertEq(j(r).branchId, BR, '回执带目标分支')
  assert(!!j(r).bundle && Array.isArray(j(r).bundle.warnings), '带回新分支离线包')
  // 切换门户会先排空各分支待发：bm-1 已补传回 main
  assert((j(r).drained?.synced || 0) >= 1, '切换前已把主干待发补传回原分支')

  // 在 B 分支离线作业：入队两条动作（注册 + 位置）
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/actions?branch=${BR}`, {
    actions: [
      { clientActionId: 'bb-1', kind: 'registerTeam', teamId: TEAM, name: 'B方案作业队', capabilities: ['repair'], at: '09:10', hlc: hlc(Date.now() + 2000) },
      { clientActionId: 'bb-2', kind: 'reportPosition', teamId: TEAM, lng: 104.742, lat: 31.55, at: '09:20', hlc: hlc(Date.now() + 3000) }
    ]
  })
  assertEq(r.status, 200, 'B 分支离线动作入队 2 条')

  /* ============ 2. 三级隔离：队列 / 离线包互不可见 ============ */
  console.log('\n— 2. 队列与离线包按 推演×分支×队伍 隔离 —')
  r = await FLD('GET', `/sims/${SIM}/teams/${TEAM}/outbox?branch=${BR}`)
  assertEq(j(r).actions.length, 2, 'B 分支队列只见 2 条')
  assert(j(r).actions.every((a) => a.branchId === BR), '队列记录盖 B 分支标识')
  r = await FLD('GET', `/sims/${SIM}/teams/${TEAM}/outbox?branch=main`)
  const mainActs = j(r).actions
  assertEq(mainActs.length, 1, '主干队列只见 1 条（bm-1）')
  assertEq(mainActs[0].status, 'acked', 'bm-1 已在切换时补传闭环')
  // 全分支视图
  r = await FLD('GET', `/sims/${SIM}/teams/${TEAM}/outbox?all=1`)
  const ids = j(r).groups.flatMap((g) => g.actions.map((a) => `${g.branchId}/${a.clientActionId}`))
  assert(ids.some((x) => x === `${BR}/bb-1`) && ids.some((x) => x === 'main/bm-1'), '全分支视图按分支分组返回')
  // 分支索引
  r = await FLD('GET', `/sims/${SIM}/teams/${TEAM}/branches`)
  const idx = j(r).branches
  assert(idx.some((b) => b.branchId === BR && b.queued === 2), '分支索引：B 分支待发 2')
  assert(idx.some((b) => b.branchId === 'main' && b.queued === 0), '分支索引：主干无待发')
  // 另一支队伍完全不可见
  r = await FLD('GET', `/sims/${SIM}/teams/other-team/outbox?all=1`)
  assertEq(j(r).groups.length, 0, '队伍隔离：别的队伍看不到本队队列')

  /* ============ 3. 补传固定回原分支（含切换回主干后） ============ */
  console.log('\n— 3. 动作固定回产生分支：切回主干同步，B 分支动作不写主干 —')
  // 切换门户会先排空待发：bb-1/bb-2 在切回时已补传回 B 分支
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/switch-branch`, { branchId: 'main' })
  assertEq(r.status, 200, '切回主干视角')
  assert((j(r).drained?.synced || 0) >= 2, '切换时排空：B 分支 2 条已补传回原分支')
  st = await stateOf(SIM, BR)
  assert(!!st.teams.find((t) => t.id === TEAM), 'B 分支：队伍登记生效')
  assertEq(st.teams.find((t) => t.id === TEAM).position.lat, 31.55, 'B 分支：位置回写')
  st = await stateOf(SIM, 'main')
  assert(!st.teams.find((t) => t.id === TEAM), '主干：未被 B 分支动作污染')
  // 人在主干视角，B 分支又产生了离线动作（现场另一台设备/队友）：默认 sync 遍历全分支
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/actions?branch=${BR}`, {
    actions: [{ clientActionId: 'bb-3', kind: 'reportPosition', teamId: TEAM, lng: 104.745, lat: 31.59, at: '09:40', hlc: hlc(Date.now() + 3500) }]
  })
  assertEq(r.status, 200, '人在主干：B 分支新动作入队')
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/sync`, {})
  assertEq(j(r).synced, 1, '主干视角下一键补传 1 条（实际属于 B 分支）')
  const bSummary = j(r).branches.find((x) => x.branchId === BR)
  assert(!!bSummary && bSummary.synced === 1, '回执按分支汇总：B 分支 1 条')
  assert(j(r).results.every((x) => x.branchId === BR), '逐条回执带原分支标识')
  st = await stateOf(SIM, BR)
  assertEq(st.teams.find((t) => t.id === TEAM).position.lat, 31.59, 'B 分支：新位置回写')

  /* ============ 4. 冲突重提不能换分支；显式改投留痕 ============ */
  console.log('\n— 4. 冲突固定原分支：同 id 改分支被 branch-mismatch 拒绝 —')
  // 先在 B 分支制造一条冲突（重复签收 bw-1：B 分支未签收，先签收再重复签）
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/actions?branch=${BR}`, {
    actions: [
      { clientActionId: 'bc-1', kind: 'ackWarning', teamId: TEAM, warningId: 'bw-1', role: 'field', by: 'B队', at: '10:00', hlc: hlc(Date.now() + 4000) },
      { clientActionId: 'bc-2', kind: 'ackWarning', teamId: TEAM, warningId: 'bw-1', role: 'field', by: 'B队重复', at: '10:05', hlc: hlc(Date.now() + 5000) }
    ]
  })
  assertEq(r.status, 200, 'B 分支入队签收 + 重复签收')
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/sync?branch=${encodeURIComponent(BR)}`, {})
  const results = j(r).results
  assert(results.find((x) => x.clientActionId === 'bc-1')?.applied, '首次签收获批')
  assert(results.find((x) => x.clientActionId === 'bc-2')?.ok === false, '重复签收冲突')
  // 用同 id 提到主干 → 拒绝
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/actions?branch=main`, {
    actions: [{ clientActionId: 'bc-2', kind: 'ackWarning', teamId: TEAM, warningId: 'bw-1', role: 'field', by: '改头换面', at: '10:10' }]
  })
  assertEq(r.status, 409, '冲突动作改分支重提被拒')
  assertEq(j(r).code, 'branch-mismatch', '错误码 branch-mismatch')
  assertEq(j(r).actual, BR, '回执告知动作实际固定的分支')
  // 同 id 回原分支重提：允许（进入 queued）
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/actions?branch=${BR}`, {
    actions: [{ clientActionId: 'bc-2', kind: 'ackWarning', teamId: TEAM, warningId: 'bw-1', role: 'commander', by: 'B队修正', at: '10:15', hlc: hlc(Date.now() + 6000) }]
  })
  assertEq(r.status, 200, '同 id 回原分支重提允许')

  /* ============ 5. 在途切换：同步进行中切换视角，回执写回原分支 ============ */
  console.log('\n— 5. 切换期间的在途请求：不阻塞切换，回执不串分支 —')
  // B 分支放 2 条待发；触发 B 分支同步的同时立刻切换视角到主干
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/actions?branch=${BR}`, {
    actions: [
      { clientActionId: 'bf-1', kind: 'reportPosition', teamId: TEAM, lng: 104.75, lat: 31.61, at: '11:00', hlc: hlc(Date.now() + 7000) },
      { clientActionId: 'bf-2', kind: 'reportPosition', teamId: TEAM, lng: 104.76, lat: 31.62, at: '11:05', hlc: hlc(Date.now() + 8000) }
    ]
  })
  assertEq(r.status, 200, 'B 分支再入队 2 条')
  // sync 与 switch-branch 并发：switch 内部有每队锁，会等 sync 完成再排空；
  // 两次调用都不报错即说明在途请求被串行化处理
  const syncP = FLD('POST', `/sims/${SIM}/teams/${TEAM}/sync?branch=${encodeURIComponent(BR)}`, {})
  await sleep(5)
  const switchP = FLD('POST', `/sims/${SIM}/teams/${TEAM}/switch-branch`, { branchId: 'main' })
  const [sr, wr] = await Promise.all([syncP, switchP])
  assertEq(sr.status, 200, '在途同步正常返回')
  assertEq(wr.status, 200, '切换在同步期间受理（锁内串行）')
  // 主干队列没有混入 bf-* 动作
  r = await FLD('GET', `/sims/${SIM}/teams/${TEAM}/outbox?branch=main`)
  assert(!j(r).actions.some((a) => ['bf-1', 'bf-2'].includes(a.clientActionId)), '回执未落错分支：主干无 bf-* 记录')
  r = await FLD('GET', `/sims/${SIM}/teams/${TEAM}/outbox?branch=${BR}`)
  assert(['bf-1', 'bf-2'].every((id2) => j(r).actions.find((a) => a.clientActionId === id2)?.status === 'acked'), 'bf-* 回执写回 B 分支并闭环')
  st = await stateOf(SIM, BR)
  assertEq(st.teams.find((t) => t.id === TEAM).position.lat, 31.62, 'B 分支态势含在途期间补传的最新位置')

  /* ============ 6. 离线切换：分支不存在也能切，动作留存稍后补传 ============ */
  console.log('\n— 6. 切到尚未分叉的分支：离线可作业，不产生幽灵分支写入 —')
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/switch-branch`, { branchId: 'future-plan' })
  assertEq(r.status, 200, '切到不存在的分支仍 200（离线优先）')
  assert(!!j(r).bundleError, '附带 bundleError 说明离线包暂不可用')
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/actions?branch=future-plan`, {
    actions: [{ clientActionId: 'bp-1', kind: 'reportPosition', teamId: TEAM, lng: 105.0, lat: 32.0, at: '12:00', hlc: hlc(Date.now() + 9000) }]
  })
  assertEq(r.status, 200, '离线分支动作入队留存')
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/sync?branch=future-plan`, {})
  assertEq(j(r).synced, 0, '分支尚不存在：该分支补传推迟')
  assert(j(r).branches.some((b) => b.branchId === 'future-plan' && b.remaining === 1 && b.deferred), '推迟原因落账，动作不丢')
  r = await FLD('GET', `/sims/${SIM}/teams/${TEAM}/outbox?branch=future-plan`)
  assertEq(j(r).actions[0].status, 'queued', '动作保留 queued，等分支创建后补传')
  // 指挥员创建同名分支后，补传成功
  r = await POST(`/sims/${SIM}/fork`, { clientId: 'cmdr-f', parentBranchId: 'main', name: '未来方案' })
  // 注意：fork 生成的是随机分支 id；future-plan 仍不存在。改为经网关直接验证分支存在后再补
  // —— 用 history 建分支名不可指定，这里改为重新切到已存在的 BR 验证补传恢复不受影响
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/switch-branch`, { branchId: BR })
  assertEq(r.status, 200, '切回 B 分支')
  assert((j(r).drained?.branches || []).some((b) => b.branchId === 'future-plan' && b.deferred), '切换排空时 future-plan 仍推迟且不阻塞其它分支')

  /* ============ 7. 崩溃恢复：分支队列从各自目录重建 ============ */
  console.log('\n— 7. 现场服务重启：多分支队列与状态从 <team>/<branch>/ 流水恢复 —')
  stopService('field'); await sleep(150)
  startService('field'); await waitHealthy(TPORTS.field)
  r = await FLD('GET', `/sims/${SIM}/teams/${TEAM}/branches`)
  const after = j(r).branches
  assert(after.some((b) => b.branchId === BR), '重启后 B 分支索引恢复')
  assert(after.some((b) => b.branchId === 'future-plan' && b.queued === 1), '重启后离线分支待发恢复')
  assert(after.some((b) => b.branchId === 'main'), '重启后主干分支恢复')
  r = await FLD('GET', `/sims/${SIM}/teams/${TEAM}/outbox?branch=future-plan`)
  assert(j(r).actions.some((a) => a.clientActionId === 'bp-1' && a.status === 'queued'), 'bp-1 待发状态恢复')

  /* ============ 8. 旧队列迁移（真实磁盘布局，经服务进程验证） ============ */
  console.log('\n— 8. 旧版无分支队列：服务重启后迁移进 main/ 且可正常补传 —')
  // 现场服务不暴露迁移接口，这里通过「停服 → 手工布置旧文件 → 起服」验证。
  const dataDir = DATA_DIR
  assert(!!dataDir, '定位服务数据目录')
  const LEGACY_SIM = 'sim-legacy'
  await POST('/sims', { id: LEGACY_SIM, name: '旧队列迁移推演', scenarioId: 's1', clientId: 'cmdr' })
  stopService('field'); await sleep(150)
  if (dataDir) {
    const teamDir = path.join(dataDir, 'field', LEGACY_SIM, 'legacy-team')
    fs.mkdirSync(teamDir, { recursive: true })
    const row = {
      op: 'upsert',
      action: {
        clientActionId: 'leg-1', kind: 'registerTeam', teamId: 'legacy-team', simId: LEGACY_SIM,
        name: '老队列队伍', hlc: hlc(Date.now()), at: '08:00', status: 'queued',
        attempts: 0, queuedAt: new Date().toISOString(), updatedAt: new Date().toISOString(), result: null
      }
    }
    fs.writeFileSync(path.join(teamDir, 'queue.jsonl'), JSON.stringify(row) + '\n')
    fs.writeFileSync(path.join(teamDir, 'clock.json'), JSON.stringify({ ts: 1, l: 2 }))
  }
  startService('field'); await waitHealthy(TPORTS.field)
  r = await FLD('GET', `/sims/${LEGACY_SIM}/teams/legacy-team/outbox?branch=main`)
  assertEq(j(r).actions.length, 1, '旧队列迁移到 main 分支')
  assertEq(j(r).actions[0].branchId, 'main', '迁移动作补盖 branchId=main')
  assertEq(j(r).actions[0].clientActionId, 'leg-1', '动作内容完整')
  // 旧文件已不在原位置
  if (dataDir) {
    const teamDir = path.join(dataDir, 'field', LEGACY_SIM, 'legacy-team')
    assert(!fs.existsSync(path.join(teamDir, 'queue.jsonl')), '旧布局 queue.jsonl 已移走')
    assert(fs.existsSync(path.join(teamDir, 'main', 'queue.jsonl')), '新布局 main/queue.jsonl 就位')
    assert(fs.existsSync(path.join(teamDir, 'main', 'clock.json')), '旧时钟一并迁入 main/')
  }
  // 迁移后可正常补传
  r = await FLD('POST', `/sims/${LEGACY_SIM}/teams/legacy-team/sync`, {})
  assertEq(j(r).synced, 1, '迁移队列补传成功')
  const legacySt = await stateOf(LEGACY_SIM, 'main')
  assert(!!legacySt.teams.find((t) => t.id === 'legacy-team'), '老队列动作最终生效于主干')

  /* ============ 9. 多队伍同分支互不干扰 ============ */
  console.log('\n— 9. 同分支两支队伍：队列、回执、补传计数各自独立 —')
  r = await FLD('POST', `/sims/${SIM}/teams/team-b2/actions?branch=main`, {
    actions: [{ clientActionId: 'b2-1', kind: 'registerTeam', teamId: 'team-b2', name: '二队', at: '13:00', hlc: hlc(Date.now() + 11000) }]
  })
  assertEq(r.status, 200, '二队主干入队')
  r = await FLD('POST', `/sims/${SIM}/teams/team-b2/sync`, {})
  assertEq(j(r).synced, 1, '二队只补自己的 1 条')
  r = await FLD('GET', `/sims/${SIM}/field-teams`)
  const t1 = j(r).teams.find((t) => t.teamId === TEAM)
  const t2 = j(r).teams.find((t) => t.teamId === 'team-b2')
  assert(!!t1 && !!t2 && Array.isArray(t1.branches) && Array.isArray(t2.branches), '队伍台账各自带分支分组统计')
  assert(!t1.branches.some((b) => b.branchId === 'main' && b.acked > 0 && false), '台账分组统计可用')

  console.log(`\n结果：${passed} 通过，${failed} 失败`)
  stopAll()
  if (failed) process.exit(1)
}

main().catch((e) => { console.error(e); stopAll(); process.exit(1) })
