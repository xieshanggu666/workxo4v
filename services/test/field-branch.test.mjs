// 离线作业分支协同 · 五服务真实 HTTP 集成测试
// 覆盖：现场队伍切换推演分支开展离线作业；离线包 / 动作 / 回执按 推演+分支+队伍 隔离；
//       补传固定回原分支（切分支不裹挟在途与待发动作）；sync 限定单分支；
//       切换期间在途请求不重复不错乱；冲突重提固定回原分支；旧版无分支层队列自动迁移
import fs from 'node:fs'
import path from 'node:path'
import {
  startAll, stopAll, waitAll, sleep, call, GW, POST, FLD,
  TPORTS, restartService, cleanupData, stopService, startService, waitHealthy, DATA_DIR
} from './helpers.js'

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

// 构造确定 HLC（毫秒-计数），模拟现场设备离线时钟
function hlc(ts, l = 0) { return ts.toString(16).padStart(12, '0') + '-' + l.toString(16).padStart(6, '0') }

async function main() {
  cleanupData()
  startAll()
  await waitAll()
  console.log('— 五个服务（含现场同步 :8104）已就绪 —')

  const SIM = 'sim-branch'
  const TEAM = 'team-br'
  let r = await POST('/sims', { id: SIM, name: '分支协同推演', scenarioId: 's1', clientId: 'cmdr' })
  assertEq(r.status, 200, '创建推演')

  // 主干派发食品 100；随后分叉出 B 分支，B 分支再派发饮用水 50（两分支态势分化）
  r = await POST(`/sims/${SIM}/commands/dispatchResource`, {
    clientId: 'cmdr', baseId: 'rb-2', eventId: 'ev-001', type: 'food', qty: 100
  })
  assertEq(r.status, 200, '主干派发食品 100')
  let st = await stateOf(SIM)
  const dpFood = st.dispatches.find((d) => d.type === 'food').id

  r = await POST(`/sims/${SIM}/fork`, { clientId: 'cmdr', name: 'B 方案' })
  assertEq(r.status, 200, '从主干分叉 B 分支')
  const BR = j(r).branch.id

  r = await POST(`/sims/${SIM}/commands/dispatchResource`, {
    clientId: 'cmdr', branchId: BR, baseId: 'rb-2', eventId: 'ev-001', type: 'water', qty: 50
  })
  assertEq(r.status, 200, 'B 分支派发饮用水 50')
  const dpWater = (await stateOf(SIM, BR)).dispatches.find((d) => d.type === 'water').id
  assert(!(await stateOf(SIM, 'main')).dispatches.some((d) => d.type === 'water'), '主干无饮用水单（分支隔离前提）')

  /* ============ 1. 离线包按分支隔离 ============ */
  console.log('\n— 1. 离线包：按 推演+分支+队伍 隔离 —')
  r = await FLD('GET', `/sims/${SIM}/teams/${TEAM}/bundle?branch=main`)
  assertEq(r.status, 200, '现场服务代理主干离线包')
  assertEq(j(r).branchId, 'main', '主干离线包回链 main')
  const mainBundle = j(r).bundle
  assert(mainBundle.dispatches.some((d) => d.id === dpFood), '主干离线包含食品签收单')
  assert(!mainBundle.dispatches.some((d) => d.id === dpWater), '主干离线包不含 B 分支饮用水单')
  r = await FLD('GET', `/sims/${SIM}/teams/${TEAM}/bundle?branch=${encodeURIComponent(BR)}`)
  assertEq(j(r).branchId, BR, 'B 分支离线包回链分支 id')
  assert(j(r).bundle.dispatches.some((d) => d.id === dpWater), 'B 分支离线包含饮用水签收单')

  /* ============ 2. 按分支入队 → 补传固定回原分支 ============ */
  console.log('\n— 2. 队伍在两条分支离线作业：补传各回各分支 —')
  const t0 = Date.now()
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/actions`, {
    actions: [
      { clientActionId: 'm-reg', kind: 'registerTeam', branchId: 'main', name: '分支协同队', at: '09:00', hlc: hlc(t0 + 1000) },
      { clientActionId: 'm-sign', kind: 'signDispatch', branchId: 'main', dispatchId: dpFood, qty: 30, receiver: '李现场', at: '09:05', hlc: hlc(t0 + 2000) },
      { clientActionId: 'b-reg', kind: 'registerTeam', branchId: BR, name: '分支协同队B面', at: '09:10', hlc: hlc(t0 + 3000) },
      { clientActionId: 'b-pos', kind: 'reportPosition', branchId: BR, lng: 104.81, lat: 31.51, at: '09:15', hlc: hlc(t0 + 4000) }
    ]
  })
  assertEq(r.status, 200, '两条分支的离线动作入队')
  assertEq(j(r).queued, 4, '队列待发 4 条')
  assertEq(j(r).accepted[1].branchId, 'main', '入队回执钉住 main 分支')
  assertEq(j(r).accepted[3].branchId, BR, '入队回执钉住 B 分支')
  st = await stateOf(SIM)
  assert(!st.teams.some((x) => x.id === TEAM), '补传前主干无队伍')
  assertEq(st.dispatches.find((d) => d.id === dpFood).signedQty, 0, '补传前食品未签收')

  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/sync`, {})
  assertEq(r.status, 200, '触发联网补传')
  assertEq(j(r).synced, 4, '4 条动作全部补传成功')
  assertEq(j(r).remaining, 0, '队列清空')
  assertEq(j(r).branches?.main?.synced, 2, 'main 分组补传 2 条')
  assertEq(j(r).branches?.[BR]?.synced, 2, 'B 分组补传 2 条')

  const mainSt = await stateOf(SIM, 'main')
  const brSt = await stateOf(SIM, BR)
  assertEq(mainSt.dispatches.find((d) => d.id === dpFood).signedQty, 30, '签收动作补传回主干：实收 30')
  assertEq(mainSt.teams.find((x) => x.id === TEAM)?.name, '分支协同队', '主干队伍注册生效')
  assert(!mainSt.teams.find((x) => x.id === TEAM)?.position, '主干无位置上报（位置动作钉在 B 分支）')
  assertEq(brSt.dispatches.find((d) => d.id === dpFood).signedQty, 0, 'B 分支食品单未被主干签收污染')
  assertEq(brSt.teams.find((x) => x.id === TEAM)?.name, '分支协同队B面', 'B 分支队伍注册生效')
  assertEq(brSt.teams.find((x) => x.id === TEAM)?.position?.lng, 104.81, 'B 分支位置上报生效')

  /* ============ 3. 回执按分支隔离 ============ */
  console.log('\n— 3. 队列 / 回执按分支隔离查询 —')
  r = await FLD('GET', `/sims/${SIM}/teams/${TEAM}/outbox?branch=main`)
  assertEq(j(r).actions.length, 2, 'main 分支回执 2 条')
  assert(j(r).actions.every((a) => a.status === 'acked' && a.branchId === 'main'), 'main 回执全部已闭环且钉住 main')
  r = await FLD('GET', `/sims/${SIM}/teams/${TEAM}/outbox?branch=${encodeURIComponent(BR)}`)
  assertEq(j(r).actions.length, 2, 'B 分支回执 2 条')
  r = await FLD('GET', `/sims/${SIM}/teams/${TEAM}/outbox`)
  assertEq(j(r).actions.length, 4, '缺省查询全分支汇总 4 条')
  r = await FLD('GET', `/sims/${SIM}/field-teams`)
  const teamRow = j(r).teams.find((x) => x.teamId === TEAM)
  assert(!!teamRow && teamRow.acked === 4, '队伍汇总 4 条已闭环')
  assert(teamRow.branches.some((b) => b.branchId === 'main' && b.acked === 2), '汇总含 main 分支账')
  assert(teamRow.branches.some((b) => b.branchId === BR && b.acked === 2), '汇总含 B 分支账')

  /* ============ 4. sync 限定单分支：其余分支动作留队 ============ */
  console.log('\n— 4. sync 指定 branchId 只补该分支 —')
  await FLD('POST', `/sims/${SIM}/teams/${TEAM}/actions`, {
    actions: [
      { clientActionId: 'm-pos2', kind: 'reportPosition', branchId: 'main', lng: 104.82, lat: 31.52, at: '10:00', hlc: hlc(t0 + 5000) },
      { clientActionId: 'b-pos2', kind: 'reportPosition', branchId: BR, lng: 104.83, lat: 31.53, at: '10:05', hlc: hlc(t0 + 6000) }
    ]
  })
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/sync`, { branchId: BR })
  assertEq(j(r).synced, 1, '只补传 B 分支 1 条')
  assertEq(j(r).remaining, 1, 'main 分支动作继续留队')
  r = await FLD('GET', `/sims/${SIM}/teams/${TEAM}/outbox?status=queued&branch=main`)
  assertEq(j(r).actions.length, 1, '留队的正是 main 分支动作')
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/sync`, {})
  assertEq(j(r).synced, 1, '再补传主干 1 条')
  assertEq((await stateOf(SIM, 'main')).teams.find((x) => x.id === TEAM).position.lng, 104.82, '主干位置回写')
  assertEq((await stateOf(SIM, BR)).teams.find((x) => x.id === TEAM).position.lng, 104.83, 'B 分支位置回写')

  /* ============ 5. 切换期间在途请求：不裹挟、不重复、不错分支 ============ */
  console.log('\n— 5. 补传在途期间切分支继续入队 —')
  await FLD('POST', `/sims/${SIM}/teams/${TEAM}/actions`, {
    actions: [{ clientActionId: 'm-inflight', kind: 'reportPosition', branchId: 'main', lng: 104.84, lat: 31.54, at: '10:10', hlc: hlc(t0 + 7000) }]
  })
  // 不等待的补传（在途），随后立刻向另一分支入队 —— 模拟切换分支瞬间的在途请求
  const p1 = FLD('POST', `/sims/${SIM}/teams/${TEAM}/sync`, {})
  await FLD('POST', `/sims/${SIM}/teams/${TEAM}/actions`, {
    actions: [{ clientActionId: 'b-inflight', kind: 'reportPosition', branchId: BR, lng: 104.85, lat: 31.55, at: '10:15', hlc: hlc(t0 + 8000) }]
  })
  // 并发第二次补传：单队伍单在途，复用同一批
  const [r1, r2] = await Promise.all([p1, FLD('POST', `/sims/${SIM}/teams/${TEAM}/sync`, {})])
  assertEq(r1.status, 200, '在途补传正常返回')
  assertEq(r2.status, 200, '并发补传去重复用')
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/sync`, {})
  assertEq(j(r).remaining, 0, '在途期间入队的动作最终补传完毕')
  const mainTeam = (await stateOf(SIM, 'main')).teams.find((x) => x.id === TEAM)
  const brTeam = (await stateOf(SIM, BR)).teams.find((x) => x.id === TEAM)
  assertEq(mainTeam.positions.length, 2, '主干轨迹恰好 2 点（在途动作不重复入账）')
  assertEq(mainTeam.position.lng, 104.84, '主干最新位置为 m-inflight')
  assertEq(brTeam.positions.length, 3, 'B 分支轨迹恰好 3 点')
  assertEq(brTeam.position.lng, 104.85, 'B 分支最新位置为 b-inflight（未裹挟进主干批次）')

  /* ============ 6. 冲突重提：固定回原分支 ============ */
  console.log('\n— 6. B 分支冲突动作重提仍回 B 分支 —')
  await FLD('POST', `/sims/${SIM}/teams/${TEAM}/actions`, {
    actions: [{ clientActionId: 'b-cf', kind: 'signDispatch', branchId: BR, dispatchId: dpWater, qty: 999, at: '11:00', hlc: hlc(t0 + 9000) }]
  })
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/sync`, {})
  assertEq(j(r).synced, 0, '超量签收无成功项')
  r = await FLD('GET', `/sims/${SIM}/teams/${TEAM}/outbox?status=conflict`)
  const cf = j(r).actions.find((a) => a.clientActionId === 'b-cf')
  assert(!!cf && cf.branchId === BR, '冲突项落账并钉住 B 分支')
  assert(!!cf.result, '冲突原因透传留痕')
  // 修正数量后同 id 重提：不显式给分支，服务端按原记录钉住 B 分支
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/actions`, {
    actions: [{ clientActionId: 'b-cf', kind: 'signDispatch', dispatchId: dpWater, qty: 10, receiver: '王现场', at: '11:05', hlc: hlc(t0 + 9500) }]
  })
  assertEq(j(r).accepted[0].branchId, BR, '同 id 重提缺省钉住原分支')
  r = await FLD('POST', `/sims/${SIM}/teams/${TEAM}/sync`, {})
  assertEq(j(r).synced, 1, '重提补传成功')
  assertEq((await stateOf(SIM, BR)).dispatches.find((d) => d.id === dpWater).signedQty, 10, 'B 分支饮用水实收 10')
  assert(!(await stateOf(SIM, 'main')).dispatches.some((d) => d.id === dpWater), '主干仍无饮用水单（重提未串分支）')

  /* ============ 7. 旧队列迁移：无分支层 → 按分支隔离 ============ */
  console.log('\n— 7. 旧版队列（无分支层）自动迁移 —')
  const SIM2 = 'sim-legacy'
  r = await POST('/sims', { id: SIM2, name: '旧队列迁移推演', scenarioId: 's1', clientId: 'cmdr' })
  assertEq(r.status, 200, '创建迁移用推演')
  r = await POST(`/sims/${SIM2}/fork`, { clientId: 'cmdr', name: '迁移目标分支' })
  const LBR = j(r).branch.id

  stopService('field'); await sleep(150)
  // 手工落旧版布局：<root>/<simId>/<teamId>/queue.jsonl + clock.json（无分支层）
  const legacyDir = path.join(DATA_DIR, 'field', SIM2, 'team-old')
  fs.mkdirSync(legacyDir, { recursive: true })
  const upsert = (action) => JSON.stringify({ op: 'upsert', action: { status: 'queued', attempts: 0, queuedAt: new Date().toISOString(), updatedAt: new Date().toISOString(), ...action } })
  fs.writeFileSync(legacyDir + '/queue.jsonl', [
    upsert({ clientActionId: 'old-1', kind: 'registerTeam', teamId: 'team-old', simId: SIM2, name: '老队伍', at: '08:00', hlc: hlc(t0 - 3000) }),
    upsert({ clientActionId: 'old-2', kind: 'reportPosition', teamId: 'team-old', simId: SIM2, branchId: LBR, lng: 104.66, lat: 31.66, at: '08:20', hlc: hlc(t0 - 1000) }),
    upsert({ clientActionId: 'old-3', kind: 'registerTeam', teamId: 'team-old', simId: SIM2, branchId: LBR, name: '老队伍B面', at: '08:10', hlc: hlc(t0 - 2000) })
  ].join('\n') + '\n')
  fs.writeFileSync(legacyDir + '/clock.json', JSON.stringify({ ts: t0, l: 7 }))
  startService('field'); await waitHealthy(TPORTS.field)

  assert(!fs.existsSync(legacyDir), '旧版队伍目录已移除')
  assert(fs.existsSync(path.join(DATA_DIR, 'field', SIM2, 'main', 'team-old', 'queue.jsonl')), '无分支动作迁入 main 队列')
  assert(fs.existsSync(path.join(DATA_DIR, 'field', SIM2, LBR, 'team-old', 'queue.jsonl')), '带分支动作迁入对应分支队列')
  assert(fs.existsSync(path.join(DATA_DIR, 'field', SIM2, 'team-old.clock.json')), '队伍时钟平移保留')

  r = await FLD('GET', `/sims/${SIM2}/teams/team-old/outbox`)
  assertEq(j(r).actions.length, 3, '迁移后 3 条动作可查询')
  assertEq(j(r).actions.find((a) => a.clientActionId === 'old-1')?.branchId, 'main', 'old-1 归入 main')
  assertEq(j(r).actions.find((a) => a.clientActionId === 'old-2')?.branchId, LBR, 'old-2 保留原分支')

  r = await FLD('POST', `/sims/${SIM2}/teams/team-old/sync`, {})
  assertEq(j(r).synced, 3, '迁移动作全部补传成功')
  const oldMain = (await stateOf(SIM2, 'main')).teams.find((x) => x.id === 'team-old')
  const oldBr = (await stateOf(SIM2, LBR)).teams.find((x) => x.id === 'team-old')
  assertEq(oldMain?.name, '老队伍', '迁移动作在主干生效')
  assert(!oldMain?.position, '主干无位置（位置动作钉在迁移分支）')
  assertEq(oldBr?.name, '老队伍B面', '迁移分支队伍注册生效')
  assertEq(oldBr?.position?.lng, 104.66, '迁移分支位置回写')

  // 迁移幂等：再次重启现场服务，队列不重复、不丢失
  await restartService('field', 300)
  r = await FLD('GET', `/sims/${SIM2}/teams/team-old/outbox`)
  assertEq(j(r).actions.length, 3, '重启后队列不重复不丢失（迁移幂等）')
  assert(j(r).actions.every((a) => a.status === 'acked'), '迁移动作终态保持')

  console.log(`\n结果：${passed} 通过，${failed} 失败`)
  stopAll()
  if (failed) process.exit(1)
}

main().catch((e) => { console.error(e); stopAll(); process.exit(1) })
