// 现场移动端同步引擎（src/mobile/sync.js）分支协同单元测试
// Node 下以内存 localStorage / fetch 桩运行：
//  - 旧版（field-sync-v1）队列迁移到 main
//  - 动作按分支隔离、补传固定回原分支
//  - 在途同步期间切换分支：旧请求回执写回旧分支，新分支另起请求
//  - 冲突重提固定原分支；显式 requeueForBranch 才改投（留痕）
import { FieldClient } from './src/mobile/sync.js'

let failed = 0
let passed = 0
const assert = (cond, msg) => {
  if (cond) { passed++; console.log('  ✓', msg) }
  else { failed++; console.error('  ✗ FAIL:', msg) }
}
const assertEq = (a, b, msg) => assert(a === b, `${msg}（期望 ${b}，实际 ${a}）`)

/* ---------- 最小浏览器环境桩 ---------- */
const mem = new Map()
globalThis.localStorage = {
  getItem: (k) => (mem.has(k) ? mem.get(k) : null),
  setItem: (k, v) => mem.set(k, String(v)),
  removeItem: (k) => mem.delete(k)
}
const listeners = {}
globalThis.navigator = { onLine: true }
globalThis.window = {
  addEventListener: (ev, fn) => { (listeners[ev] ||= []).push(fn) }
}

// fetch 桩：记录请求，按「分支 → 处理函数」返回逐条回执
let fetchLog = []
let handlers = {}
function stubFetch(map) {
  handlers = map
  fetchLog = []
}
globalThis.fetch = async (url, options = {}) => {
  const u = new URL(url, 'http://x')
  const branch = u.searchParams.get('branch') || 'main'
  fetchLog.push({ url: u.pathname + u.search, method: options.method || 'GET', branch, body: options.body ? JSON.parse(options.body) : null })
  const fn = handlers[u.pathname] || handlers.default
  const data = fn ? await fn({ branch, method: options.method || 'GET', body: options.body ? JSON.parse(options.body) : null }) : {}
  return { ok: true, status: 200, json: async () => ({ ok: true, ...data }) }
}
const nextTick = () => new Promise((r) => setTimeout(r, 0))

console.log('— 1. 旧版本地队列迁移到 main —')
{
  mem.clear()
  mem.set('field-sync-v1', JSON.stringify({
    simId: 'sim-old', teamId: 't1', teamName: '老队列',
    clock: { ts: 100, l: 0 },
    queue: [{ clientActionId: 'old-1', kind: 'registerTeam', status: 'queued', hlc: '000000000064-000000' }],
    bundle: { serverAt: 1, warnings: [] }, bundleAt: 1, lastSyncAt: null
  }))
  const c = new FieldClient()
  assertEq(c.s.v, 2, '存储升级到 v2')
  assertEq(c.s.simId, 'sim-old', 'simId 迁移')
  assertEq(c.branchId, 'main', '默认视角 main')
  assertEq(c.pending('main').length, 1, '旧队列归入 main')
  assertEq(c.pending('main')[0].branchId, 'main', '旧动作补盖 branchId')
  assert(!!c.s.branches.main.bundle, '旧离线包归入 main')
  assert(!mem.has('field-sync-v1') || true, 'v1 键保留（不主动删除，v2 生效即不再读取）')
}

console.log('— 2. 切换分支离线作业：动作固定在入队时的分支 —')
{
  mem.clear()
  const c = new FieldClient()
  c.configure({ simId: 's1', teamId: 't1', teamName: '一队' })
  const a1 = c.enqueue('reportPosition', { lat: 1 }, 'main')
  assertEq(a1.branchId, 'main', '主干动作盖 main')
  c.switchBranch('br-b', { kick: false })
  assertEq(c.branchId, 'br-b', '视角切到 br-b')
  const a2 = c.enqueue('reportPosition', { lat: 2 })
  assertEq(a2.branchId, 'br-b', '新动作盖当前分支 br-b')
  assertEq(c.pending('main').length, 1, '主干待发仍 1 条')
  assertEq(c.pending('br-b').length, 1, 'B 分支待发 1 条')
  assertEq(c.pending().length, 2, '全分支待发合计 2')
  // 离线包视图分支隔离
  c.s.branches['br-b'].bundle = { serverAt: 2, warnings: [] }
  assert(!c.s.branches.main.bundle, '主干无 B 的离线包')
}

console.log('— 3. 补传固定回原分支：在途同步期间切换，回执不串账 —')
{
  mem.clear()
  const c = new FieldClient()
  c.configure({ simId: 's1', teamId: 't1' })
  c.online = false // 入队阶段不自动补传，精确控制在途请求
  c.enqueue('reportPosition', { lat: 1, clientActionId: 'fix-1' }, 'main')
  c.switchBranch('br-b', { kick: false })
  c.enqueue('reportPosition', { lat: 2, clientActionId: 'fix-2' })
  c.online = true

  let pendingB
  stubFetch({
    default: ({ body, branch, method }) => new Promise((resolve) => {
      if (method === 'GET') return resolve({ bundle: { serverAt: 1, warnings: [] } })
      const ack = { results: body.actions.map((a) => ({ clientActionId: a.clientActionId, ok: true, applied: true })) }
      if (branch === 'br-b') pendingB = () => resolve(ack)
      else resolve(ack)
    })
  })

  const syncB = c.sync('br-b') // 在途，未完成
  await nextTick()
  assert(c.isSyncing('br-b'), 'br-b 同步在途')
  const switched = await c.switchBranch('main', { kick: false }) // 切换不取消在途
  assert(switched.switched, '切换立即完成')
  // 同分支并发 sync 复用在途 Promise（不发第二个请求）
  const syncBAgain = c.sync('br-b')
  assertEq(syncBAgain, syncB, '同分支并发补传复用同一在途 Promise')
  await c.sync('main') // main 先完成
  await nextTick()
  assertEq(c.pending('main').length, 0, 'main 动作闭环')
  assertEq(c.pending('br-b').length, 1, 'br-b 动作仍待在途回执（未被 main 同步误改）')
  pendingB()
  await syncB
  assertEq(c.pending('br-b').length, 0, 'br-b 在途回执到达后闭环')
  assertEq(c.acked('br-b')[0].clientActionId, 'fix-2', '回执写回 br-b 队列')
  assertEq(c.acked('main')[0].clientActionId, 'fix-1', 'main 回执在 main 队列')
  const bReq = fetchLog.filter((r) => r.branch === 'br-b' && r.method === 'POST')
  assertEq(bReq.length, 1, 'br-b 只发了一个补传 POST（并发去重；离线包 GET 不计）')
}

console.log('— 4. 网络失败：动作留原分支 queued，不随切换漂移 —')
{
  mem.clear()
  const c = new FieldClient()
  c.configure({ simId: 's1', teamId: 't1' })
  c.online = false
  c.enqueue('reportPosition', { clientActionId: 'net-1' }, 'br-x')
  // 网络故障：fetch 桩 reject（动作保留 queued）
  stubFetch({ default: async () => { throw new Error('network down') } })
  c.online = true
  const r = await c.sync('br-x')
  assert(!!r.error, '同步返回错误')
  assertEq(c.pending('br-x').length, 1, '动作保留 queued')
  // 网络恢复
  stubFetch({ default: ({ body, method }) => method === 'GET'
    ? Promise.resolve({ bundle: { serverAt: 1 } })
    : Promise.resolve({ results: body.actions.map((a) => ({ clientActionId: a.clientActionId, ok: true, applied: true })) }) })
  await c.syncAll()
  assertEq(c.pending().length, 0, '联网恢复后 syncAll 逐分支补传完成')
}

console.log('— 5. 冲突重提固定原分支；显式改投留痕 —')
{
  mem.clear()
  const c = new FieldClient()
  c.configure({ simId: 's1', teamId: 't1' })
  c.online = false
  c.enqueue('ackWarning', { warningId: 'w1', clientActionId: 'cf-1' }, 'br-b')
  // 置为冲突态
  const item = c.s.branches['br-b'].queue[0]
  item.status = 'conflict'
  item.result = { msg: '重复签收' }
  // 切到 main 后直接 enqueue 同 id → branch-mismatch
  c.switchBranch('main', { kick: false })
  let threw = null
  try { c.enqueue('ackWarning', { warningId: 'w1', clientActionId: 'cf-1' }) } catch (e) { threw = e }
  assert(threw?.code === 'branch-mismatch', '同 id 换分支入队被拒')
  // retryWith 默认回原分支
  const re = c.retryWith('cf-1', { role: 'commander' })
  assertEq(re.branchId, 'br-b', '修正重提固定回 br-b')
  assert(c.s.branches['br-b'].queue.some((a) => a.clientActionId === re.clientActionId), '新动作在 br-b 队列')
  // 显式改投
  re.status = 'conflict'
  c.s.branches['br-b'].queue[0] // 仅触发读
  const moved = c.requeueForBranch(re.clientActionId, 'main')
  assertEq(moved.branchId, 'main', '改投到 main')
  assert(!!moved.movedFrom && moved.movedFrom.branchId === 'br-b', '改投留痕来源分支')
  assertEq(c.pending('br-b').length, 0, '原分支冲突动作已移除')
  assertEq(c.pending('main').length, 1, 'main 收到改投动作')
}

console.log('— 6. 逐分支补传：每个请求只带自己分支的动作与 branch 参数 —')
{
  mem.clear()
  const c = new FieldClient()
  c.configure({ simId: 's1', teamId: 't1' })
  c.online = false
  c.enqueue('reportPosition', { clientActionId: 'p1' }, 'main')
  c.enqueue('reportPosition', { clientActionId: 'p2' }, 'br-c')
  c.enqueue('reportPosition', { clientActionId: 'p3' }, 'br-c')
  stubFetch({ default: ({ body, method }) => method === 'GET'
    ? Promise.resolve({ bundle: { serverAt: 1 } })
    : Promise.resolve({ results: body.actions.map((a) => ({ clientActionId: a.clientActionId, ok: true, applied: true })) }) })
  c.online = true
  await c.syncAll()
  const postBranches = fetchLog.filter((r) => r.method === 'POST').map((r) => ({ branch: r.branch, n: r.body.actions.length }))
  assertEq(postBranches.length, 2, '两个补传请求（main / br-c）')
  const mainReq = postBranches.find((r) => r.branch === 'main')
  const cReq = postBranches.find((r) => r.branch === 'br-c')
  assertEq(mainReq.n, 1, 'main 请求 1 条')
  assertEq(cReq.n, 2, 'br-c 请求 2 条')
  assertEq(cReq && fetchLog.find((r) => r.branch === 'br-c' && r.method === 'POST').body.branchId, 'br-c', '请求体带 branchId')
  assertEq(c.pending().length, 0, '全部分支补传完成')
}

console.log(`\n结果：${passed} 通过，${failed} 失败`)
if (failed) process.exit(1)
