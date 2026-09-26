<template>
  <div class="phone">
    <!-- 登录 / 队伍绑定 -->
    <section v-if="!client.configured" class="login card">
      <div class="brand">🛡️ 现场协同</div>
      <p class="sub">离线接收预警 · 位置/路况上报 · 物资签收 · 抢修进度</p>
      <label>推演 ID
        <input v-model="form.simId" placeholder="如 sim-a" />
      </label>
      <label>队伍标识
        <input v-model="form.teamId" placeholder="如 team-1" />
      </label>
      <label>队伍名称
        <input v-model="form.teamName" placeholder="如 青川抢修一队" />
      </label>
      <button class="primary block" @click="login">进入现场模式</button>
    </section>

    <template v-else>
      <!-- 顶栏：连接状态 / 分支视角 / 离线包时间 / 补传按钮 -->
      <header class="topbar">
        <div class="who">
          <strong>{{ client.s.teamName || client.s.teamId }}</strong>
          <small>{{ client.s.simId }}</small>
        </div>
        <!-- 推演分支视角切换：离线也可切，动作固定回各自原分支 -->
        <label class="branch-pick">
          🌿
          <input
            :list="branchListId"
            :value="client.branchId"
            @change="switchBranch"
          />
          <datalist :id="branchListId">
            <option v-for="b in branchOptions" :key="b.branchId" :value="b.branchId">
              {{ b.branchId }}（待发 {{ b.queued }} · 冲突 {{ b.conflict }}）
            </option>
          </datalist>
        </label>
        <span class="net" :class="client.online ? 'on' : 'off'">
          {{ client.online ? '🟢 在线' : '🔴 离线' }}
        </span>
        <button class="sync-btn" :disabled="client.syncing || !pendingTotal" @click="doSyncAll">
          {{ client.syncing ? '补传中…' : (`补传${pendingTotal ? `(${pendingTotal})` : ''}`) }}
        </button>
      </header>

      <div class="hint" v-if="!client.online">
        📵 当前离线（分支 {{ client.branchId }}），操作已存入本机队列；联网后将按发生顺序自动补传回原分支
      </div>
      <div class="hint ok" v-else-if="conflictTotal">
        ⚠️ {{ conflictTotal }} 条补传存在冲突，请在「队列」中处理
      </div>

      <!-- 预警 -->
      <section v-show="tab === 'warn'" class="page">
        <h2>🚨 预警接收 <small>离线可查看缓存</small></h2>
        <p class="stale" v-if="bundleStamp">离线包更新于 {{ bundleStamp }}</p>
        <div v-if="!warnings.length" class="empty">暂无生效预警</div>
        <div v-for="w in warnings" :key="w.id" class="card warn" :class="w.level">
          <div class="warn-head">
            <span class="lv">{{ levelLabel(w.level) }}</span>
            <strong>{{ w.title }}</strong>
          </div>
          <div class="warn-meta">
            <span>来源 {{ w.source }}</span><span>发布 {{ w.issuedAt }}</span>
          </div>
          <div class="roles">
            <span v-for="r in w.targets" :key="r" class="role" :class="{ done: !!w.acks[r] }">
              {{ roleLabel(r) }}{{ w.acks[r] ? '✓' : '' }}
            </span>
          </div>
          <div class="warn-actions">
            <button v-if="w.pendingRoles.length" class="primary" @click="ack(w)">
              签收预警（{{ w.pendingRoles.map(roleLabel).join('、') }}）
            </button>
            <span v-else class="all-done">本队相关角色已全部签收 ✅</span>
          </div>
        </div>
      </section>

      <!-- 任务：物资签收 + 抢修工单 -->
      <section v-show="tab === 'task'" class="page">
        <h2>📦 我的任务</h2>

        <h3>物资签收</h3>
        <div v-if="!dispatches.length" class="empty">无在途签收单</div>
        <details v-for="d in dispatches" :key="d.id" class="card task">
          <summary>
            <span>{{ d.typeLabel }} {{ d.qty }}{{ d.unit }}</span>
            <b>{{ d.status === 'held' ? '挂起' : '在途' }}</b>
          </summary>
          <p class="line">去向：{{ d.dest }}｜来自 {{ d.baseName }}｜待签 <b>{{ d.outstanding }}{{ d.unit }}</b></p>
          <div class="form-row">
            <label>实收 <input v-model.number="sf(d).qty" type="number" min="0" placeholder="0" /></label>
            <label>短缺 <input v-model.number="sf(d).shortQty" type="number" min="0" placeholder="0" /></label>
          </div>
          <label class="full">签收人 <input v-model="sf(d).receiver" :placeholder="client.s.teamName || '现场签收员'" /></label>
          <button class="primary" @click="sign(d)">提交签收（离线可先存）</button>
        </details>

        <h3>道路抢修工单</h3>
        <div v-if="!orders.length" class="empty">无进行中工单</div>
        <details v-for="o in orders" :key="o.id" class="card task">
          <summary>
            <span>🔧 {{ o.blockName }}</span>
            <b>{{ orderStatusLabel(o.status) }} {{ o.status === 'accepted' ? o.progress + '%' : '' }}</b>
          </summary>
          <div class="bar"><i :style="{ width: o.progress + '%' }"></i></div>
          <button v-if="o.status === 'dispatched'" class="primary" @click="acceptOrder(o)">接单</button>
          <template v-if="o.status === 'accepted'">
            <div class="form-row">
              <label>进度% <input v-model.number="progForm[o.id]" type="number" min="0" max="100" /></label>
            </div>
            <button class="primary" @click="reportProg(o)">上报进度</button>
            <div class="form-row">
              <label>延期至 <input v-model="progForm[o.id + '_deadline']" placeholder="如 18:30" /></label>
            </div>
            <button @click="delay(o)">申请延期</button>
            <div class="form-row">
              <label>实际用人 <input v-model.number="progForm[o.id + '_p']" type="number" min="0" /></label>
              <label>实际用车 <input v-model.number="progForm[o.id + '_v']" type="number" min="0" /></label>
            </div>
            <button class="primary" :disabled="o.progress < 100" @click="finish(o)">完工上报（待验收）</button>
            <p v-if="o.progress < 100" class="tip">进度达 100% 后才能完工上报（当前 {{ o.progress }}%）</p>
          </template>
        </details>
      </section>

      <!-- 上报：位置 + 道路变化 -->
      <section v-show="tab === 'report'" class="page">
        <h2>🛰️ 现场上报</h2>
        <div class="card">
          <h3>位置上报</h3>
          <div class="form-row">
            <label>经度 <input v-model.number="pos.lng" type="number" step="0.0001" placeholder="104.74" /></label>
            <label>纬度 <input v-model.number="pos.lat" type="number" step="0.0001" placeholder="31.46" /></label>
          </div>
          <button class="ghost" @click="locate">📍 使用本机定位</button>
          <button class="primary" @click="reportPos">上报位置</button>
        </div>

        <div class="card">
          <h3>道路变化</h3>
          <div class="seg">
            <button :class="{ on: road.roadKind === 'closed' }" @click="road.roadKind = 'closed'">🚳 新封闭</button>
            <button :class="{ on: road.roadKind === 'reopened' }" @click="road.roadKind = 'reopened'">✅ 恢复通行</button>
            <button :class="{ on: road.roadKind === 'condition' }" @click="road.roadKind = 'condition'">🛣️ 路况备注</button>
          </div>
          <label v-if="road.roadKind !== 'closed'" class="full">
            关联阻断
            <select v-model="road.blockId">
              <option value="">请选择</option>
              <option v-for="b in activeBlocks" :key="b.id" :value="b.id">{{ b.name }}</option>
            </select>
          </label>
          <template v-if="road.roadKind === 'closed'">
            <p class="tip">依次输入/添加封闭区顶点（至少 3 个，经纬度）；现场可按行进轨迹逐点追加。</p>
            <div v-for="(pt, i) in road.points" :key="i" class="form-row">
              <label>经度 <input v-model.number="pt.lng" type="number" step="0.0001" /></label>
              <label>纬度 <input v-model.number="pt.lat" type="number" step="0.0001" /></label>
              <button class="mini danger" @click="road.points.splice(i, 1)">删</button>
            </div>
            <button class="ghost" @click="road.points.push({ lng: pos.lng ?? null, lat: pos.lat ?? null })">＋ 用当前位置加顶点</button>
            <label class="full">说明 <input v-model="road.reason" placeholder="如 塌方双向中断" /></label>
          </template>
          <label v-else class="full">备注 <input v-model="road.note" placeholder="恢复通行 / 路面泥泞减速等" /></label>
          <button class="primary" @click="reportRoad">上报道路变化</button>
        </div>
      </section>

      <!-- 队列：补传状态 / 冲突处理（按分支隔离，补传固定回原分支） -->
      <section v-show="tab === 'queue'" class="page">
        <h2>📤 同步队列
          <small>本分支待发 {{ pendingCount }} · 冲突 {{ currentConflicts.length }} · 全部待发 {{ pendingTotal }}</small>
        </h2>
        <div class="seg">
          <button :class="{ on: queueScope === 'current' }" @click="queueScope = 'current'">本分支（{{ client.branchId }}）</button>
          <button :class="{ on: queueScope === 'all' }" @click="queueScope = 'all'">全部分支（{{ branchSummaries.length }}）</button>
        </div>
        <button class="primary block" :disabled="!pendingTotal || client.syncing" @click="doSyncAll">
          {{ client.syncing ? '补传中…' : '立即联网补传（各分支按因果顺序回原分支）' }}
        </button>
        <div v-if="queueScope === 'all'" class="branch-summary">
          <div v-for="b in branchSummaries" :key="b.branchId" class="brow" :class="{ current: b.branchId === client.branchId }">
            <button class="link" @click="switchBranch(b.branchId)">🌿 {{ b.branchId }}</button>            <span>待发 {{ b.queued }} · 冲突 {{ b.conflict }} · 已闭环 {{ b.acked }}</span>
            <button v-if="b.queued" class="mini" :disabled="client.isSyncing(b.branchId)" @click="client.sync(b.branchId)">
              {{ client.isSyncing(b.branchId) ? '补传中…' : '补传该分支' }}
            </button>
          </div>
        </div>
        <div v-if="!visibleQueue.length" class="empty">队列为空，所有操作均已闭环</div>
        <div v-for="a in [...visibleQueue].reverse()" :key="a.clientActionId" class="card q" :class="a.status">
          <div class="q-head">
            <b>{{ actionLabel(a.kind) }}</b>
            <span class="badge branch" v-if="a.branchId !== client.branchId">🌿 {{ a.branchId }}</span>
            <span class="badge">{{ statusLabel(a.status) }}</span>
          </div>
          <small>{{ a.at }} · {{ a.clientActionId.slice(0, 14) }}… · 尝试 {{ a.attempts }} · 固定分支 🌿{{ a.branchId }}</small>
          <p v-if="a.movedFrom" class="adv">由分支 {{ a.movedFrom.branchId }} 的 {{ a.movedFrom.clientActionId.slice(0, 12) }}… 改投而来</p>
          <div v-if="a.status === 'conflict'" class="conflict">
            <p>⛔ {{ a.result?.msg || '业务前置不满足或与并发处置冲突' }}</p>
            <p v-if="a.result?.advisory?.length" class="adv">联动提示：{{ a.result.advisory.join('；') }}</p>
            <div class="q-actions">
              <button class="mini" @click="quickRetry(a)">原样重提（回原分支）</button>
              <button class="mini" @click="moveRetry(a)">修正后改投当前分支</button>
              <button class="mini danger" @click="client.discard(a.clientActionId)">丢弃</button>
            </div>
          </div>
        </div>
      </section>

      <!-- 底栏 -->
      <nav class="tabbar">
        <button :class="{ on: tab === 'warn' }" @click="tab = 'warn'">
          🚨<span>预警<i v-if="unackedCount" class="dot">{{ unackedCount }}</i></span>
        </button>
        <button :class="{ on: tab === 'task' }" @click="tab = 'task'">📦<span>任务</span></button>
        <button :class="{ on: tab === 'report' }" @click="tab = 'report'">🛰️<span>上报</span></button>
        <button :class="{ on: tab === 'queue' }" @click="tab = 'queue'">
          📤<span>队列<i v-if="pendingTotal" class="dot red">{{ pendingTotal }}</i><i v-else-if="conflictTotal" class="dot">{{ conflictTotal }}</i></span>
        </button>
      </nav>
    </template>
  </div>
</template>

<script setup>
import { reactive, ref, computed, onMounted, onUnmounted } from 'vue'
import { FieldClient } from './sync.js'

const client = new FieldClient(() => {}) // 用下面的响应式镜像驱动视图
const form = reactive({ simId: '', teamId: '', teamName: '' })
const tab = ref('warn')
const queueScope = ref('current') // 队列页视角：current | all

// 响应式镜像：每次 client 变更时 tick++，模板读取 client.s（普通对象也会重渲染）
const tick = ref(0)
client.onChange = () => { tick.value++ }

const pos = reactive({ lng: null, lat: null })
const road = reactive({ roadKind: 'closed', blockId: '', reason: '', note: '', points: [] })
const signForm = reactive({})
const progForm = reactive({})

function signSeed(d) {
  if (!signForm[d.id]) signForm[d.id] = { qty: d.outstanding, shortQty: 0, receiver: '' }
  return signForm[d.id]
}
// 模板中 v-model 需要可写的响应式属性；惰性建种子
function sf(d) { return signSeed(d) }

const bundle = computed(() => { void tick.value; return client.s.branches[client.branchId]?.bundle })
const warnings = computed(() => bundle.value?.warnings || [])
const dispatches = computed(() => bundle.value?.dispatches || [])
const orders = computed(() => bundle.value?.orders || [])
const activeBlocks = computed(() => bundle.value?.blocks || [])
const pendingCount = computed(() => { void tick.value; return client.pending(client.branchId).length })
const pendingTotal = computed(() => { void tick.value; return client.pending().length })
const conflictTotal = computed(() => { void tick.value; return client.conflicts().length })
const currentConflicts = computed(() => { void tick.value; return client.conflicts(client.branchId) })
const unackedCount = computed(() => warnings.value.filter((w) => w.pendingRoles.includes('field')).length)
const bundleStamp = computed(() => {
  void tick.value
  const at = client.s.branches[client.branchId]?.bundleAt
  if (!at) return ''
  return new Date(at).toLocaleTimeString('zh-CN', { hour12: false })
})
// 分支切换器候选项：本地留过痕的分支（离线也可切换）
const branchListId = 'field-branches'
const branchSummaries = computed(() => { void tick.value; return client.allBranchSummaries() })
const branchOptions = computed(() => {
  void tick.value
  const known = client.allBranchSummaries()
  if (!known.some((b) => b.branchId === client.branchId)) {
    return [{ branchId: client.branchId, queued: 0, conflict: 0, acked: 0 }, ...known]
  }
  return known
})
// 队列页展示：本分支（按当前视角）或全部分支展开
const visibleQueue = computed(() => {
  void tick.value
  if (queueScope.value === 'all') return client._filter(null)
  return client._filter(null, client.branchId)
})

async function switchBranch(arg) {
  // 顶栏 @change 传 Event；队列页按钮直接传分支 id
  const raw = arg?.target ? arg.target.value : arg
  const branchId = (raw || '').trim()
  if (!branchId || branchId === client.branchId) {
    if (arg?.target) arg.target.value = client.branchId
    return
  }
  const switched = await client.switchBranch(branchId).catch((err) => { alert('切换失败：' + err.message); return null })
  if (!switched && arg?.target) arg.target.value = client.branchId
  if (switched) await refresh()
}

function login() {
  if (!form.simId.trim() || !form.teamId.trim()) return alert('请填写推演 ID 与队伍标识')
  client.configure({ simId: form.simId.trim(), teamId: form.teamId.trim(), teamName: form.teamName.trim() })
  // 注册队伍（离线入队，联网补传，固定在当前分支 main）
  client.enqueue('registerTeam', { teamId: form.teamId.trim(), name: form.teamName.trim(), capabilities: ['repair'] })
  refresh()
}

async function doSync() {
  await client.sync()
  await refresh()
}
async function doSyncAll() {
  await client.syncAll()
  await refresh()
}
async function refresh() {
  try { await client.fetchBundle() } catch { /* 离线时使用当前分支缓存 */ }
}
let timer = null
onMounted(() => {
  form.simId = client.s.simId; form.teamId = client.s.teamId; form.teamName = client.s.teamName
  if (client.configured) refresh()
  timer = setInterval(() => { if (navigator.onLine && client.configured) refresh() }, 15000)
})
onUnmounted(() => clearInterval(timer))

function ack(w) {
  client.enqueue('ackWarning', { warningId: w.id, role: 'field', by: client.s.teamName || client.s.teamId })
}
function sign(d) {
  const f = signSeed(d)
  const qty = Math.round(f.qty || 0), shortQty = Math.round(f.shortQty || 0)
  if (qty <= 0 && shortQty <= 0) return alert('请填写实收或短缺数量')
  if (qty + shortQty > d.outstanding) return alert(`签认总数不能超过待签 ${d.outstanding}`)
  client.enqueue('signDispatch', { dispatchId: d.id, qty, shortQty, receiver: f.receiver || undefined })
}
function acceptOrder(o) { client.enqueue('reportRepair', { orderId: o.id, stage: 'accept' }) }
function reportProg(o) {
  const p = Math.round(progForm[o.id] ?? NaN)
  if (!(p >= 0 && p <= 100)) return alert('请填写 0-100 的进度')
  client.enqueue('reportRepair', { orderId: o.id, stage: 'progress', progress: p })
}
function delay(o) {
  client.enqueue('reportRepair', { orderId: o.id, stage: 'delay', deadline: progForm[o.id + '_deadline'] || '' })
}
function finish(o) {
  if ((o.progress || 0) < 100) return alert(`进度未达到 100%（当前 ${o.progress || 0}%），不能完工上报`)
  client.enqueue('reportRepair', {
    orderId: o.id, stage: 'finish',
    used: { personnel: Math.round(progForm[o.id + '_p'] || 0), vehicles: Math.round(progForm[o.id + '_v'] || 0) }
  })
}
function locate() {
  if (!navigator.geolocation) return alert('本机不支持定位，请手动输入')
  navigator.geolocation.getCurrentPosition(
    (p) => { pos.lng = +p.coords.longitude.toFixed(6); pos.lat = +p.coords.latitude.toFixed(6) },
    () => alert('定位失败，请手动输入'),
    { enableHighAccuracy: true, timeout: 8000 }
  )
}
function reportPos() {
  if (typeof pos.lng !== 'number' || typeof pos.lat !== 'number') return alert('请先获取或输入坐标')
  client.enqueue('reportPosition', { teamId: client.s.teamId, lng: pos.lng, lat: pos.lat })
}
function reportRoad() {
  if (road.roadKind === 'closed') {
    const pts = road.points.filter((p) => typeof p.lng === 'number' && typeof p.lat === 'number')
    if (pts.length < 3) return alert('封闭区至少需要 3 个有效顶点')
    client.enqueue('reportRoad', {
      roadKind: 'closed', polygon: pts.map((p) => [p.lng, p.lat]),
      name: `现场上报-${road.points.length}点`, reason: road.reason, by: client.s.teamName
    })
  } else if (road.roadKind === 'reopened') {
    if (!road.blockId) return alert('请选择已恢复的阻断')
    client.enqueue('reportRoad', { roadKind: 'reopened', blockId: road.blockId, by: client.s.teamName })
  } else {
    client.enqueue('reportRoad', { roadKind: 'condition', blockId: road.blockId || null, note: road.note, by: client.s.teamName })
  }
}
function quickRetry(a) {
  // 原样重提：新动作复用同一业务参数（旧冲突项丢弃，避免同 id 永久冲突）；固定回原分支
  client.retryWith(a.clientActionId, {})
}
function moveRetry(a) {
  // 现场确认动作本应在当前视角分支执行：修正改投（新 id、留痕 movedFrom）
  if (a.branchId === client.branchId) return
  client.requeueForBranch(a.clientActionId, client.branchId)
}

const LEVEL = { yellow: '黄色', orange: '橙色', red: '红色' }
const ROLES = { field: '现场队伍', duty: '值班长', dispatch: '调度员', commander: '指挥长', expert: '专家组', shelter: '安置点联络员' }
const levelLabel = (l) => LEVEL[l] || l
const roleLabel = (r) => ROLES[r] || r
const orderStatusLabel = (s) => ({ dispatched: '待接单', accepted: '抢修中', done: '待验收' })[s] || s
function actionLabel(k) {
  return {
    ackWarning: '签收预警', signDispatch: '物资签收', reportRepair: '抢修上报',
    reportPosition: '位置上报', reportRoad: '道路变化', registerTeam: '队伍注册'
  }[k] || k
}
function statusLabel(s) {
  return { queued: '待补传', acked: '已闭环', conflict: '冲突' }[s] || s
}
</script>
