const DATA_BASE = "./data";
const TIER_LABELS = {lt_50:"50以下",p50_100:"50-100",p100_300:"100-300",p300_500:"300-500",p500_1000:"500-1000",gt_1000:"1000以上",unknown:"未知"};
const ACTION_LABELS = {BUY:"買進",SELL:"賣出",HOLD:"續抱",REDUCE:"減碼",ADD:"加碼",STOP_LOSS:"停損",WATCH:"觀察",CANDIDATE:"候選",BUY_READY:"可買",HOLD_MONITOR:"持有監控",NONE:"未進策略",IGNORE:"忽略"};

const FIXED_OWNER = "bichcs5566-alt";
const FIXED_REPO = "V204-app";
const FIXED_MAIN_WORKFLOW = "v266_10_EF_pipeline.yml";
const FIXED_POSITION_WORKFLOW = "v266_9A_position_writeback.yml";
const FIXED_WATCHLIST_WORKFLOW = "v266_10_watchlist_writeback.yml";
const FIXED_REF = "main";

let tradeRows=[], positionMergedRows=[], watchMergedRows=[], summaryRows=[], debugRows=[];
let currentPositionsRows=[], watchSourceRows=[];
let lastMetaGeneratedAt = "";
let currentMeta = {};

function mapActionLabel(v){const key=String(v||"").trim().toUpperCase(); return ACTION_LABELS[key]||v||"";}
function actionClass(v){return String(v||"").trim().toLowerCase();}
function tierLabel(v){return TIER_LABELS[String(v||"").trim()]||v||"未知";}
function formatNumber(v){const n=Number(v); if(!Number.isFinite(n)) return v??""; return n.toLocaleString("zh-TW",{maximumFractionDigits:3});}
function saveLocal(name,val){localStorage.setItem(name, JSON.stringify(val));}
function loadLocal(name, fallback=[]){try{return JSON.parse(localStorage.getItem(name)||JSON.stringify(fallback));}catch{return fallback;}}
function hasBadEncoding(s){return /Ã|æ|ä|å|ç|é|è|ê|î|ï|ð|þ|œ|�/.test(String(s||""));}
function safeText(v, fallback=""){ const s = String(v ?? "").trim(); if (!s) return fallback; if (hasBadEncoding(s)) return "資料已舊版錯碼，等待新資料覆蓋"; return s; }
function nonEmpty(v, fallback="目前沒有資料"){const s=String(v??"").trim(); return s ? s : fallback;}

function getConfig(){
  const cfg = window.GITHUB_CONFIG || {};
  const local = loadLocal("github_dispatch_config_v26610ef", {});
  return { token: cfg.token || local.token || "" };
}
function saveConfig(cfg){ saveLocal("github_dispatch_config_v26610ef", cfg); }

function resetGithubConfig() {
  localStorage.removeItem("github_dispatch_config_v26610ef");
  const token = prompt("請輸入 GitHub Token（只存這台裝置）", "");
  if (!token) return;
  saveConfig({ token: token.trim() });
  setBackendState("GitHub 設定已更新", "backend-ok");
  alert("✅ GitHub 設定完成");
}
function promptConfigIfNeeded(){
  const cfg = getConfig();
  if (cfg.token) return cfg;
  const token = prompt("請輸入 GitHub Token（只存這台裝置）", cfg.token || "");
  if (token === null || !token.trim()) return null;
  const newCfg = { token: token.trim() };
  saveConfig(newCfg);
  return newCfg;
}
async function githubPost(url, bodyObj){
  const cfg = promptConfigIfNeeded();
  if (!cfg) throw new Error("GitHub Token 未完成設定");
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Accept": "application/vnd.github+json",
      "Authorization": `Bearer ${cfg.token}`,
      "X-GitHub-Api-Version": "2022-11-28",
      "Content-Type": "application/json"
    },
    body: JSON.stringify(bodyObj)
  });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`GitHub API 失敗: ${res.status} ${txt}`);
  }
  return true;
}

async function fetchJSON(path){ const res=await fetch(path+"?t="+Date.now(),{cache:"no-store"}); if(!res.ok) throw new Error(path+" 讀取失敗"); return await res.json(); }
async function fetchText(path){ const res=await fetch(path+"?t="+Date.now(),{cache:"no-store"}); if(!res.ok) throw new Error(path+" 讀取失敗"); return await res.text(); }

function parseCSV(text){
  const rows=[]; let row=[]; let cell=""; let inQuotes=false;
  for(let i=0;i<text.length;i++){
    const ch=text[i], next=text[i+1];
    if(ch==='\"'){ if(inQuotes&&next==='\"'){cell+='\"'; i++;} else inQuotes=!inQuotes; }
    else if(ch===','&&!inQuotes){row.push(cell); cell='';}
    else if((ch==='\n'||ch==='\r')&&!inQuotes){ if(ch==='\r'&&next==='\n') i++; row.push(cell); if(row.some(v=>String(v).trim()!=='')) rows.push(row); row=[]; cell='';}
    else cell+=ch;
  }
  if(cell.length>0||row.length>0){row.push(cell); if(row.some(v=>String(v).trim()!=='')) rows.push(row);}
  if(!rows.length) return [];
  const headers=rows[0].map(h=>String(h).trim());
  return rows.slice(1).map(r=>{const obj={}; headers.forEach((h,idx)=>obj[h]=(r[idx]??'').trim()); return obj;});
}
async function loadCSV(file, optional=false){ try{return parseCSV(await fetchText(`${DATA_BASE}/${file}`));} catch(err){ if(optional) return []; throw err; } }

function startLiveClock(){
  const el = document.getElementById("liveClock");
  const tick = ()=>{ if (el) el.textContent = new Date().toLocaleString("zh-TW", {hour12:false}); };
  tick();
  setInterval(tick, 1000);
}
function setBackendState(text, cls){
  const el = document.getElementById("backendState");
  if (!el) return;
  el.textContent = text;
  el.className = cls || "backend-idle";
}
function setWritebackState(text, cls){
  const el = document.getElementById("writebackState");
  if (!el) return;
  el.textContent = text;
  el.className = cls || "backend-idle";
}
function parseDateOnly(s){
  if (!s) return null;
  const d = new Date(`${s}T00:00:00`);
  return Number.isNaN(d.getTime()) ? null : d;
}
function toTaipeiNow(){
  const now = new Date();
  return new Date(now.toLocaleString("en-US", { timeZone: "Asia/Taipei" }));
}
function isWeekend(dateObj){
  const day = dateObj.getDay();
  return day === 0 || day === 6;
}
function updateStatus(meta){
  currentMeta = meta || {};
  const generated = String(meta.generated_at||"").trim();
  const signal = String(meta.signal_date||"").trim();
  const trade = String(meta.trade_date||"").trim();
  const lastEl = document.getElementById("lastUpdate");
  const sigEl = document.getElementById("signalDate");
  const tradeEl = document.getElementById("tradeDate");
  if (lastEl) lastEl.textContent = generated || "--";
  if (sigEl) sigEl.textContent = signal || "--";
  if (tradeEl) tradeEl.textContent = trade || "--";
  const el = document.getElementById("dataState");
  if (!el) return;
  let stateText = "⚠️ 缺少資料狀態";
  let stateClass = "state-old";
  if (meta.data_state === "fail") {
    stateText = "❌ 讀取失敗"; stateClass = "state-fail";
  } else if (generated && trade) {
    const nowTp = toTaipeiNow();
    const todayStr = nowTp.toLocaleDateString("sv-SE");
    const hour = nowTp.getHours();
    const tradeDate = parseDateOnly(trade);
    const todayDate = parseDateOnly(todayStr);
    let isFresh = false;
    if (trade === todayStr) isFresh = true;
    else if (tradeDate && todayDate) {
      const diffDays = Math.round((todayDate - tradeDate) / 86400000);
      if (isWeekend(todayDate)) isFresh = diffDays >= 1 && diffDays <= 3;
      else if (hour < 18) isFresh = diffDays >= 0 && diffDays <= 3;
      else isFresh = diffDays === 0;
    }
    if (isFresh) { stateText = "✅ 最新資料"; stateClass = "state-fresh"; }
    else { stateText = "⚠️ 舊資料"; stateClass = "state-old"; }
  }
  el.textContent = stateText;
  el.className = stateClass;
  lastMetaGeneratedAt = generated || lastMetaGeneratedAt;
}
function todayTradeRows(rows){
  const today = toTaipeiNow().toLocaleDateString("sv-SE");
  const tradeDate = String(currentMeta.trade_date || "").trim();
  if (!tradeDate) return rows || [];
  if (tradeDate === today) return rows || [];
  return rows || [];
}
async function pollForMetaChange(before, maxLoops=48, waitMs=10000){
  for (let i=0; i<maxLoops; i++) {
    await new Promise(r=>setTimeout(r, waitMs));
    try{
      const meta = await fetchJSON(`${DATA_BASE}/meta.json`);
      const generated = String(meta.generated_at||"").trim();
      if (generated && generated !== before) {
        await init();
        return true;
      }
    } catch(err) { console.error(err); }
  }
  return false;
}
async function dispatchWorkflowByFile(workflowFile, inputs=null){
  const url = `https://api.github.com/repos/${FIXED_OWNER}/${FIXED_REPO}/actions/workflows/${workflowFile}/dispatches`;
  const body = { ref: FIXED_REF };
  if (inputs) body.inputs = inputs;
  await githubPost(url, body);
}
async function dispatchMainPipeline(){
  await dispatchWorkflowByFile(FIXED_MAIN_WORKFLOW);
}
async function dispatchPositionWriteback(actionType, stockId, shares="", avgCost=""){
  await dispatchWorkflowByFile(FIXED_POSITION_WORKFLOW, {
    action_type: String(actionType),
    stock_id: String(stockId),
    shares: String(shares),
    avg_cost: String(avgCost)
  });
}
async function dispatchWatchlistWriteback(actionType, stockId){
  await dispatchWorkflowByFile(FIXED_WATCHLIST_WORKFLOW, {
    action_type: String(actionType),
    stock_id: String(stockId)
  });
}
async function addTradeToPosition(stockId, refPrice){
  const shares = prompt(`請輸入 ${stockId} 的持有股數`, "1000");
  if (shares === null || !String(shares).trim()) return;
  const cost = prompt(`請輸入 ${stockId} 的平均成本`, refPrice || "");
  if (cost === null || !String(cost).trim()) return;

  setWritebackState("送出持倉寫回中", "backend-running");
  try {
    const before = lastMetaGeneratedAt;
    await dispatchPositionWriteback("upsert", stockId, shares, cost);
    setWritebackState("已寫回，等待策略重算", "backend-running");
    const ok = await pollForMetaChange(before);
    if (ok) setWritebackState("持倉已真寫回", "backend-ok");
    else setWritebackState("寫回已送出，尚未看到新資料", "backend-running");
  } catch(err) {
    console.error(err);
    setWritebackState("持倉寫回失敗", "backend-err");
    alert(`持倉真回寫失敗：\n${err.message}`);
  }
}
async function addPositionManual(){
  const stockId = document.getElementById("posCode")?.value.trim();
  const shares = document.getElementById("posShares")?.value.trim();
  const avgCost = document.getElementById("posCost")?.value.trim();
  if (!stockId || !shares || !avgCost) return;

  setWritebackState("送出持倉寫回中", "backend-running");
  try {
    const before = lastMetaGeneratedAt;
    await dispatchPositionWriteback("upsert", stockId, shares, avgCost);
    document.getElementById("posCode").value=""; document.getElementById("posShares").value=""; document.getElementById("posCost").value="";
    setWritebackState("已寫回，等待策略重算", "backend-running");
    const ok = await pollForMetaChange(before);
    if (ok) setWritebackState("持倉已真寫回", "backend-ok");
    else setWritebackState("寫回已送出，尚未看到新資料", "backend-running");
  } catch(err) {
    console.error(err);
    setWritebackState("持倉寫回失敗", "backend-err");
    alert(`持倉真回寫失敗：\n${err.message}`);
  }
}
async function removePosition(stockId){
  if (!confirm(`確定要移除持倉 ${stockId} 嗎？`)) return;
  setWritebackState("送出持倉刪除中", "backend-running");
  try {
    const before = lastMetaGeneratedAt;
    currentPositionsRows = currentPositionsRows.filter(r => String(r.stock_id) !== String(stockId));
    renderPositionTable();
    await dispatchPositionWriteback("delete", stockId, "", "");
    setWritebackState("已寫回，等待策略重算", "backend-running");
    const ok = await pollForMetaChange(before);
    if (ok) setWritebackState("持倉已真移除", "backend-ok");
    else setWritebackState("刪除已送出，尚未看到新資料", "backend-running");
  } catch(err) {
    console.error(err);
    setWritebackState("持倉刪除失敗", "backend-err");
    alert(`持倉刪除失敗：\n${err.message}`);
  }
}
async function addWatchManual(){
  const code = document.getElementById("watchCode")?.value.trim();
  if (!code) return;
  if (!watchSourceRows.find(r => String(r.stock_id) === String(code))) {
    watchSourceRows.push({stock_id: code, note: ""});
  }
  renderWatchTable();
  document.getElementById("watchCode").value="";
  try{
    const before = lastMetaGeneratedAt;
    setWritebackState("送出自選股寫回中", "backend-running");
    await dispatchWatchlistWriteback("upsert", code);
    const ok = await pollForMetaChange(before);
    if (ok) setWritebackState("自選股已真寫回", "backend-ok");
    else setWritebackState("自選股已送出，尚未看到新資料", "backend-running");
  }catch(err){
    console.error(err);
    setWritebackState("自選股寫回失敗", "backend-err");
    alert(`自選股寫回失敗：\n${err.message}`);
  }
}
async function removeWatch(stockId){
  if (!confirm(`確定要移除自選股 ${stockId} 嗎？`)) return;
  watchSourceRows = watchSourceRows.filter(x => String(x.stock_id) !== String(stockId));
  renderWatchTable();
  try{
    const before = lastMetaGeneratedAt;
    setWritebackState("送出自選股刪除中", "backend-running");
    await dispatchWatchlistWriteback("delete", stockId);
    const ok = await pollForMetaChange(before);
    if (ok) setWritebackState("自選股已真移除", "backend-ok");
    else setWritebackState("自選股刪除已送出，尚未看到新資料", "backend-running");
  }catch(err){
    console.error(err);
    setWritebackState("自選股刪除失敗", "backend-err");
    alert(`自選股刪除失敗：\n${err.message}`);
  }
}
function renderTradeTable(){
  const tbody=document.querySelector("#trade-table tbody");
  if (!tbody) return;
  const selectedTier=document.getElementById("tierFilter")?.value || "全部";
  let rows=[...todayTradeRows(tradeRows)];
  if(selectedTier!=="全部") rows=rows.filter(r=>String(r.price_tier||"")===selectedTier);
  tbody.innerHTML="";
  if(!rows.length){tbody.innerHTML='<tr><td colspan="8" class="muted">目前沒有可執行名單</td></tr>'; return;}
  rows.forEach(r=>{
    const stockId = nonEmpty(r.stock_id, "-");
    tbody.innerHTML += `<tr>
      <td class="${actionClass(r.action)}">${mapActionLabel(r.action)}</td><td>${stockId}</td><td>${tierLabel(r.price_tier)}</td>
      <td>${formatNumber(r.ref_price)}</td><td>${r.target_weight||""}</td><td>${formatNumber(r.suggested_amount)}</td>
      <td>${safeText(r.note, "目前沒有備註")}</td>
      <td><button class="sync-btn" data-stock="${stockId}" data-price="${r.ref_price || ''}" type="button">加入持倉</button></td>
    </tr>`;
  });
  tbody.querySelectorAll(".sync-btn").forEach(btn=>btn.addEventListener("click",()=>addTradeToPosition(btn.dataset.stock, btn.dataset.price)));
}
function renderPositionTable(){
  const tbody=document.querySelector("#position-table tbody");
  if (!tbody) return;
  tbody.innerHTML="";
  if(!currentPositionsRows.length){tbody.innerHTML='<tr><td colspan="11" class="muted">目前沒有持倉監控資料</td></tr>'; return;}
  const enrichMap = new Map(positionMergedRows.map(r => [String(r.stock_id), r]));
  const merged = currentPositionsRows.map(cp=>{
    const pr = enrichMap.get(String(cp.stock_id)) || {};
    return {
      stock_id: cp.stock_id,
      price_tier: pr.price_tier || "unknown",
      ref_price: pr.ref_price || "",
      shares: cp.shares || "",
      avg_cost: cp.avg_cost || "",
      pnl_pct: pr.pnl_pct || "",
      target_weight: pr.target_weight || "",
      current_weight_est: pr.current_weight_est || "",
      action: pr.action || "HOLD",
      note: safeText(pr.note, cp.note || "目前沒有備註")
    };
  });
  merged.forEach(r=>{
    const stockId = nonEmpty(r.stock_id, "-");
    const waiting = String(r.ref_price??"").trim() === "" || Number(r.ref_price) === 0;
    const tier = waiting ? "等待新資料" : tierLabel(r.price_tier);
    const ref = waiting ? "等待新資料" : formatNumber(r.ref_price);
    tbody.innerHTML += `<tr>
      <td>${stockId}</td><td>${tier}</td><td>${ref}</td><td>${formatNumber(r.shares)}</td><td>${formatNumber(r.avg_cost)}</td>
      <td>${r.pnl_pct || "目前沒有資料"}</td><td>${r.target_weight || "目前沒有資料"}</td><td>${r.current_weight_est || "目前沒有資料"}</td>
      <td class="${actionClass(r.action)}">${mapActionLabel(r.action)}</td><td>${safeText(r.note, "目前沒有備註")}</td>
      <td><button class="remove-btn" data-stock="${stockId}" type="button">移除</button></td>
    </tr>`;
  });
  tbody.querySelectorAll(".remove-btn").forEach(btn=>btn.addEventListener("click",()=>removePosition(btn.dataset.stock)));
}
function renderWatchTable(){
  const tbody=document.querySelector("#watch-table tbody");
  if (!tbody) return;
  tbody.innerHTML="";
  if(!watchSourceRows.length){tbody.innerHTML='<tr><td colspan="8" class="muted">目前沒有自選股監控資料</td></tr>'; return;}
  const enrichMap = new Map(watchMergedRows.map(r => [String(r.stock_id), r]));
  const merged = watchSourceRows.map(ws=>{
    const mr = enrichMap.get(String(ws.stock_id)) || {};
    return {
      stock_id: ws.stock_id,
      price_tier: mr.price_tier || "unknown",
      ref_price: mr.ref_price || "",
      holding_status: mr.holding_status || "未持有",
      strategy_bucket: mr.strategy_bucket || "NONE",
      action: mr.action || "WATCH",
      pnl_pct: mr.pnl_pct || ""
    };
  });
  merged.forEach(r=>{
    const stockId = nonEmpty(r.stock_id, "-");
    const waiting = String(r.ref_price??"").trim() === "" || Number(r.ref_price) === 0;
    const ref = waiting ? "等待新資料" : formatNumber(r.ref_price);
    const tier = waiting ? "等待新資料" : tierLabel(r.price_tier);
    tbody.innerHTML += `<tr>
      <td>${stockId}</td><td>${tier}</td><td>${ref}</td><td>${safeText(r.holding_status, "目前沒有資料")}</td>
      <td>${mapActionLabel(r.strategy_bucket)}</td><td class="${actionClass(r.action)}">${mapActionLabel(r.action)}</td><td>${r.pnl_pct || "目前沒有資料"}</td>
      <td><button class="remove-btn" data-stock="${stockId}" type="button">移除</button></td>
    </tr>`;
  });
  tbody.querySelectorAll(".remove-btn").forEach(btn=>btn.addEventListener("click",()=>removeWatch(btn.dataset.stock)));
}
function renderSummaryTable(){
  const tbody=document.querySelector("#summary-table tbody");
  if (!tbody) return;
  tbody.innerHTML="";
  if(!summaryRows.length){tbody.innerHTML='<tr><td colspan="3" class="muted">目前沒有績效摘要資料</td></tr>'; return;}
  summaryRows.forEach(r=>{tbody.innerHTML += `<tr><td>${r.return||"0"}</td><td>${r.mdd||"0"}</td><td>${r.sharpe_daily||"0"}</td></tr>`;});
}
function renderDebugTable(){
  const tbody=document.querySelector("#debug-table tbody");
  if (!tbody) return;
  tbody.innerHTML="";
  if(!debugRows.length){tbody.innerHTML='<tr><td colspan="6" class="muted">目前沒有篩選除錯資料</td></tr>'; return;}
  debugRows.forEach(r=>{tbody.innerHTML += `<tr><td>${r.total_input||"0"}</td><td>${r.valid_after_na||"0"}</td><td>${r.core_primary_count||"0"}</td><td>${r.alpha_primary_count||"0"}</td><td>${r.core_final_count||"0"}</td><td>${r.alpha_final_count||"0"}</td></tr>`;});
}
async function init(){
  const [meta, trade, posMerged, watchMerged, summary, debug, currentPos, watchSource] = await Promise.all([
    fetchJSON(`${DATA_BASE}/meta.json`),
    loadCSV("trade_plan.csv", true),
    loadCSV("position_monitor_merged.csv", true),
    loadCSV("watchlist_monitor_merged.csv", true),
    loadCSV("full_summary.csv", true),
    loadCSV("selection_debug.csv", true),
    loadCSV("current_positions.csv", true),
    loadCSV("watchlist.csv", true)
  ]);
  currentMeta = meta || {};
  tradeRows = trade;
  positionMergedRows = posMerged;
  watchMergedRows = watchMerged;
  summaryRows = summary;
  debugRows = debug;
  currentPositionsRows = currentPos;
  watchSourceRows = watchSource;
  updateStatus(meta); renderTradeTable(); renderPositionTable(); renderWatchTable(); renderSummaryTable(); renderDebugTable();
}
async function refreshAll(){
  const btn = document.getElementById("refreshBtn");
  if (!btn) return;
  const original = btn.textContent;
  btn.disabled = true; btn.textContent = "⏳ 更新中...";
  try{ await init(); btn.textContent = "✅ 已更新"; }
  catch(err){ console.error(err); btn.textContent = "❌ 失敗"; setBackendState("頁面刷新失敗", "backend-err"); }
  finally{ setTimeout(()=>{ btn.textContent = original; btn.disabled = false; }, 1200); }
}
async function dispatchBackendUpdate(){
  const btn = document.getElementById("updateBtn");
  if (!btn) return;
  const original = btn.textContent;
  try{
    btn.disabled = true;
    btn.textContent = "🚀 送出中...";
    setBackendState(`送出後端更新中（${FIXED_REPO}/${FIXED_MAIN_WORKFLOW}）`, "backend-running");
    const before = lastMetaGeneratedAt;
    await dispatchMainPipeline();
    btn.textContent = "⏳ 後端執行中...";
    setBackendState(`已送出，等待資料更新（${FIXED_REPO}/${FIXED_MAIN_WORKFLOW}）`, "backend-running");
    const ok = await pollForMetaChange(before);
    if (ok) { btn.textContent = "✅ 後端已更新"; setBackendState("資料與策略已更新", "backend-ok"); }
    else { btn.textContent = "⚠️ 尚未完成"; setBackendState("已送出，但尚未看到新資料", "backend-running"); }
  } catch (err) {
    console.error(err);
    btn.textContent = "❌ 送出失敗";
    setBackendState(`後端送出失敗（${FIXED_REPO}/${FIXED_MAIN_WORKFLOW}）`, "backend-err");
    alert(`GitHub 送出失敗：\n${err.message}`);
  } finally {
    setTimeout(()=>{ btn.textContent = original; btn.disabled = false; }, 1800);
  }
}
document.addEventListener("DOMContentLoaded", ()=>{
  startLiveClock();
  const refreshBtn = document.getElementById("refreshBtn"); if (refreshBtn) refreshBtn.addEventListener("click", refreshAll);
  const updateBtn = document.getElementById("updateBtn"); if (updateBtn) updateBtn.addEventListener("click", dispatchBackendUpdate);
  const resetBtn = document.getElementById("resetConfigBtn"); if (resetBtn) resetBtn.addEventListener("click", resetGithubConfig);
  const tierFilter = document.getElementById("tierFilter"); if (tierFilter) tierFilter.addEventListener("change", renderTradeTable);
  const addPosBtn = document.getElementById("addPositionBtn"); if (addPosBtn) addPosBtn.addEventListener("click", addPositionManual);
  const addWatchBtn = document.getElementById("addWatchBtn"); if (addWatchBtn) addWatchBtn.addEventListener("click", addWatchManual);
  setBackendState("待命", "backend-idle");
  setWritebackState("待命", "backend-idle");
  refreshAll();
});
