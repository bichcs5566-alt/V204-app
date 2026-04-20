const DATA_BASE = "./data";
const TIER_LABELS = {lt_50:"50以下",p50_100:"50-100",p100_300:"100-300",p300_500:"300-500",p500_1000:"500-1000",gt_1000:"1000以上",unknown:"未知"};
const ACTION_LABELS = {BUY:"買進",SELL:"賣出",HOLD:"續抱",REDUCE:"減碼",ADD:"加碼",STOP_LOSS:"停損",WATCH:"觀察",CANDIDATE:"候選",BUY_READY:"可買",HOLD_MONITOR:"持有監控",NONE:"未進策略",IGNORE:"忽略"};
let tradeRows=[], positionRows=[], watchRows=[], summaryRows=[], debugRows=[];
let localPositions=[], localWatchlist=[]; let lastMetaGeneratedAt = "";

function mapActionLabel(v){const key=String(v||"").trim().toUpperCase(); return ACTION_LABELS[key]||v||"";}
function actionClass(v){return String(v||"").trim().toLowerCase();}
function tierLabel(v){return TIER_LABELS[String(v||"").trim()]||v||"未知";}
function formatNumber(v){const n=Number(v); if(!Number.isFinite(n)) return v??""; return n.toLocaleString("zh-TW",{maximumFractionDigits:2});}
function saveLocal(name,val){localStorage.setItem(name, JSON.stringify(val));}
function loadLocal(name, fallback=[]){try{return JSON.parse(localStorage.getItem(name)||JSON.stringify(fallback));}catch{return fallback;}}
function hasBadEncoding(s){return /Ã|æ|ä|å|ç|é|è|ê|î|ï|ð|þ|œ|�/.test(String(s||""));}
function safeText(v, fallback=""){ const s = String(v ?? "").trim(); if (!s) return fallback; if (hasBadEncoding(s)) return "資料已舊版錯碼，等待新資料覆蓋"; return s; }
function nonEmpty(v, fallback="目前沒有資料"){const s=String(v??"").trim(); return s ? s : fallback;}

function getConfig(){
  const cfg = window.GITHUB_CONFIG || {};
  const local = loadLocal("github_dispatch_config_v2668", {});
  return {
    owner: cfg.owner || local.owner || "",
    repo: cfg.repo || local.repo || "",
    workflow: cfg.workflow || local.workflow || "v266_8_pipeline.yml",
    ref: cfg.ref || local.ref || "main",
    token: cfg.token || local.token || ""
  };
}
function saveConfig(cfg){ saveLocal("github_dispatch_config_v2668", cfg); }
function resetConfig(){
  localStorage.removeItem("github_dispatch_config_v2668");
  setBackendState("已清除 GitHub 設定", "backend-idle");
  alert("已清除目前裝置的 GitHub 設定。下次按「更新資料與策略」會重新要求輸入。");
}
function promptConfigIfNeeded(){
  const cfg = getConfig();
  if (cfg.owner && cfg.repo && cfg.workflow && cfg.ref && cfg.token) return cfg;
  const owner = prompt("請輸入 GitHub owner", cfg.owner || "");
  if (owner === null) return null;
  const repo = prompt("請輸入 repo 名稱", cfg.repo || "");
  if (repo === null) return null;
  const workflow = prompt("請輸入 workflow 檔名", cfg.workflow || "v266_8_pipeline.yml");
  if (workflow === null) return null;
  const ref = prompt("請輸入 branch", cfg.ref || "main");
  if (ref === null) return null;
  const token = prompt("請輸入 GitHub Token（只存這台裝置）", cfg.token || "");
  if (token === null) return null;
  const newCfg = {owner: owner.trim(), repo: repo.trim(), workflow: workflow.trim(), ref: ref.trim(), token: token.trim()};
  saveConfig(newCfg);
  return newCfg;
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
  const tick = ()=>{ el.textContent = new Date().toLocaleString("zh-TW", {hour12:false}); };
  tick(); setInterval(tick, 1000);
}
function setBackendState(text, cls){
  const el = document.getElementById("backendState");
  el.textContent = text;
  el.className = cls || "backend-idle";
}
function updateStatus(meta){
  const generated = String(meta.generated_at||"").trim();
  const signal = String(meta.signal_date||"").trim();
  const trade = String(meta.trade_date||"").trim();
  document.getElementById("lastUpdate").textContent = generated || "--";
  document.getElementById("signalDate").textContent = signal || "--";
  document.getElementById("tradeDate").textContent = trade || "--";
  const el = document.getElementById("dataState");
  const today = new Date().toLocaleDateString("sv-SE");
  let stateText = "✅ 最新資料"; let stateClass = "state-fresh";
  if (!generated || !signal || !trade) { stateText = "⚠️ 缺少資料狀態"; stateClass = "state-old"; }
  else if (trade < today) { stateText = "⚠️ 舊資料"; stateClass = "state-old"; }
  if (meta.data_state === "fail") { stateText = "❌ 讀取失敗"; stateClass = "state-fail"; }
  el.textContent = stateText; el.className = stateClass;
  lastMetaGeneratedAt = generated || lastMetaGeneratedAt;
}

function addTradeToPosition(stockId, refPrice){
  const shares = prompt(`請輸入 ${stockId} 的持有股數`, "1000");
  if (shares === null) return;
  const cost = prompt(`請輸入 ${stockId} 的平均成本`, refPrice || "");
  if (cost === null) return;
  const row = { stock_id:String(stockId), shares:String(shares).trim(), avg_cost:String(cost).trim(), last_action_date:new Date().toISOString().slice(0,10), note:"由今日操作加入持倉" };
  const idx = localPositions.findIndex(x => String(x.stock_id) === String(stockId));
  if (idx >= 0) localPositions[idx] = row; else localPositions.push(row);
  saveLocal("current_positions_v2668", localPositions);
  renderPositionTable();
}
function addPositionManual(){
  const stockId = document.getElementById("posCode").value.trim();
  const shares = document.getElementById("posShares").value.trim();
  const avgCost = document.getElementById("posCost").value.trim();
  if (!stockId || !shares || !avgCost) return;
  const row = {stock_id:stockId, shares, avg_cost:avgCost, last_action_date:new Date().toISOString().slice(0,10), note:"由持倉監控新增"};
  const idx = localPositions.findIndex(x => String(x.stock_id) === stockId);
  if (idx >= 0) localPositions[idx] = row; else localPositions.push(row);
  saveLocal("current_positions_v2668", localPositions);
  document.getElementById("posCode").value=""; document.getElementById("posShares").value=""; document.getElementById("posCost").value="";
  renderPositionTable();
}
function removePosition(stockId){ localPositions = localPositions.filter(x => String(x.stock_id) !== String(stockId)); saveLocal("current_positions_v2668", localPositions); renderPositionTable(); }
function addWatchManual(){ const code = document.getElementById("watchCode").value.trim(); if (!code) return; if (!localWatchlist.includes(code)) localWatchlist.push(code); saveLocal("watchlist_v2668", localWatchlist); document.getElementById("watchCode").value=""; renderWatchTable(); }
function removeWatch(stockId){ localWatchlist = localWatchlist.filter(x => String(x) !== String(stockId)); saveLocal("watchlist_v2668", localWatchlist); renderWatchTable(); }

function renderTradeTable(){
  const tbody=document.querySelector("#trade-table tbody");
  const selectedTier=document.getElementById("tierFilter").value;
  let rows=[...tradeRows];
  if(selectedTier!=="全部") rows=rows.filter(r=>String(r.price_tier||"")===selectedTier);
  tbody.innerHTML="";
  if(!rows.length){tbody.innerHTML='<tr><td colspan="8" class="muted">目前沒有符合條件的資料</td></tr>'; return;}
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
  const tbody=document.querySelector("#position-table tbody"); tbody.innerHTML="";
  const merged=[...positionRows];
  localPositions.forEach(p=>{ if(!merged.find(x=>String(x.stock_id)===String(p.stock_id))){ merged.push({ stock_id:p.stock_id, price_tier:"unknown", ref_price:"", shares:p.shares, avg_cost:p.avg_cost, pnl_pct:"", target_weight:"", current_weight_est:"", action:"HOLD", note:"前端新增，待後端同步" }); }});
  if(!merged.length){tbody.innerHTML='<tr><td colspan="11" class="muted">目前沒有持倉監控資料</td></tr>'; return;}
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
  const tbody=document.querySelector("#watch-table tbody"); tbody.innerHTML="";
  const merged=[...watchRows];
  localWatchlist.forEach(code=>{ if(!merged.find(x=>String(x.stock_id)===String(code))){ merged.push({ stock_id:code, price_tier:"unknown", ref_price:"", holding_status:"未持有", strategy_bucket:"NONE", action:"WATCH", pnl_pct:"" }); }});
  if(!merged.length){tbody.innerHTML='<tr><td colspan="8" class="muted">目前沒有自選股監控資料</td></tr>'; return;}
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
  const tbody=document.querySelector("#summary-table tbody"); tbody.innerHTML="";
  if(!summaryRows.length){tbody.innerHTML='<tr><td colspan="3" class="muted">目前沒有績效摘要資料</td></tr>'; return;}
  summaryRows.forEach(r=>{tbody.innerHTML += `<tr><td>${r.return||"0"}</td><td>${r.mdd||"0"}</td><td>${r.sharpe_daily||"0"}</td></tr>`;});
}
function renderDebugTable(){
  const tbody=document.querySelector("#debug-table tbody"); tbody.innerHTML="";
  if(!debugRows.length){tbody.innerHTML='<tr><td colspan="6" class="muted">目前沒有篩選除錯資料</td></tr>'; return;}
  debugRows.forEach(r=>{tbody.innerHTML += `<tr><td>${r.total_input||"0"}</td><td>${r.valid_after_na||"0"}</td><td>${r.core_primary_count||"0"}</td><td>${r.alpha_primary_count||"0"}</td><td>${r.core_final_count||"0"}</td><td>${r.alpha_final_count||"0"}</td></tr>`;});
}

async function init(){
  const [meta, trade, posMon, watchMon, summary, debug, posCsv, watchCsv] = await Promise.all([
    fetchJSON(`${DATA_BASE}/meta.json`),
    loadCSV("trade_plan.csv"),
    loadCSV("position_monitor.csv", true),
    loadCSV("watchlist_monitor.csv", true),
    loadCSV("full_summary.csv", true),
    loadCSV("selection_debug.csv", true),
    loadCSV("current_positions.csv", true),
    loadCSV("watchlist.csv", true),
  ]);
  tradeRows = trade; positionRows = posMon; watchRows = watchMon; summaryRows = summary; debugRows = debug;
  const lp = loadLocal("current_positions_v2668", []); localPositions = lp.length ? lp : posCsv;
  const lw = loadLocal("watchlist_v2668", []); localWatchlist = lw.length ? lw : watchCsv.map(x => String(x.stock_id || "").trim()).filter(Boolean);
  updateStatus(meta); renderTradeTable(); renderPositionTable(); renderWatchTable(); renderSummaryTable(); renderDebugTable();
}
async function refreshAll(){
  const btn = document.getElementById("refreshBtn"); const original = btn.textContent;
  btn.disabled = true; btn.textContent = "⏳ 更新中...";
  try{ await init(); btn.textContent = "✅ 已更新"; }
  catch(err){ console.error(err); btn.textContent = "❌ 失敗"; setBackendState("頁面刷新失敗", "backend-err"); }
  finally{ setTimeout(()=>{ btn.textContent = original; btn.disabled = false; }, 1200); }
}

async function dispatchBackendUpdate(){
  const btn = document.getElementById("updateBtn");
  const original = btn.textContent;
  const cfg = promptConfigIfNeeded();
  if (!cfg) return;

  btn.disabled = true;
  btn.textContent = "🚀 送出中...";
  setBackendState("送出後端更新中", "backend-running");

  try{
    const url = `https://api.github.com/repos/${cfg.owner}/${cfg.repo}/actions/workflows/${cfg.workflow}/dispatches`;
    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Accept": "application/vnd.github+json",
        "Authorization": `Bearer ${cfg.token}`,
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ ref: cfg.ref })
    });
    if (!res.ok) {
      const txt = await res.text();
      throw new Error(`dispatch 失敗: ${res.status} ${txt}`);
    }
    btn.textContent = "⏳ 後端執行中...";
    setBackendState("已送出，等待資料更新", "backend-running");
  } catch (err) {
    console.error(err);
    btn.textContent = "❌ 送出失敗";
    setBackendState("後端送出失敗", "backend-err");
    alert(`GitHub 送出失敗：\n${err.message}`);
    setTimeout(()=>{ btn.textContent = original; btn.disabled = false; }, 1800);
    return;
  }

  let success = false;
  const before = lastMetaGeneratedAt;
  for (let i=0; i<36; i++) {
    await new Promise(r=>setTimeout(r, 10000));
    try{
      const meta = await fetchJSON(`${DATA_BASE}/meta.json`);
      const generated = String(meta.generated_at||"").trim();
      if (generated && generated !== before) {
        await init();
        success = true;
        break;
      }
    }catch(err){ console.error(err); }
  }

  if (success) { btn.textContent = "✅ 後端已更新"; setBackendState("資料與策略已更新", "backend-ok"); }
  else { btn.textContent = "⚠️ 尚未完成"; setBackendState("已送出，但尚未看到新資料", "backend-running"); }
  setTimeout(()=>{ btn.textContent = original; btn.disabled = false; }, 1800);
}

document.addEventListener("DOMContentLoaded", ()=>{
  startLiveClock();
  const refreshBtn = document.getElementById("refreshBtn"); if (refreshBtn) refreshBtn.addEventListener("click", refreshAll);
  const updateBtn = document.getElementById("updateBtn"); if (updateBtn) updateBtn.addEventListener("click", dispatchBackendUpdate);
  const resetBtn = document.getElementById("resetConfigBtn"); if (resetBtn) resetBtn.addEventListener("click", resetConfig);
  const tierFilter = document.getElementById("tierFilter"); if (tierFilter) tierFilter.addEventListener("change", renderTradeTable);
  const addPosBtn = document.getElementById("addPositionBtn"); if (addPosBtn) addPosBtn.addEventListener("click", addPositionManual);
  const addWatchBtn = document.getElementById("addWatchBtn"); if (addWatchBtn) addWatchBtn.addEventListener("click", addWatchManual);
  setBackendState("待命", "backend-idle");
  refreshAll();
});
