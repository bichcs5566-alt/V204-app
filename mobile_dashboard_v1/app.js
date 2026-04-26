const DATA_DIR="./data";const GH_CONFIG_KEY="v3_github_config";let POSITION_CACHE=[];

document.addEventListener("DOMContentLoaded",async()=>{injectV381Style();bindUI();loadSavedConfig();await loadAll();});

function bindUI(){
  bind("refreshBtn","click",async()=>{setBanner("頁面重新同步中…","#2f7d32");await loadAll(true);});
  bind("updateBtn","click",async()=>{await dispatchWorkflow("v2_8_auto_update.yml",{},"已送出更新資料與策略，請等待 Actions 跑完後重新整理。");});
  bind("saveConfigBtn","click",saveConfig);
  bind("clearConfigBtn","click",clearConfig);
  bind("addPositionBtn","click",submitAddPositionInstant);
  document.addEventListener("click",async e=>{const b=e.target.closest("[data-remove-position]");if(b)await submitRemovePositionInstant(b.getAttribute("data-remove-position"));});
}

function bind(id,event,handler){const el=document.getElementById(id);if(!el)return;el.addEventListener(event,async e=>{try{await handler(e)}catch(err){console.error(err);setBanner(`操作失敗：${err.message}`,"#b42318")}})}


function injectV381Style(){
  if(document.getElementById("v381-style"))return;
  const style=document.createElement("style");
  style.id="v381-style";
  style.textContent=`
    .decision-hint{display:inline-block;padding:6px 10px;border-radius:999px;font-size:13px;font-weight:800;white-space:nowrap;background:#f2f4f7;color:#344054}
    .decision-strong{background:#fff1f0;color:#b42318}
    .decision-warn{background:#fff8e6;color:#b54708}
    .decision-danger{background:#fdecec;color:#b42318}
    .decision-sync{background:#eef3ff;color:#25406f}
  `;
  document.head.appendChild(style);
}


function loadSavedConfig(){try{const raw=localStorage.getItem(GH_CONFIG_KEY);if(!raw){text("configStatus","未儲存");val("ghOwner","bichcs5566-alt");val("ghBranch","main");return}const cfg=JSON.parse(raw);val("ghOwner",cfg.owner||"bichcs5566-alt");val("ghRepo",cfg.repo||"");val("ghBranch",cfg.branch||"main");val("ghToken",cfg.token||"");text("configStatus","✅ 已儲存本機設定")}catch{text("configStatus","讀取失敗")}}

function saveConfig(){const cfg={owner:val("ghOwner").trim(),repo:val("ghRepo").trim(),branch:val("ghBranch").trim()||"main",token:val("ghToken").trim()};if(!cfg.owner||!cfg.repo||!cfg.branch||!cfg.token){setBanner("GitHub 設定不可空白","#b42318");text("configStatus","欄位不完整");return}localStorage.setItem(GH_CONFIG_KEY,JSON.stringify(cfg));text("configStatus","✅ 已儲存本機設定");setBanner("GitHub 本機設定已儲存","#2f7d32")}

function clearConfig(){localStorage.removeItem(GH_CONFIG_KEY);["ghOwner","ghRepo","ghBranch","ghToken"].forEach(id=>val(id,""));text("configStatus","已清除");setBanner("GitHub 本機設定已清除","#92400e")}

function getGithubConfig(){const raw=localStorage.getItem(GH_CONFIG_KEY);if(!raw){setBanner("請先在 GitHub 本機設定區儲存 owner / repo / branch / token","#b42318");text("configStatus","未儲存");return null}try{return JSON.parse(raw)}catch{setBanner("GitHub 本機設定格式錯誤，請重新儲存","#b42318");text("configStatus","格式錯誤");return null}}

async function dispatchWorkflow(workflowId,inputs={},successMessage="已送出"){const cfg=getGithubConfig();if(!cfg)return false;const url=`https://api.github.com/repos/${cfg.owner}/${cfg.repo}/actions/workflows/${workflowId}/dispatches`;const res=await fetch(url,{method:"POST",headers:{"Accept":"application/vnd.github+json","Authorization":`Bearer ${cfg.token}`,"Content-Type":"application/json","X-GitHub-Api-Version":"2022-11-28"},body:JSON.stringify({ref:cfg.branch,inputs})});if(!res.ok){const txt=await res.text();console.error(txt);setBanner(`同步送出失敗：${res.status}，畫面已回復`, "#b42318");return false}setBanner(successMessage,"#2f7d32");return true}

/* v3.5 即時持倉：先改畫面，再背景同步 GitHub */
async function submitAddPositionInstant(){
  const stockId=val("positionStockInput").trim();
  const shares=val("positionSharesInput").trim();
  const avgCost=val("positionCostInput").trim();

  if(!stockId||!shares||!avgCost){
    setBanner("加入持倉前請填完整：股票代號 / 股數 / 成本","#b42318");
    return;
  }

  const oldCache=deepClone(POSITION_CACHE);
  const row={
    stock_id:stockId,
    price_tier:priceTierFromPrice(avgCost),
    ref_price:"同步中",
    shares:shares,
    avg_cost:avgCost,
    pnl_pct:"",
    target_weight:"",
    action:"SYNCING",
    note:"正在寫回 GitHub..."
  };

  POSITION_CACHE=upsertByStockId(POSITION_CACHE,row);
  renderPosition(POSITION_CACHE);
  renderTierSummary(POSITION_CACHE);
  renderActionSummary([],POSITION_CACHE);
  setBanner(`已先加入畫面：${stockId}，背景同步中...`,"#2f7d32");

  val("positionStockInput","");
  val("positionSharesInput","");
  val("positionCostInput","");

  const ok=await dispatchWorkflow("v3_position_writeback.yml",{
    action:"add",
    stock_id:stockId,
    shares:shares,
    avg_cost:avgCost
  },`✅ 已送出持倉新增 / 更新：${stockId}。Actions 跑完後重新整理即可確認正式資料。`);

  if(!ok){
    POSITION_CACHE=oldCache;
    renderPosition(POSITION_CACHE);
    renderTierSummary(POSITION_CACHE);
    renderActionSummary([],POSITION_CACHE);
  }
}

async function submitRemovePositionInstant(stockId){
  stockId=String(stockId||"").trim();
  if(!stockId){setBanner("找不到要移除的股票代號","#b42318");return}
  if(!confirm(`確定要移除持倉 ${stockId} 嗎？`))return;

  const oldCache=deepClone(POSITION_CACHE);
  POSITION_CACHE=POSITION_CACHE.filter(r=>String(r.stock_id).trim()!==stockId);
  renderPosition(POSITION_CACHE);
  renderTierSummary(POSITION_CACHE);
  renderActionSummary([],POSITION_CACHE);
  setBanner(`已先從畫面移除：${stockId}，背景同步中...`,"#2f7d32");

  const ok=await dispatchWorkflow("v3_position_writeback.yml",{
    action:"remove",
    stock_id:stockId,
    shares:"",
    avg_cost:""
  },`✅ 已送出持倉移除：${stockId}。Actions 跑完後重新整理即可確認正式資料。`);

  if(!ok){
    POSITION_CACHE=oldCache;
    renderPosition(POSITION_CACHE);
    renderTierSummary(POSITION_CACHE);
    renderActionSummary([],POSITION_CACHE);
  }
}

async function loadAll(force=false){
  try{
    const[meta,tradePlanRaw,positionRaw,summary,debug,chipRows]=await Promise.all([
      fetchJSON(`${DATA_DIR}/meta.json`,force),
      fetchCSV(`${DATA_DIR}/trade_plan.csv`,force),
      fetchCSV(`${DATA_DIR}/position_monitor.csv`,force),
      fetchCSV(`${DATA_DIR}/full_summary.csv`,force),
      fetchCSV(`${DATA_DIR}/selection_debug.csv`,force),
      fetchCSV(`${DATA_DIR}/chip_light.csv`,force).catch(()=>[])
    ]);

    const chipMap=buildChipMap(chipRows);
    const tradePlan=mergeChipIntoRows(tradePlanRaw,chipMap);
    const position=mergeChipIntoRows(positionRaw,chipMap);

    POSITION_CACHE=position;
    renderMeta(meta);renderTradePlan(tradePlan);renderPosition(position);renderSummary(summary);renderDebug(debug);renderTierSummary(position);renderActionSummary(tradePlan,position);
    if(!document.getElementById("syncBanner").textContent.includes("已送出"))setBanner("頁面資料已同步","#2f7d32");
  }catch(err){console.error(err);setBanner(`讀取失敗：${err.message}`,"#b42318")}
}

async function fetchJSON(url,force=false){const finalUrl=force?`${url}?t=${Date.now()}`:url;const res=await fetch(finalUrl,{cache:"no-store"});if(!res.ok)throw new Error(`JSON 讀取失敗：${url}`);return await res.json()}
async function fetchCSV(url,force=false){const finalUrl=force?`${url}?t=${Date.now()}`:url;const res=await fetch(finalUrl,{cache:"no-store"});if(!res.ok)throw new Error(`CSV 讀取失敗：${url}`);return parseCSV(await res.text())}

function parseCSV(text){const cleaned=text.replace(/^\uFEFF/,"").trim();if(!cleaned)return[];const lines=cleaned.split(/\r?\n/);const headers=splitCSVLine(lines[0]).map(h=>h.trim());return lines.slice(1).filter(Boolean).map(line=>{const values=splitCSVLine(line),row={};headers.forEach((h,i)=>row[h]=(values[i]??"").trim());return row})}
function splitCSVLine(line){const result=[];let current="",inQuotes=false;for(let i=0;i<line.length;i++){const ch=line[i];if(ch==='"'){if(inQuotes&&line[i+1]==='"'){current+='"';i++}else inQuotes=!inQuotes}else if(ch===","&&!inQuotes){result.push(current);current=""}else current+=ch}result.push(current);return result}

function renderMeta(meta){text("nowTime",meta.now_time||meta.generated_at||"--");text("generatedAt",meta.generated_at||"--");text("signalDate",meta.signal_date||"--");text("tradeDate",meta.trade_date||"--");text("panelDate",meta.price_panel_latest_date||"--");text("dataState",prettyDataState(meta.data_state));text("sourceName",meta.source||"--");text("writebackState",prettyWriteback(meta.position_writeback_state))}
function renderTradePlan(rows){const body=document.getElementById("tradePlanBody");if(!rows.length){body.innerHTML=`<tr><td colspan="8" class="empty">目前沒有資料</td></tr>`;return}body.innerHTML=rows.map(r=>`<tr><td>${badgeForAction(r.action)}</td><td>${safe(r.stock_id)}</td><td>${prettyTier(r.price_tier)}</td><td>${safe(r.ref_price)}</td><td>${safe(r.target_weight)}</td><td>${safeMoney(r.suggested_amount)}</td><td>${decisionHintBadge(getDecisionHint(r.action,r.chip_label))}</td><td>${safe(r.note)}</td></tr>`).join("")}

function renderPosition(rows){
  const body=document.getElementById("positionBody");
  if(!rows.length){body.innerHTML=`<tr><td colspan="11" class="empty">目前沒有持倉資料</td></tr>`;return}
  body.innerHTML=rows.map(r=>`<tr><td>${safe(r.stock_id)}</td><td>${prettyTier(r.price_tier)}</td><td>${safe(r.ref_price)}</td><td>${safeInt(r.shares)}</td><td>${safe(r.avg_cost)}</td><td>${safePct(r.pnl_pct)}</td><td>${safe(r.target_weight)}</td><td>${badgeForAction(r.action)}</td><td>${decisionHintBadge(getDecisionHint(r.action,r.chip_label))}</td><td>${safe(r.note)}</td><td><button class="btn-remove" data-remove-position="${safe(r.stock_id)}">移除</button></td></tr>`).join("");
}

function renderSummary(rows){const row=rows[0]||{};text("returnVal",pctDisplay(row["return"]));text("mddVal",pctDisplay(row["mdd"]));text("sharpeVal",blankDash(row["sharpe_daily"]))}
function renderDebug(rows){const row=rows[0]||{};text("dbgTotal",blankDash(row.total_input));text("dbgValid",blankDash(row.valid_after_na));text("dbgCorePrimary",blankDash(row.core_primary_count));text("dbgAlphaPrimary",blankDash(row.alpha_primary_count));text("dbgCoreFinal",blankDash(row.core_final_count));text("dbgAlphaFinal",blankDash(row.alpha_final_count))}

function renderTierSummary(positionRows){
  const tiers={};positionRows.forEach(r=>{const key=prettyTier(r.price_tier||priceTierFromPrice(r.avg_cost)||"unknown");tiers[key]=(tiers[key]||0)+1});
  const container=document.getElementById("tierSummary"),entries=Object.entries(tiers);
  if(!entries.length){container.innerHTML=`<div class="tier-box"><div class="tier-label">分層狀態</div><div class="tier-value">--</div><div class="tier-sub">目前沒有持倉可展示</div></div>`;return}
  container.innerHTML=entries.map(([k,v])=>`<div class="tier-box"><div class="tier-label">${k}</div><div class="tier-value">${v}</div><div class="tier-sub">此分層目前有 ${v} 檔持倉</div></div>`).join("");
}

function renderActionSummary(tradeRows,positionRows){
  const buys=tradeRows.filter(r=>(r.action||"").toUpperCase()==="BUY");
  const positionActions=positionRows.filter(r=>["ADD","REDUCE","SELL","STOP_LOSS","SYNCING"].includes((r.action||"").toUpperCase()));
  let headline="觀察",desc="今天沒有新的買進動作";
  if(buys.length>0){headline="偏多";desc=`今天有 ${buys.length} 檔新進場候選`}
  else if(positionActions.length>0){headline="調整";desc=`今天有 ${positionActions.length} 筆持倉調整或同步`}
  const totalBuyAmount=buys.reduce((acc,r)=>acc+toNum(r.suggested_amount),0);
  const stopLossCount=positionRows.filter(r=>(r.action||"").toUpperCase()==="STOP_LOSS").length;
  text("headlineAction",headline);text("headlineDesc",desc);text("buyCount",String(buys.length));text("buyAmount",`建議金額：${moneyDisplay(totalBuyAmount)}`);
  text("positionActionCount",String(positionActions.length));text("positionActionDesc",positionActions.length?"請優先查看持倉監控區":"目前無加減碼");
  text("riskLevel",stopLossCount>0?"偏高":buys.length>8?"中等":"正常");text("riskDesc",stopLossCount>0?`有 ${stopLossCount} 筆停損訊號`:"目前沒有明顯停損警示");
}


// ================================
// v3.8.1 Decision Hint PRO
// 只補解讀，不改策略、不改寫回、不改資料源
// ================================

function buildChipMap(chipRows){
  const map={};
  (chipRows||[]).forEach(r=>{
    const sid=normalizeStockId(r.stock_id||r.stock||r.symbol||r.code);
    if(!sid)return;
    map[sid]={
      chip_score:r.chip_score||"",
      chip_label:r.chip_label||"",
      chip_tags:r.chip_tags||"",
      chip_note:r.chip_note||""
    };
  });
  return map;
}

function mergeChipIntoRows(rows,chipMap){
  return (rows||[]).map(r=>{
    const sid=normalizeStockId(r.stock_id||r.stock||r.symbol||r.code);
    const chip=chipMap[sid]||{};
    return {...r,...chip};
  });
}

function normalizeStockId(v){
  return String(v??"").replace(/^\uFEFF/,"").replace(/[^\dA-Za-z]/g,"").trim();
}

function getDecisionHint(actionRaw,chipLabelRaw){
  const action=String(actionRaw||"").toUpperCase();
  const chip=String(chipLabelRaw||"");

  const isStrong=chip.includes("強")||chip.includes("偏強")||chip.includes("加分");
  const isWarn=chip.includes("注意")||chip.includes("⚠️");
  const isNormal=chip.includes("普通")||!chip;

  if(isStrong){
    if(action==="BUY")return"🔥 主升段初期 → 可進場";
    if(action==="HOLD")return"🚀 主升段中 → 持有";
    if(action==="ADD")return"➕ 強勢加碼區";
    if(action==="REDUCE")return"⚠️ 主升段尾 → 建議減碼";
    if(action==="SELL")return"⚠️ 強勢轉折 → 照紀律賣出";
    if(action==="STOP_LOSS")return"❗ 強轉弱 → 出場";
    if(action==="SYNCING")return"⏳ 寫回同步中";
  }

  if(isWarn){
    if(action==="BUY")return"⚠️ 訊號有疑慮 → 小倉或略過";
    if(action==="HOLD")return"⚠️ 觀察風險";
    if(action==="ADD")return"⚠️ 不建議追高加碼";
    if(action==="REDUCE")return"❗ 風險升高 → 減碼";
    if(action==="SELL")return"❗ 風險出場";
    if(action==="STOP_LOSS")return"❗ 停損優先";
  }

  if(isNormal){
    if(action==="BUY")return"👉 可進場，但倉位保守";
    if(action==="HOLD")return"👉 持有觀察";
    if(action==="ADD")return"👉 可加碼但勿重倉";
    if(action==="REDUCE")return"👉 減碼控風險";
    if(action==="SELL")return"👉 賣出";
    if(action==="STOP_LOSS")return"👉 停損";
    if(action==="SYNCING")return"⏳ 寫回同步中";
  }

  return"—";
}

function decisionHintBadge(hint){
  const h=safe(hint);
  let cls="decision-neutral";
  if(h.includes("🔥")||h.includes("🚀")||h.includes("➕"))cls="decision-strong";
  else if(h.includes("⚠️"))cls="decision-warn";
  else if(h.includes("❗"))cls="decision-danger";
  else if(h.includes("⏳"))cls="decision-sync";
  return`<span class="decision-hint ${cls}">${h}</span>`;
}

function badgeForAction(actionRaw){
  const action=(actionRaw||"").toUpperCase();
  const map={BUY:["badge-buy","買進"],HOLD:["badge-hold","持有"],SELL:["badge-sell","賣出"],ADD:["badge-add","加碼"],REDUCE:["badge-reduce","減碼"],STOP_LOSS:["badge-stop","停損"],SYNCING:["badge-watch","同步中"]};
  const pair=map[action]||["badge-hold",safe(actionRaw)];
  return`<span class="badge ${pair[0]}">${pair[1]}</span>`;
}

function upsertByStockId(rows,row){const sid=String(row.stock_id).trim();const out=rows.filter(r=>String(r.stock_id).trim()!==sid);out.push(row);return out}
function deepClone(obj){return JSON.parse(JSON.stringify(obj||[]))}
function priceTierFromPrice(v){const n=toNum(v);if(Number.isNaN(n))return"unknown";if(n<50)return"lt_50";if(n<100)return"p50_100";if(n<300)return"p100_300";if(n<500)return"p300_500";if(n<1000)return"p500_1000";return"gt_1000"}

function prettyTier(tier){const map={lt_50:"50以下",p50_100:"50-100",p100_300:"100-300",p300_500:"300-500",p500_1000:"500-1000",gt_1000:"1000以上",unknown:"未分類"};return map[tier]||tier||"未分類"}
function prettyDataState(state){const map={fresh:"✅ 最新資料",ok:"✅ 正常",stale:"⚠️ 舊資料",loading:"⌛ 讀取中",idle:"待命"};return map[state]||state||"--"}
function prettyWriteback(state){const map={idle:"待命",submitted:"已送出",syncing:"同步中",success:"已完成",failed:"失敗"};return map[state]||state||"--"}
function pctDisplay(v){const n=toNum(v);if(Number.isNaN(n))return"--";return`${(n*100).toFixed(2)}%`}
function safePct(v){const n=toNum(v);if(Number.isNaN(n))return"--";return`${(n*100).toFixed(2)}%`}
function moneyDisplay(v){const n=Number(v||0);if(!Number.isFinite(n))return"0";return n.toLocaleString("zh-TW",{maximumFractionDigits:0})}
function safeMoney(v){const n=Number(String(v||"").replace(/,/g,""));if(!Number.isFinite(n))return"--";return n.toLocaleString("zh-TW",{maximumFractionDigits:0})}
function safeInt(v){const n=Number(String(v||"").replace(/,/g,""));if(!Number.isFinite(n))return"--";return n.toLocaleString("zh-TW",{maximumFractionDigits:0})}
function blankDash(v){return v===undefined||v===null||v===""?"--":String(v)}
function safe(v){return blankDash(v)}
function toNum(v){const n=Number(String(v??"").replace(/,/g,""));return Number.isFinite(n)?n:NaN}
function text(id,value){const el=document.getElementById(id);if(el)el.textContent=value}
function val(id,setValue){const el=document.getElementById(id);if(!el)return"";if(typeof setValue!=="undefined")el.value=setValue;return el.value||""}
function setBanner(text,color="#2f7d32"){const el=document.getElementById("syncBanner");if(el){el.textContent=text;el.style.color=color}}
// ================================
// v3.9 Execution Layer（不破壞補丁）
// ================================

// ===== 行為比例 =====
function getActionRatioV39(action){
  action = (action || "").toUpperCase();
  if(action === "BUY") return 1.0;
  if(action === "ADD") return 0.5;
  if(action === "REDUCE") return 0.3;
  if(action === "SELL") return 1.0;
  return 0;
}

// ===== 張數計算 =====
function calcLotsV39(r){
  const shares = Number(r.shares || 0);
  const price = Number(r.ref_price || r.avg_cost || 0);
  const amount = Number((r.suggested_amount || "").toString().replace(/,/g,"")) || 0;
  const action = (r.action || "").toUpperCase();

  const lotSize = 1000;

  // 買
  if(action === "BUY"){
    if(!price || !amount) return "--";
    const lots = Math.floor(amount / (price * lotSize));
    return Math.max(lots, 1);
  }

  // 減碼 / 出場
  if(action === "REDUCE" || action === "SELL"){
    const ratio = getActionRatioV39(action);
    let lots = Math.floor((shares * ratio) / lotSize);

    if(lots < 1 && shares >= lotSize){
      lots = 1; // 最少1張
    }

    return lots;
  }

  return "--";
}

// ===== 大盤狀態（簡化）=====
function getMarketV39(r){
  const mom20 = Number(r.mom20 || 0);
  const near = Number(r.near_high_20 || 0);

  if(mom20 > 0 && near > 0.9) return "🟢 多頭";
  if(mom20 > 0) return "🟡 中性";
  return "🔴 空頭";
}

// ===== 決策（升級版）=====
function getDecisionV39(r){
  const action = (r.action || "").toUpperCase();
  const chip = r.chip_label || "";

  if(chip.includes("強")){
    if(action==="BUY") return "🔥 建倉100%";
    if(action==="ADD") return "🚀 加碼50%";
    if(action==="REDUCE") return "⚠️ 減碼30%";
    if(action==="SELL") return "❗ 全出";
  }

  if(chip.includes("普通")){
    if(action==="BUY") return "⚠️ 小倉測試";
    if(action==="REDUCE") return "👉 減碼";
  }

  return "—";
}

// ===== Debug（只顯示，不影響原系統）=====
function attachV39Debug(){
  const rows = document.querySelectorAll("#positionBody tr");

  rows.forEach((row, idx) => {
    const data = POSITION_CACHE[idx];
    if(!data) return;

    const lots = calcLotsV39(data);
    const market = getMarketV39(data);
    const decision = getDecisionV39(data);

    row.title = `
v3.9:
決策: ${decision}
張數: ${lots}
市場: ${market}
    `;
  });
}

// ===== 自動掛載（不干擾原 render）=====
setTimeout(() => {
  try{
    attachV39Debug();
  }catch(e){
    console.log("v3.9 debug fail", e);
  }
}, 1500);
