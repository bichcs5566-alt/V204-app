const DATA_DIR="./data";const GH_CONFIG_KEY="v3_github_config";let POSITION_CACHE=[];

document.addEventListener("DOMContentLoaded",async()=>{injectV384Style();bindUI();loadSavedConfig();await loadAll();});

function bindUI(){
  bind("refreshBtn","click",async()=>{setBanner("頁面重新同步中…","#2f7d32");await loadAll(true);});
  bind("updateBtn","click",async()=>{await dispatchWorkflow("v2_8_auto_update.yml",{},"已送出更新資料與策略，請等待 Actions 跑完後重新整理。");});
  bind("saveConfigBtn","click",saveConfig);
  bind("clearConfigBtn","click",clearConfig);
  bind("addPositionBtn","click",submitAddPositionInstant);
  document.addEventListener("click",async e=>{const b=e.target.closest("[data-remove-position]");if(b)await submitRemovePositionInstant(b.getAttribute("data-remove-position"));});
}

function bind(id,event,handler){const el=document.getElementById(id);if(!el)return;el.addEventListener(event,async e=>{try{await handler(e)}catch(err){console.error(err);setBanner(`操作失敗：${err.message}`,"#b42318")}})}


function injectV384Style(){
  if(document.getElementById("v384-style"))return;
  const style=document.createElement("style");
  style.id="v384-style";
  style.textContent=`
    .entry-display-action{display:inline-block;padding:5px 8px;border-radius:999px;font-size:12px;font-weight:900;white-space:nowrap;line-height:1.25}
    .entry-skip{background:#fdecec;color:#b42318}
    .entry-wait{background:#fff8e6;color:#b54708}
    .entry-buy{background:#ecfdf3;color:#027a48}
    .entry-hold{background:#eef3ff;color:#25406f}
    .chip-mini{display:inline-block;padding:4px 8px;border-radius:999px;font-size:12px;font-weight:900;white-space:nowrap;background:#f2f4f7;color:#344054}
    .chip-strong{background:#ecfdf3;color:#027a48}
    .chip-mid{background:#fff8e6;color:#b54708}
    .chip-weak{background:#fdecec;color:#b42318}
    .chip-none{background:#f2f4f7;color:#667085}
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

    const chipMap=buildChipMapV384(chipRows);
    const tradePlan=mergeChipIntoRowsV384(tradePlanRaw,chipMap);
    const position=mergeChipIntoRowsV384(positionRaw,chipMap);

    POSITION_CACHE = position;
    ensureV384Headers();
    renderMeta(meta);renderTradePlan(tradePlan);renderPosition(position);renderSummary(summary);renderDebug(debug);renderTierSummary(position);renderActionSummary(tradePlan,position);
    if(!document.getElementById("syncBanner").textContent.includes("已送出"))setBanner("頁面資料已同步","#2f7d32");
  }catch(err){console.error(err);setBanner(`讀取失敗：${err.message}`,"#b42318")}
}

async function fetchJSON(url,force=false){const finalUrl=force?`${url}?t=${Date.now()}`:url;const res=await fetch(finalUrl,{cache:"no-store"});if(!res.ok)throw new Error(`JSON 讀取失敗：${url}`);return await res.json()}
async function fetchCSV(url,force=false){const finalUrl=force?`${url}?t=${Date.now()}`:url;const res=await fetch(finalUrl,{cache:"no-store"});if(!res.ok)throw new Error(`CSV 讀取失敗：${url}`);return parseCSV(await res.text())}

function parseCSV(text){const cleaned=text.replace(/^\uFEFF/,"").trim();if(!cleaned)return[];const lines=cleaned.split(/\r?\n/);const headers=splitCSVLine(lines[0]).map(h=>h.trim());return lines.slice(1).filter(Boolean).map(line=>{const values=splitCSVLine(line),row={};headers.forEach((h,i)=>row[h]=(values[i]??"").trim());return row})}
function splitCSVLine(line){const result=[];let current="",inQuotes=false;for(let i=0;i<line.length;i++){const ch=line[i];if(ch==='"'){if(inQuotes&&line[i+1]==='"'){current+='"';i++}else inQuotes=!inQuotes}else if(ch===","&&!inQuotes){result.push(current);current=""}else current+=ch}result.push(current);return result}

function renderMeta(meta){text("nowTime",meta.now_time||meta.generated_at||"--");text("generatedAt",meta.generated_at||"--");text("signalDate",meta.signal_date||"--");text("tradeDate",meta.trade_date||"--");text("panelDate",meta.price_panel_latest_date||"--");text("dataState",prettyDataState(meta.data_state));text("sourceName",meta.source||"--");text("writebackState",prettyWriteback(meta.position_writeback_state))}
function renderTradePlan(rows){const body=document.getElementById("tradePlanBody");if(!rows.length){body.innerHTML=`<tr><td colspan="7" class="empty">目前沒有資料</td></tr>`;return}body.innerHTML=rows.map(r=>`<tr><td>${actionDecisionBadgeV384(r)}</td><td>${safe(r.stock_id)}</td><td>${prettyTier(r.price_tier)}</td><td>${safe(r.ref_price)}</td><td>${safe(r.target_weight)}</td><td>${safeMoney(r.suggested_amount)}</td><td>${chipMiniBadgeV384(r)}</td></tr>`).join("")}

function renderPosition(rows){
  const body=document.getElementById("positionBody");
  if(!rows.length){body.innerHTML=`<tr><td colspan="11" class="empty">目前沒有持倉資料</td></tr>`;return}
  body.innerHTML=rows.map(r=>`<tr><td>${safe(r.stock_id)}</td><td>${prettyTier(r.price_tier)}</td><td>${safe(r.ref_price)}</td><td>${safeInt(r.shares)}</td><td>${safe(r.avg_cost)}</td><td>${safePct(r.pnl_pct)}</td><td>${safe(r.target_weight)}</td><td>${badgeForAction(r.action)}</td><td>${chipMiniBadgeV384(r)}</td><td>${safe(r.note)}</td><td><button class="btn-remove" data-remove-position="${safe(r.stock_id)}">移除</button></td></tr>`).join("");
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
// v3.8.4 Decision Only Patch
// 原則：不改策略、不改 CSV、不改寫回，只改前端顯示。
// 動作欄：直接顯示「可進場 / 等回檔 / 尾段不追」
// 籌碼欄：只顯示「%（強/中/弱）」，不顯示長文字。
// ================================

function ensureV384Headers(){
  const tradeBody=document.getElementById("tradePlanBody");
  const tradeTable=tradeBody?tradeBody.closest("table"):null;
  const tradeHead=tradeTable?tradeTable.querySelector("thead tr"):null;
  if(tradeHead&&tradeHead.children.length>=7){
    tradeHead.children[6].textContent="籌碼";
  }

  const posBody=document.getElementById("positionBody");
  const posTable=posBody?posBody.closest("table"):null;
  const posHead=posTable?posTable.querySelector("thead tr"):null;
  if(posHead&&posHead.children.length===10){
    const th=document.createElement("th");
    th.textContent="籌碼";
    posHead.insertBefore(th,posHead.children[8]);
  }
}

function buildChipMapV384(chipRows){
  const map={};
  (chipRows||[]).forEach(r=>{
    const sid=normalizeStockIdV384(r.stock_id||r.stock||r.symbol||r.code);
    if(!sid)return;
    map[sid]={
      chip_score:r.chip_score||"",
      chip_label:r.chip_label||"",
      chip_tags:r.chip_tags||"",
      chip_note:r.chip_note||"",
      concentration_pct:r.concentration_pct||"",
      chip_pct:r.chip_pct||"",
      chip_percent:r.chip_percent||"",
      vol_ratio_5:r.vol_ratio_5||"",
      mom5:r.mom5||"",
      mom20:r.mom20||"",
      near_high_20:r.near_high_20||""
    };
  });
  return map;
}

function mergeChipIntoRowsV384(rows,chipMap){
  return (rows||[]).map(r=>{
    const sid=normalizeStockIdV384(r.stock_id||r.stock||r.symbol||r.code);
    const chip=chipMap[sid]||{};
    return {...r,...chip};
  });
}

function normalizeStockIdV384(v){
  return String(v??"").replace(/^\uFEFF/,"").replace(/[^\dA-Za-z]/g,"").trim();
}

function getEntryDecisionV384(r){
  const action=(r.action||"").toUpperCase();
  const near=toNum(r.near_high_20);
  const mom5=toNum(r.mom5);
  const mom20=toNum(r.mom20);
  const hasSignal=Number.isFinite(near)||Number.isFinite(mom5)||Number.isFinite(mom20);

  if(action!=="BUY"){
    return {display:action, level:"hold", label:null};
  }

  if(!hasSignal){
    return {display:"WAIT", level:"wait", label:"買進<br>等回檔"};
  }

  if(mom20>0&&mom5<0){
    return {display:"WAIT", level:"wait", label:"買進<br>等止跌"};
  }

  if((near>=0.97&&mom5>=0.03)||(near>=0.985)||(mom5>=0.08)){
    return {display:"SKIP", level:"skip", label:"買進<br>尾段不追"};
  }

  if(near>=0.93||mom5>=0.05){
    return {display:"WAIT", level:"wait", label:"買進<br>等回檔"};
  }

  if(mom20>0&&mom5>=0&&mom5<0.05&&near<0.93){
    return {display:"BUY", level:"buy", label:"買進<br>可進場"};
  }

  return {display:"WAIT", level:"wait", label:"買進<br>等回檔"};
}

function actionDecisionBadgeV384(r){
  const d=getEntryDecisionV384(r);
  const action=(r.action||"").toUpperCase();

  if(action==="BUY"){
    if(d.display==="SKIP")return `<span class="entry-display-action entry-skip">${d.label}</span>`;
    if(d.display==="WAIT")return `<span class="entry-display-action entry-wait">${d.label}</span>`;
    return `<span class="entry-display-action entry-buy">${d.label}</span>`;
  }

  return badgeForAction(r.action);
}

function chipPercentV384(r){
  const direct=toNum(r.concentration_pct||r.chip_pct||r.chip_percent);
  if(Number.isFinite(direct))return Math.max(0,Math.min(100,direct));

  const score=toNum(r.chip_score);
  if(Number.isFinite(score)){
    return Math.max(0,Math.min(100,Math.round(score*12.5)));
  }

  const label=String(r.chip_label||"");
  if(label.includes("強"))return 75;
  if(label.includes("普通"))return 50;
  if(label.includes("注意"))return 35;

  return null;
}

function chipLevelV384(pct){
  if(pct===null||pct===undefined||!Number.isFinite(Number(pct)))return "—";
  if(pct>=70)return "強";
  if(pct>=45)return "中";
  return "弱";
}

function chipClassV384(level){
  if(level==="強")return "chip-strong";
  if(level==="中")return "chip-mid";
  if(level==="弱")return "chip-weak";
  return "chip-none";
}

function chipMiniBadgeV384(r){
  const pct=chipPercentV384(r);
  const level=chipLevelV384(pct);
  const cls=chipClassV384(level);
  const label=(pct===null||pct===undefined||!Number.isFinite(Number(pct)))?"--":`${Math.round(pct)}%（${level}）`;
  return `<span class="chip-mini ${cls}">${label}</span>`;
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
