// v266.10.1 FINAL
async function loadStatus(){
  const res = await fetch('./data/meta.json?t='+Date.now());
  const meta = await res.json();

  const batch = meta.trade_plan_batch;
  const now = new Date();

  function format(ts){
    return new Date(ts).toLocaleString('zh-TW',{hour12:false});
  }

  function isToday(ts){
    return new Date(ts).toDateString() === now.toDateString();
  }

  let text = "";
  if(isToday(batch)){
    text = `🟢 已更新（${format(batch)}）`;
  }else{
    text = `⚠️ 尚未更新（${format(batch)}）`;
  }

  document.getElementById('planStatus').innerText = text;
}

loadStatus();
