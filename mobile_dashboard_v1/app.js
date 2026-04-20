let dataVersion = null;

function reloadData(){
  loadData();
}

function setStatus(now){
  document.getElementById("lastUpdate").innerText = now;
  document.getElementById("signalDate").innerText = now.split(" ")[0];
  document.getElementById("tradeDate").innerText = now.split(" ")[0];

  if(dataVersion === now){
    document.getElementById("dataState").innerText = "⚠️ 舊資料";
  }else{
    document.getElementById("dataState").innerText = "✅ 最新資料";
    dataVersion = now;
  }
}

function loadData(){
  let now = new Date().toLocaleString();
  setStatus(now);

  // mock data
  let trades = [
    {action:"買進", stock:"2330", price:2000},
    {action:"觀察", stock:"2317", price:150}
  ];

  let table = document.getElementById("tradeTable");
  table.innerHTML = "<tr><th>動作</th><th>股票</th><th>價格</th><th>操作</th></tr>";

  trades.forEach(t=>{
    table.innerHTML += `<tr>
      <td>${t.action}</td>
      <td>${t.stock}</td>
      <td>${t.price}</td>
      <td><button onclick="addToPosition('${t.stock}')">加入持倉</button></td>
    </tr>`;
  });

  renderPosition();
  renderWatch();
}

function addToPosition(stock){
  let list = JSON.parse(localStorage.getItem("positions")||"[]");
  list.push({stock, qty:100, cost:0});
  localStorage.setItem("positions", JSON.stringify(list));
  renderPosition();
}

function addPosition(){
  let stock = document.getElementById("stockInput").value;
  let qty = document.getElementById("qtyInput").value;
  let cost = document.getElementById("costInput").value;

  let list = JSON.parse(localStorage.getItem("positions")||"[]");
  list.push({stock, qty, cost});
  localStorage.setItem("positions", JSON.stringify(list));
  renderPosition();
}

function renderPosition(){
  let list = JSON.parse(localStorage.getItem("positions")||"[]");
  let table = document.getElementById("positionTable");

  table.innerHTML = "<tr><th>股票</th><th>股數</th><th>成本</th><th>動作</th></tr>";

  list.forEach((p,i)=>{
    table.innerHTML += `<tr>
      <td>${p.stock}</td>
      <td>${p.qty}</td>
      <td>${p.cost}</td>
      <td><button onclick="removePosition(${i})">移除</button></td>
    </tr>`;
  });
}

function removePosition(i){
  let list = JSON.parse(localStorage.getItem("positions")||"[]");
  list.splice(i,1);
  localStorage.setItem("positions", JSON.stringify(list));
  renderPosition();
}

function addWatch(){
  let stock = document.getElementById("watchInput").value;
  let list = JSON.parse(localStorage.getItem("watch")||"[]");
  list.push(stock);
  localStorage.setItem("watch", JSON.stringify(list));
  renderWatch();
}

function renderWatch(){
  let list = JSON.parse(localStorage.getItem("watch")||"[]");
  let table = document.getElementById("watchTable");

  table.innerHTML = "<tr><th>股票</th><th>狀態</th><th>動作</th></tr>";

  list.forEach((s,i)=>{
    table.innerHTML += `<tr>
      <td>${s}</td>
      <td>觀察</td>
      <td><button onclick="removeWatch(${i})">移除</button></td>
    </tr>`;
  });
}

function removeWatch(i){
  let list = JSON.parse(localStorage.getItem("watch")||"[]");
  list.splice(i,1);
  localStorage.setItem("watch", JSON.stringify(list));
  renderWatch();
}

loadData();
