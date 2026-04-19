
let positions = [];
let watchlist = [];

function save(name, data){
  localStorage.setItem(name, JSON.stringify(data));
}

function load(name){
  return JSON.parse(localStorage.getItem(name)||"[]");
}

function render(){
  positions = load("positions");
  watchlist = load("watchlist");

  let posT = document.querySelector("#pos tbody");
  posT.innerHTML="";
  positions.forEach((p,i)=>{
    posT.innerHTML += `<tr>
    <td>${p.code}</td>
    <td>監控中</td>
    <td><button onclick="removePos(${i})">刪</button></td>
    </tr>`;
  });

  let wT = document.querySelector("#watch tbody");
  wT.innerHTML="";
  watchlist.forEach((w,i)=>{
    wT.innerHTML += `<tr>
    <td>${w}</td>
    <td>觀察</td>
    <td><button onclick="removeWatch(${i})">刪</button></td>
    </tr>`;
  });
}

function addPosition(){
  let code=document.getElementById("pos_code").value;
  let shares=document.getElementById("pos_shares").value;
  let cost=document.getElementById("pos_cost").value;
  if(!code) return;
  let data=load("positions");
  data.push({code,shares,cost});
  save("positions",data);
  render();
}

function removePos(i){
  let data=load("positions");
  data.splice(i,1);
  save("positions",data);
  render();
}

function addWatch(){
  let code=document.getElementById("watch_code").value;
  if(!code) return;
  let data=load("watchlist");
  if(!data.includes(code)) data.push(code);
  save("watchlist",data);
  render();
}

function removeWatch(i){
  let data=load("watchlist");
  data.splice(i,1);
  save("watchlist",data);
  render();
}

render();
