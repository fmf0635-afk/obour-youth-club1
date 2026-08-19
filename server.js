const express = require('express');
const http = require('http');
const fs = require('fs').promises;
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const PORT = process.env.PORT || 3000;
const HOST = '0.0.0.0';
const DATA_DIR = process.env.DATA_DIR || (process.env.RAILWAY_ENVIRONMENT ? '/data' : path.join(os.homedir(), 'ObourYouthClubData', 'data'));
const DATA_FILE = path.join(DATA_DIR, 'db.json');
const BACKUP_DIR = process.env.BACKUP_DIR || path.join(os.path.dirname(DATA_FILE), 'backups');
const LEGACY_DATA_FILE = path.join(__dirname, 'db.json');
let dataStoreReady = null;
async function ensureDataStore(){
  if(!dataStoreReady){
    dataStoreReady=(async()=>{
      await fs.mkdir(DATA_DIR,{recursive:true});
      await fs.mkdir(BACKUP_DIR,{recursive:true});
      try{ await fs.access(DATA_FILE); }
      catch(e){
        if(e.code!=='ENOENT') throw e;
        try{ await fs.copyFile(LEGACY_DATA_FILE, DATA_FILE); }
        catch(err){
          if(err.code!=='ENOENT') throw err;
          await fs.writeFile(DATA_FILE, JSON.stringify({sports:[],users:[],federations:[],activity:[],attendance:[],archive:[],backups:[]},null,2),'utf8');
        }
      }
    })().catch(err=>{dataStoreReady=null; throw err;});
  }
  return dataStoreReady;
}
const REMOTE_SOURCE_URL = process.env.REMOTE_SOURCE_URL || 'https://obour-youth-club-production.up.railway.app';
const REMOTE_SOURCE_TOKEN = process.env.REMOTE_SOURCE_TOKEN || '';
const ADMIN_USER = process.env.ADMIN_USER || 'Fasmo';
const ADMIN_PASS = process.env.ADMIN_PASS || 'Fasmo123#';
const sessions = new Map();
const online = new Map();
let wsClients = new Set();
let saveQueue = Promise.resolve();

app.use(express.json({limit:'25mb'}));
app.use(require('cors')());
app.use(express.static(__dirname));

function uid(){return crypto.randomBytes(24).toString('hex');}
function hashPassword(p,salt=crypto.randomBytes(16).toString('hex')){return {salt,hash:crypto.scryptSync(String(p),salt,64).toString('hex')};}
function verifyPassword(p,u){try{return crypto.timingSafeEqual(Buffer.from(hashPassword(p,u.salt).hash,'hex'),Buffer.from(u.passwordHash,'hex'));}catch{return false;}}
function safeUser(u){const {passwordHash,salt,...rest}=u;return {...rest,sports:Array.isArray(u.sports)?u.sports:(u.sport?[u.sport]:[])};}
async function loadDatabase(){
  await ensureDataStore();
  try{return JSON.parse(await fs.readFile(DATA_FILE,'utf8'));}
  catch(e){if(e instanceof SyntaxError){const broken=`${DATA_FILE}.broken-${Date.now()}.json`;await fs.copyFile(DATA_FILE,broken).catch(()=>{});const d={sports:[],users:[],federations:[],activity:[],attendance:[],archive:[],backups:[]};await writeDatabaseFile(d,false);return d;}throw e;}
}
async function writeDatabaseFile(data, makeBackup=true){
  await ensureDataStore();
  const snapshot=JSON.stringify(data,null,2);
  const tmp=DATA_FILE+'.tmp';
  if(makeBackup){
    const stamp=new Date().toISOString().replace(/[:.]/g,'-');
    await fs.writeFile(path.join(BACKUP_DIR,`db-${stamp}.json`),snapshot,'utf8').catch(()=>{});
  }
  await fs.writeFile(tmp,snapshot,'utf8');
  await fs.rename(tmp,DATA_FILE);
  if(makeBackup){
    try{
      const files=(await fs.readdir(BACKUP_DIR)).filter(f=>f.endsWith('.json')).sort().reverse();
      await Promise.all(files.slice(30).map(f=>fs.unlink(path.join(BACKUP_DIR,f)).catch(()=>{})));
    }catch(_){}
  }
}
async function saveDatabase(data){
  saveQueue=saveQueue.catch(()=>{}).then(()=>writeDatabaseFile(data,true));
  await saveQueue;
  broadcast({type:'data-changed'});
  return data;
}
function broadcast(payload){const s=JSON.stringify(payload);wsClients.forEach(c=>{if(c.readyState===WebSocket.OPEN)c.send(s);});}
function auth(req){const h=req.headers.authorization||'';const token=h.startsWith('Bearer ')?h.slice(7):'';return sessions.get(token)||null;}
function requireAdmin(req,res,next){const u=auth(req);if(!u||u.role!=='admin')return res.status(403).json({ok:false,message:'Admin only'});req.user=u;next();}
function canEditSportForUser(u,sport){
  if(!u)return false;
  if(u.role==='admin')return true;
  const perms=Array.isArray(u.permissions)?u.permissions:[];
  return perms.some(p=>(String(p.sport)===String(sport?.name)||String(p.sportId)===String(sport?.id))&&p.canEdit===true);
}
function addActivity(data){saveQueue=saveQueue.catch(()=>{}).then(async()=>{const db=await loadDatabase();db.activity=db.activity||[];db.activity.unshift({...data,id:Date.now()+Math.random(),at:new Date().toISOString()});db.activity=db.activity.slice(0,5000);await writeDatabaseFile(db,false);broadcast({type:'data-changed'});}).catch(()=>{});}
function touch(token){const s=sessions.get(token);if(!s)return null;s.lastSeen=Date.now();online.set(s.userId,{...s,lastSeen:s.lastSeen});return s;}

wss.on('connection',(socket)=>{wsClients.add(socket);socket.on('close',()=>wsClients.delete(socket));});
setInterval(()=>{const now=Date.now();for(const [t,s] of sessions){if(now-s.lastSeen>24*60*60*1000){sessions.delete(t);online.delete(s.userId);}}broadcast({type:'presence',online:[...online.values()].map(s=>safeUser(s))});},30000);


function monthKey(date=new Date()){const d=new Date(date);return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;}
function paidForMonth(player, key=monthKey()){
  if(player?.isPaid===false) return false;
  const raw=String(player?.paidThroughMonth||'').trim();
  if(!raw) return false;
  if(/^\d{4}-\d{2}$/.test(raw)) return raw===key;
  const names=['يناير','فبراير','مارس','ابريل','أبريل','مايو','يونيو','يوليو','أغسطس','سبتمبر','أكتوبر','نوفمبر','ديسمبر'];
  const d=new Date(`${key}-01`), y=d.getFullYear(), n=d.getMonth();
  const low=raw.toLowerCase();
  if(low.includes(String(y)) && names.some((x,i)=>i===n && low.includes(x))) return true;
  return low.includes('الشهر الحالي') || low.includes('current month');
}
async function buildSubscriptionArchive(){
  const now=new Date();
  if(now.getDate()<=5) return;
  const db=await loadDatabase();
  db.archive=Array.isArray(db.archive)?db.archive:[];
  const key=monthKey(now);
  const already=db.archive.some(x=>x.month===key);
  if(already) return;
  const items=(db.sports||[]).map(s=>{
    const expired=(s.players||[]).filter(p=>!paidForMonth(p,key)).map(p=>({id:p.id,name:p.name,membershipNumber:p.membershipNumber||'',receiptNo:p.receiptNo||''}));
    return expired.length?{sportId:s.id,sport:s.name,month:key,createdAt:now.toISOString(),players:expired}:null;
  }).filter(Boolean);
  db.archive.push(...items);
  await saveDatabase(db);
}

app.get('/health',(req,res)=>res.json({ok:true,service:'obour-youth-club'}));

app.get('/api/admin/backup',requireAdmin,async(req,res)=>{
  try{
    await buildSubscriptionArchive();
    const data=await fs.readFile(DATA_FILE,'utf8');
    res.setHeader('Content-Type','application/json; charset=utf-8');
    res.setHeader('Content-Disposition',`attachment; filename="obour-youth-club-backup-${new Date().toISOString().slice(0,10)}.json"`);
    res.send(data);
  }catch(e){res.status(500).json({ok:false,message:'تعذر إنشاء النسخة الاحتياطية'});}
});
app.get('/api/archive',async(req,res)=>{const u=auth(req);if(!u)return res.status(401).json({message:'Login required'});await buildSubscriptionArchive();const db=await loadDatabase();res.json(db.archive||[]);});
app.get('/api/attendance',async(req,res)=>{
  const u=auth(req); if(!u)return res.status(401).json({message:'Login required'});
  const db=await loadDatabase(); const date=String(req.query.date||'');
  const sportId=req.query.sportId?Number(req.query.sportId):null;
  if(sportId){const sport=(db.sports||[]).find(x=>Number(x.id)===sportId);if(!sport)return res.status(404).json({message:'Sport not found'});if(!canViewSportForUser(u,sport))return res.status(403).json({message:'هذه اللعبة غير متاحة لهذا الحساب'});}
  let rows=Array.isArray(db.attendance)?db.attendance:[];
  if(date)rows=rows.filter(x=>x.date===date);
  if(sportId)rows=rows.filter(x=>Number(x.sportId)===sportId);
  res.json(rows);
});
app.post('/api/attendance',async(req,res)=>{
  const u=auth(req); if(!u || (u.role!=='admin'&&!u.canEdit))return res.status(403).json({message:'صلاحية تعديل مطلوبة'});
  const db=await loadDatabase(); db.attendance=Array.isArray(db.attendance)?db.attendance:[];
  const sportId=Number(req.body?.sportId), date=String(req.body?.date||'').trim();
  const sport=(db.sports||[]).find(x=>Number(x.id)===sportId);
  if(!sport||!/^\d{4}-\d{2}-\d{2}$/.test(date))return res.status(400).json({message:'اختر الرياضة والتاريخ بشكل صحيح'});
  const records=Array.isArray(req.body?.records)?req.body.records:[];
  for(const r of records){
    const player=(sport.players||[]).find(p=>Number(p.id)===Number(r.playerId)); if(!player)continue;
    const status=['present','absent','excused'].includes(r.status)?r.status:'absent';
    const i=db.attendance.findIndex(x=>Number(x.sportId)===sportId&&Number(x.playerId)===Number(player.id)&&x.date===date);
    const row={id:i>=0?db.attendance[i].id:uid(),sportId, sport:sport.name,playerId:player.id,playerName:player.name,date,status,updatedAt:new Date().toISOString(),updatedBy:u.username};
    if(i>=0)db.attendance[i]=row; else db.attendance.push(row);
  }
  await saveDatabase(db); res.json({ok:true,count:records.length});
});
app.put('/api/sports/:sportId/players/:playerId/receipt',async(req,res)=>{
  const u=auth(req);
  const db=await loadDatabase();const sport=db.sports.find(x=>Number(x.id)===Number(req.params.sportId));if(!canEditSportForUser(u,sport))return res.status(403).json({message:'صلاحية تعديل مطلوبة لهذه اللعبة'});const p=sport?.players?.find(x=>Number(x.id)===Number(req.params.playerId));
  if(!p)return res.status(404).json({message:'اللاعب غير موجود'});
  const image=String(req.body?.receiptImage||'');if(image.length>10*1024*1024)return res.status(413).json({message:'صورة الإيصال كبيرة جداً'});
  p.receiptImage=image;p.receiptDate=req.body?.receiptDate||new Date().toISOString().slice(0,10);
  if(req.body?.paidThroughMonth!==undefined)p.paidThroughMonth=String(req.body.paidThroughMonth||'');
  p.isPaid=true;await saveDatabase(db);res.json(p);
});

app.get('/api/sports',async(req,res)=>{const db=await loadDatabase();const u=auth(req);const sports=Array.isArray(db.sports)?db.sports:[];if(u&&u.role==='user'){const allowed=new Set((Array.isArray(u.sports)?u.sports:(u.sport?[u.sport]:[])).map(String));return res.json(sports.filter(s=>allowed.has(String(s.name)) || allowed.has(String(s.id))));}res.json(sports);});

function normalizeRemotePlayer(r, sportName) {
  const name = String(r?.name ?? r?.['اسم اللاعب'] ?? r?.['الاسم'] ?? '').trim();
  return {
    id: Number(r?.id) || Date.now() + Math.floor(Math.random() * 1000000),
    name,
    registrationDate: r?.registrationDate || new Date().toISOString().slice(0,10),
    registrationNo: String(r?.registrationNo ?? r?.['رقم القيد'] ?? ''),
    membershipNumber: String(r?.membershipNumber ?? r?.['رقم العضوية'] ?? ''),
    receiptNo: String(r?.receiptNo ?? r?.['رقم الإيصال'] ?? r?.['رقم الايصال'] ?? ''),
    age: Number(r?.age ?? r?.['السن'] ?? 0),
    medicalHistory: r?.medicalHistory ?? r?.['التاريخ المرضي'] ?? 'سليم',
    isMember: r?.isMember !== undefined ? !!r.isMember : true,
    isPaid: r?.isPaid !== undefined ? !!r.isPaid : true,
    paidThroughMonth: r?.paidThroughMonth ?? '',
    coach: String(r?.coach ?? r?.['الكابتن'] ?? ''),
    notes: r?.notes ?? r?.['ملاحظات'] ?? '',
    federation: r?.federation ?? '',
    federationId: r?.federationId ?? '',
    photo: r?.photo ?? '',
    fileUrl: r?.fileUrl ?? '',
    sourceSport: sportName,
    sourceId: r?.id ?? ''
  };
}

async function remoteLoginAndFetchSports(sourceUrl, username='', password=''){
  const base=String(sourceUrl||'').replace(/\/$/,'');
  if(!/^https?:\/\//i.test(base)) throw new Error('رابط الاتحاد يجب أن يبدأ بـ http:// أو https://');
  const headers={Accept:'application/json'};
  if(username || password){
    let token='';
    for(const endpoint of ['/api/auth/login','/api/admin/login']){
      try{
        const lr=await fetch(base+endpoint,{method:'POST',headers:{'Content-Type':'application/json',Accept:'application/json'},body:JSON.stringify(endpoint.includes('/admin/')?{user:username,pass:password}:{username,password})});
        const body=await lr.json().catch(()=>({}));
        if(lr.ok && body.token){ token=body.token; break; }
      }catch(_){}
    }
    if(!token) throw new Error('بيانات حساب الاتحاد غير صحيحة أو الحساب غير مصرح له بالوصول.');
    headers.Authorization=`Bearer ${token}`;
  }
  const response=await fetch(base+'/api/sports',{headers});
  const text=await response.text();
  if(!response.ok) throw new Error(`الموقع المصدر أعاد الحالة ${response.status}`);
  let data; try{data=JSON.parse(text)}catch{throw new Error('الموقع المصدر لم يُرجع JSON صالحًا')}
  if(!Array.isArray(data)) throw new Error('بيانات الرياضات من الموقع المصدر غير صحيحة');
  return data;
}
async function fetchRemoteSports(){
  const headers={Accept:'application/json'};
  if(REMOTE_SOURCE_TOKEN)headers.Authorization=`Bearer ${REMOTE_SOURCE_TOKEN}`;
  const r=await fetch(`${REMOTE_SOURCE_URL.replace(/\/$/,'')}/api/sports`,{headers});
  const text=await r.text(); if(!r.ok)throw new Error(`Remote server returned ${r.status}`);
  const data=JSON.parse(text); if(!Array.isArray(data))throw new Error('بيانات المصدر غير صحيحة'); return data;
}

app.post('/api/remote-import/preview',requireAdmin,async(req,res)=>{
  try {
    const remoteSports = await remoteLoginAndFetchSports(req.body?.url||REMOTE_SOURCE_URL, req.body?.username||'', req.body?.password||'');
    const preview = remoteSports.map(s => ({
      id: s.id,
      name: s.name || 'رياضة بدون اسم',
      players: Array.isArray(s.players) ? s.players.length : 0,
      coaches: Array.isArray(s.coaches) ? s.coaches.length : 0
    }));
    res.json({ok:true,source:REMOTE_SOURCE_URL,sports:preview,totalSports:preview.length,totalPlayers:preview.reduce((n,s)=>n+s.players,0)});
  } catch (e) {
    res.status(502).json({ok:false,message:`تعذر الوصول إلى الموقع المصدر: ${e.message}`});
  }
});

app.post('/api/remote-import/sync',requireAdmin,async(req,res)=>{
  try {
    const remoteSports = await remoteLoginAndFetchSports(req.body?.url||REMOTE_SOURCE_URL, req.body?.username||'', req.body?.password||'');
    const db = await loadDatabase();
    db.sports = Array.isArray(db.sports) ? db.sports : [];
    const selected = Array.isArray(req.body?.sportIds) && req.body.sportIds.length
      ? new Set(req.body.sportIds.map(String)) : null;
    const stats = { sportsCreated:0, sportsUpdated:0, playersAdded:0, playersUpdated:0, playersSkipped:0 };

    for (const remote of remoteSports) {
      if (selected && !selected.has(String(remote.id))) continue;
      const remoteName = String(remote.name || '').trim();
      if (!remoteName) continue;
      let local = db.sports.find(s => String(s.name || '').trim().toLowerCase() === remoteName.toLowerCase());
      if (!local) {
        local = {
          id: Date.now() + Math.floor(Math.random()*100000), name: remoteName,
          technicalDirector: remote.technicalDirector || '', coaches: Array.isArray(remote.coaches) ? remote.coaches : [],
          admins: Array.isArray(remote.admins) ? remote.admins : [], formUrl: remote.formUrl || '', players: [],
          icon: remote.icon || 'fa-solid fa-trophy'
        };
        db.sports.push(local); stats.sportsCreated++;
      } else {
        stats.sportsUpdated++;
        if ((!local.technicalDirector || local.technicalDirector === '') && remote.technicalDirector) local.technicalDirector = remote.technicalDirector;
        if ((!Array.isArray(local.coaches) || local.coaches.length === 0) && Array.isArray(remote.coaches)) local.coaches = remote.coaches;
        if (!local.icon && remote.icon) local.icon = remote.icon;
      }
      local.players = Array.isArray(local.players) ? local.players : [];
      const remotePlayers = Array.isArray(remote.players) ? remote.players : [];
      for (const raw of remotePlayers) {
        const rp = normalizeRemotePlayer(raw, remoteName);
        if (!rp.name) { stats.playersSkipped++; continue; }
        const keyMembership = rp.membershipNumber.trim();
        const keyReg = rp.registrationNo.trim();
        const match = local.players.find(lp =>
          (keyMembership && String(lp.membershipNumber || '').trim() === keyMembership) ||
          (keyReg && keyReg !== 'N/A' && String(lp.registrationNo || '').trim() === keyReg) ||
          (!keyMembership && !keyReg && String(lp.name || '').trim().toLowerCase() === rp.name.toLowerCase())
        );
        if (match) {
          const keep = { id: match.id, ...match };
          Object.assign(match, rp, keep);
          stats.playersUpdated++;
        } else {
          local.players.push(rp);
          stats.playersAdded++;
        }
      }
    }
    await saveDatabase(db);
    addActivity({userId:'admin',username:ADMIN_USER,name:ADMIN_USER,action:'استيراد من موقع مركز شباب العبور',details:`رياضات جديدة: ${stats.sportsCreated}، لاعبين جدد: ${stats.playersAdded}، لاعبين محدثين: ${stats.playersUpdated}`});
    res.json({ok:true,source:REMOTE_SOURCE_URL,stats});
  } catch (e) {
    res.status(502).json({ok:false,message:`فشل الاستيراد من الموقع المصدر: ${e.message}`});
  }
});
app.get('/api/sports/:id',async(req,res)=>{const db=await loadDatabase();const s=(db.sports||[]).find(x=>x.id===Number(req.params.id));if(!s)return res.status(404).json({message:'Sport not found'});const u=auth(req);if(!u)return res.status(401).json({message:'Login required'});if(u.role!=='admin'){const allowed=new Set((Array.isArray(u.sports)?u.sports:(u.sport?[u.sport]:[])).map(String));if(!allowed.has(String(s.name))&&!allowed.has(String(s.id)))return res.status(403).json({message:'هذه اللعبة غير متاحة لهذا الحساب'});}res.json(s);});

app.post('/api/admin/login',(req,res)=>{const {user,pass}=req.body||{};if(user===ADMIN_USER&&pass===ADMIN_PASS){const token=uid(),u={userId:'admin',username:ADMIN_USER,name:ADMIN_USER,email:'',role:'admin',sport:'',canEdit:true,token};sessions.set(token,{...u,lastSeen:Date.now()});online.set('admin',sessions.get(token));addActivity({userId:'admin',username:ADMIN_USER,name:ADMIN_USER,action:'دخول الأدمن',sport:''});return res.json({ok:true,token,user:safeUser(sessions.get(token))});}res.status(401).json({ok:false,message:'بيانات أدمن غير صحيحة'});});

app.post('/api/auth/register',async(req,res)=>{const {username,password}=req.body||{};const cleanUsername=String(username||'').trim();if(!cleanUsername||!password)return res.status(400).json({message:'اكتب اسم المستخدم وكلمة المرور فقط.'});if(cleanUsername.length<3)return res.status(400).json({message:'اسم المستخدم يجب أن يكون 3 أحرف على الأقل.'});if(String(password).length<4)return res.status(400).json({message:'كلمة المرور يجب أن تكون 4 أحرف على الأقل.'});const db=await loadDatabase();db.users=db.users||[];if(db.users.some(u=>String(u.username||'').toLowerCase()===cleanUsername.toLowerCase()))return res.status(409).json({message:'اسم المستخدم مستخدم بالفعل'});const hp=hashPassword(password);const displayName=String(req.body.name||cleanUsername).trim()||cleanUsername;const user={id:uid(),username:cleanUsername,name:displayName,email:'',sports:[],sport:'',role:'user',canEdit:false,permissions:[],approved:false,passwordHash:hp.hash,salt:hp.salt,createdAt:new Date().toISOString()};db.users.push(user);await saveDatabase(db);addActivity({userId:user.id,username:cleanUsername,name:displayName,action:'طلب حساب جديد - بانتظار موافقة الأدمن',sport:'',details:'يحتاج موافقة الأدمن وتعيين لعبة'});res.status(201).json({ok:true,pending:true,message:'تم إرسال طلب الحساب إلى الأدمن. بعد الموافقة وتعيين اللعبة يمكنك تسجيل الدخول.'});});

app.post('/api/auth/login',async(req,res)=>{const {username,password}=req.body||{};const db=await loadDatabase();const u=(db.users||[]).find(x=>x.username.toLowerCase()===String(username||'').toLowerCase());if(!u||!verifyPassword(password,u))return res.status(401).json({message:'اسم المستخدم أو كلمة المرور غير صحيحة'});if(!u.approved)return res.status(403).json({message:'الحساب مسجل وينتظر موافقة الأدمن.'});const sports=Array.isArray(u.sports)?u.sports:(u.sport?[u.sport]:[]);if(!sports.length)return res.status(403).json({message:'تمت الموافقة على الحساب، لكن الأدمن لم يحدد لك لعبة بعد.'});const permissions=Array.isArray(u.permissions)?u.permissions:sports.map(x=>({sport:x,canEdit:!!u.canEdit}));const token=uid(),session={userId:u.id,username:u.username,name:u.name,email:u.email||'',role:u.role||'user',sports,permissions,sport:sports[0]||'',canEdit:!!u.canEdit,token,lastSeen:Date.now()};sessions.set(token,session);online.set(u.id,session);addActivity({userId:u.id,username:u.username,name:u.name,action:'تسجيل دخول',sport:sports.join('، ')});res.json({ok:true,token,user:safeUser(session)});});
app.post('/api/auth/heartbeat',(req,res)=>{const s=touch((req.headers.authorization||'').replace(/^Bearer /,''));if(!s)return res.status(401).json({ok:false});res.json({ok:true,user:safeUser(s)});});
app.post('/api/auth/logout',(req,res)=>{const t=(req.headers.authorization||'').replace(/^Bearer /,'');const s=sessions.get(t);if(s){addActivity({userId:s.userId,username:s.username,name:s.name,action:'تسجيل خروج',sport:s.sport});sessions.delete(t);online.delete(s.userId);}res.json({ok:true});});

app.get('/api/admin/users',requireAdmin,async(req,res)=>{const db=await loadDatabase();res.json((db.users||[]).map(u=>({...safeUser(u),online:online.has(u.id)})));});
app.put('/api/admin/users/:id',requireAdmin,async(req,res)=>{const db=await loadDatabase();const u=(db.users||[]).find(x=>x.id===req.params.id);if(!u)return res.status(404).json({message:'User not found'});if('approved' in req.body)u.approved=!!req.body.approved;if('canEdit' in req.body)u.canEdit=!!req.body.canEdit;if('role' in req.body && ['user','admin'].includes(req.body.role))u.role=req.body.role;if('sports' in req.body){u.sports=Array.isArray(req.body.sports)?req.body.sports.filter(Boolean):[];u.sport=u.sports[0]||'';u.permissions=u.sports.map(sp=>({sport:sp,canEdit:!!u.canEdit}));}else if('sport' in req.body){u.sport=req.body.sport;u.sports=[req.body.sport].filter(Boolean);u.permissions=u.sports.map(sp=>({sport:sp,canEdit:!!u.canEdit}));}if('password' in req.body && String(req.body.password).length>=4){const hp=hashPassword(req.body.password);u.passwordHash=hp.hash;u.salt=hp.salt;}await saveDatabase(db);for(const [t,s] of sessions){if(s.userId===u.id){s.canEdit=!!u.canEdit;s.sports=u.sports||[u.sport];s.permissions=u.permissions||[];s.sport=s.sports[0]||'';s.role=u.role||'user';}}addActivity({userId:'admin',username:ADMIN_USER,name:ADMIN_USER,action:'تعديل صلاحيات حساب',sport:(u.sports||[]).join('، '),details:u.username});res.json(safeUser(u));});
app.post('/api/admin/admins',requireAdmin,async(req,res)=>{const {username,password,name}=req.body||{};if(!username||!password||!name)return res.status(400).json({message:'أكمل بيانات الأدمن'});const db=await loadDatabase();db.users=db.users||[];if(db.users.some(u=>u.username.toLowerCase()===String(username).toLowerCase()) || String(username).toLowerCase()===ADMIN_USER.toLowerCase())return res.status(409).json({message:'اسم المستخدم مستخدم بالفعل'});const hp=hashPassword(password);const user={id:uid(),username,name,email:req.body.email||'',sports:[],sport:'',role:'admin',canEdit:true,permissions:[],approved:true,passwordHash:hp.hash,salt:hp.salt,createdAt:new Date().toISOString()};db.users.push(user);await saveDatabase(db);addActivity({userId:'admin',username:ADMIN_USER,name:ADMIN_USER,action:'إضافة أدمن',details:username});res.status(201).json(safeUser(user));});
app.delete('/api/admin/users/:id',requireAdmin,async(req,res)=>{const db=await loadDatabase();db.users=(db.users||[]).filter(u=>u.id!==req.params.id);for(const [t,s] of sessions)if(s.userId===req.params.id){sessions.delete(t);online.delete(s.userId);}await saveDatabase(db);addActivity({userId:'admin',username:ADMIN_USER,name:ADMIN_USER,action:'حذف حساب',details:req.params.id});res.status(204).end();});
app.get('/api/admin/online',requireAdmin,(req,res)=>res.json([...online.values()].map(s=>safeUser(s))));
app.get('/api/admin/activity',requireAdmin,async(req,res)=>res.json((await loadDatabase()).activity||[]));
app.post('/api/admin/activity',requireAdmin,async(req,res)=>{addActivity({userId:'admin',username:ADMIN_USER,name:ADMIN_USER,action:req.body.action||'',sport:req.body.sport||'',details:req.body.details||''});res.json({ok:true});});

app.post('/api/sports',requireAdmin,async(req,res)=>{try{const db=await loadDatabase();db.sports=Array.isArray(db.sports)?db.sports:[];const name=String(req.body?.name||'').trim();if(name.length<2)return res.status(400).json({ok:false,message:'اسم الرياضة يجب أن يكون حرفين على الأقل.'});if(db.sports.some(x=>String(x.name||'').trim().toLowerCase()===name.toLowerCase()))return res.status(409).json({ok:false,message:'هذه الرياضة موجودة بالفعل.'});const s={id:Date.now()+Math.floor(Math.random()*10000),name,technicalDirector:String(req.body?.technicalDirector||'').trim(),coaches:[],admins:[],formUrl:'',players:[],icon:String(req.body?.icon||'fa-solid fa-medal')};db.sports.push(s);await saveDatabase(db);addActivity({userId:'admin',username:ADMIN_USER,name:ADMIN_USER,action:'إضافة رياضة',sport:s.name});res.status(201).json(s);}catch(e){console.error('Add sport failed',e);res.status(500).json({ok:false,message:'تعذر حفظ الرياضة. حاول مرة أخرى.'});}});
app.put('/api/sports/:id',async(req,res)=>{const db=await loadDatabase();const s=db.sports.find(x=>x.id===Number(req.params.id));if(!s)return res.status(404).json({message:'Sport not found'});const u=auth(req);if(!u)return res.status(401).json({message:'Login required'});if(u.role!=='admin'&&!u.canEdit)return res.status(403).json({message:'صلاحية مشاهدة فقط'});Object.assign(s,req.body);await saveDatabase(db);addActivity({userId:u.userId,username:u.username,name:u.name,action:'تعديل بيانات رياضة',sport:s.name});res.json(s);});
app.delete('/api/sports/:id',requireAdmin,async(req,res)=>{const db=await loadDatabase();const id=Number(req.params.id);db.sports=db.sports.filter(s=>s.id!==id);await saveDatabase(db);res.status(204).end();});
app.post('/api/sports/:id/players',async(req,res)=>{const u=auth(req);const db=await loadDatabase();const s=db.sports.find(x=>x.id===Number(req.params.id));if(!s)return res.status(404).json({message:'Sport not found'});if(!canEditSportForUser(u,s))return res.status(403).json({message:'هذه اللعبة غير متاحة للتعديل لهذا الحساب'});const allowedMembershipTypes=['عضويه عائليه','عضويه شرفيه','العضويه المؤقته (التى بها اقساط)'];
const membershipType=allowedMembershipTypes.includes(String(req.body.membershipType||''))?String(req.body.membershipType):'عضويه عائليه';
const p={id:Date.now(),name:req.body.name||'لاعب جديد',registrationDate:req.body.registrationDate||new Date().toISOString().slice(0,10),registrationNo:req.body.registrationNo||'N/A',membershipNumber:req.body.membershipNumber||'',nationalId:req.body.nationalId||'',membershipType,receiptNo:req.body.receiptNo||'N/A',age:Number(req.body.age||0),medicalHistory:req.body.medicalHistory||'سليم',isMember:req.body.isMember!==undefined?!!req.body.isMember:true,isPaid:req.body.isPaid!==undefined?!!req.body.isPaid:true,paidThroughMonth:req.body.paidThroughMonth||'',coach:req.body.coach||'',notes:req.body.notes||'',federation:req.body.federation||'',photo:req.body.photo||'',fileUrl:req.body.fileUrl||'',receiptImage:req.body.receiptImage||''};s.players.push(p);await saveDatabase(db);res.status(201).json(p);});
app.put('/api/sports/:sportId/players/:playerId',async(req,res)=>{const u=auth(req);const db=await loadDatabase();const s=db.sports.find(x=>x.id===Number(req.params.sportId));if(!s)return res.status(404).end();if(!canEditSportForUser(u,s))return res.status(403).json({message:'صلاحية تعديل مطلوبة لهذه اللعبة'});const p=s.players.find(x=>x.id===Number(req.params.playerId));if(!p)return res.status(404).end();Object.assign(p,req.body);await saveDatabase(db);addActivity({userId:u.userId,username:u.username,name:u.name,action:'تعديل لاعب',sport:s.name,details:p.name});res.json(p);});
app.delete('/api/sports/:sportId/players/:playerId',async(req,res)=>{const u=auth(req);const db=await loadDatabase();const s=db.sports.find(x=>x.id===Number(req.params.sportId));if(!s)return res.status(404).end();if(!canEditSportForUser(u,s))return res.status(403).json({message:'صلاحية تعديل مطلوبة لهذه اللعبة'});s.players=s.players.filter(p=>p.id!==Number(req.params.playerId));await saveDatabase(db);res.status(204).end();});
app.post('/api/sports/:id/players/import',requireAdmin,async(req,res)=>{const db=await loadDatabase();const s=db.sports.find(x=>x.id===Number(req.params.id));if(!s)return res.status(404).end();const rows=Array.isArray(req.body.players)?req.body.players:[];s.players.push(...rows.map((r,i)=>({...r,id:r.id||Date.now()+i,registrationDate:r.registrationDate||new Date().toISOString().slice(0,10)})));await saveDatabase(db);res.json({added:rows.length});});
app.post('/api/import-all',requireAdmin,async(req,res)=>{const db=await loadDatabase();db.sports=Array.isArray(req.body.sports)?req.body.sports:[];if(Array.isArray(req.body.federations))db.federations=req.body.federations;await saveDatabase(db);res.json({ok:true});});


const FEDERATION_SECRET=process.env.FEDERATION_SECRET||ADMIN_PASS;
function encryptSecret(value){
  if(!value)return '';
  const iv=crypto.randomBytes(12),key=crypto.createHash('sha256').update(FEDERATION_SECRET).digest();
  const cipher=crypto.createCipheriv('aes-256-gcm',key,iv);
  const enc=Buffer.concat([cipher.update(String(value),'utf8'),cipher.final()]);
  return [iv.toString('hex'),cipher.getAuthTag().toString('hex'),enc.toString('hex')].join('.');
}
function decryptSecret(value){
  try{
    if(!value)return '';
    const [ivHex,tagHex,dataHex]=String(value).split('.');
    const key=crypto.createHash('sha256').update(FEDERATION_SECRET).digest();
    const decipher=crypto.createDecipheriv('aes-256-gcm',key,Buffer.from(ivHex,'hex'));
    decipher.setAuthTag(Buffer.from(tagHex,'hex'));
    return Buffer.concat([decipher.update(Buffer.from(dataHex,'hex')),decipher.final()]).toString('utf8');
  }catch{return ''}
}
app.get('/api/federations',requireAdmin,async(req,res)=>res.json((await loadDatabase()).federations||[]));
app.post('/api/federations',requireAdmin,async(req,res)=>{const db=await loadDatabase();db.federations=db.federations||[];const f={id:uid(),name:req.body.name||'',sport:req.body.sport||'',createdAt:new Date().toISOString(),remoteUrl:req.body.remoteUrl||'',remoteUsername:req.body.remoteUsername||'',remotePasswordEncrypted:encryptSecret(req.body.remotePassword||'')};db.federations.push(f);await saveDatabase(db);res.status(201).json(f);});
app.put('/api/federations/:id',requireAdmin,async(req,res)=>{const db=await loadDatabase();const f=(db.federations||[]).find(x=>x.id===req.params.id);if(!f)return res.status(404).end();Object.assign(f,req.body);await saveDatabase(db);res.json(f);});
app.delete('/api/federations/:id',requireAdmin,async(req,res)=>{const db=await loadDatabase();const id=req.params.id;db.federations=(db.federations||[]).filter(x=>x.id!==id);for(const s of db.sports||[])for(const p of s.players||[])if(String(p.federationId)===String(id)){p.federation='';p.federationId='';}await saveDatabase(db);addActivity({userId:'admin',username:ADMIN_USER,name:ADMIN_USER,action:'حذف اتحاد',details:id});res.status(204).end();});
app.post('/api/federations/:id/remote-sync',requireAdmin,async(req,res)=>{
  try{
    const db=await loadDatabase(); const f=(db.federations||[]).find(x=>x.id===req.params.id);
    if(!f)return res.status(404).json({message:'الاتحاد غير موجود'});
    const url=String(req.body?.url||f.remoteUrl||'').trim();
    const username=String(req.body?.username||f.remoteUsername||'').trim();
    const password=String(req.body?.password||decryptSecret(f.remotePasswordEncrypted)||'');
    if(!url||!username||!password)return res.status(400).json({message:'أدخل رابط الاتحاد واسم الحساب وكلمة المرور'});
    f.remoteUrl=url;f.remoteUsername=username;
    if(req.body?.password)f.remotePasswordEncrypted=encryptSecret(password);
    const remoteSports=await remoteLoginAndFetchSports(url,username,password);
    const wantedSport=String(f.sport||'').trim();
    const sources=remoteSports.filter(x=>!wantedSport||String(x.name||'').trim()===wantedSport);
    let localSport=(db.sports||[]).find(x=>String(x.name||'').trim()===wantedSport);
    if(!localSport && sources[0]){localSport={id:Date.now()+Math.floor(Math.random()*100000),name:sources[0].name,technicalDirector:sources[0].technicalDirector||'',coaches:Array.isArray(sources[0].coaches)?sources[0].coaches:[],admins:[],formUrl:'',players:[],icon:sources[0].icon||'fa-solid fa-trophy'};db.sports.push(localSport);}
    const stats={added:0,updated:0,skipped:0};
    for(const source of sources){
      for(const raw of (source.players||[])){
        const rp=normalizeRemotePlayer(raw,source.name);
        if(!rp.name){stats.skipped++;continue;}
        const match=(localSport.players||[]).find(p=>(rp.membershipNumber&&String(p.membershipNumber||'')===rp.membershipNumber)||(rp.registrationNo&&rp.registrationNo!=='N/A'&&String(p.registrationNo||'')===rp.registrationNo)||String(p.name||'').trim().toLowerCase()===rp.name.toLowerCase());
        if(match){Object.assign(match,rp,{id:match.id,federation:f.name,federationId:f.id});stats.updated++;}
        else{localSport.players.push({...rp,federation:f.name,federationId:f.id});stats.added++;}
      }
    }
    await saveDatabase(db);
    res.json({ok:true,stats});
  }catch(e){res.status(502).json({ok:false,message:e.message||'فشل الاتصال بالاتحاد'});}
});
app.post('/api/federations/:id/players',requireAdmin,async(req,res)=>{const db=await loadDatabase();const f=(db.federations||[]).find(x=>x.id===req.params.id);if(!f)return res.status(404).json({message:'Federation not found'});const ids=new Set((req.body.playerIds||[]).map(Number));let updated=0;for(const s of db.sports||[]){for(const p of s.players||[]){if(p.federationId===f.id || p.federation===f.name){p.federation='';p.federationId='';}if(ids.has(Number(p.id))){p.federation=f.name;p.federationId=f.id;updated++;}}}await saveDatabase(db);addActivity({userId:'admin',username:ADMIN_USER,name:ADMIN_USER,action:'ربط لاعبين باتحاد',sport:f.sport,details:`${f.name}: ${updated} لاعب`});res.json({ok:true,updated});});

app.get('*',(req,res)=>{if(req.path.startsWith('/api/'))return res.status(404).end();res.sendFile(path.join(__dirname,'index.html'));});
server.listen(PORT,HOST,()=>{buildSubscriptionArchive().catch(()=>{});console.log(`Obour Youth Club running on ${PORT}`);for(const nets of Object.values(os.networkInterfaces()))for(const n of nets||[])if(n.family==='IPv4'&&!n.internal)console.log(`LAN: http://${n.address}:${PORT}`);});
