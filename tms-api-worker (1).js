// MacroLogix TMS — Cloudflare Worker API v2
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,PUT,PATCH,DELETE,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type,Authorization,apikey,Prefer',
  'Content-Type': 'application/json'
};

function ok(data, status=200){ return new Response(JSON.stringify(data), {status, headers: CORS}); }
function fail(msg, status=400){ return new Response(JSON.stringify({error:msg}), {status, headers: CORS}); }

function parseQuery(url){
  const p = new URL(url).searchParams;
  const where=[], vals=[];
  let order=null, limit=null;
  for(const [k,v] of p.entries()){
    if(k==='order'){ const[c,d]=v.split('.'); order=`ORDER BY "${c}" ${(d||'asc').toUpperCase()}`; }
    else if(k==='limit'){ limit=parseInt(v); }
    else if(k==='select'){} 
    else {
      const dot=v.indexOf('.');
      if(dot>-1){
        const op=v.slice(0,dot), val=v.slice(dot+1);
        if(op==='eq')   { where.push(`"${k}"=?`);       vals.push(val); }
        if(op==='ilike'){ where.push(`"${k}" LIKE ?`);  vals.push(val.replace(/\*/g,'%')); }
        if(op==='gt')   { where.push(`"${k}">?`);       vals.push(val); }
        if(op==='lt')   { where.push(`"${k}"<?`);       vals.push(val); }
        if(op==='gte')  { where.push(`"${k}">=?`);      vals.push(val); }
        if(op==='neq')  { where.push(`"${k}"!=?`);      vals.push(val); }
      }
    }
  }
  return {where, vals, order, limit};
}

function hydrate(row){
  const JSONCOLS=['bill_to','shipper','receiver','stops','segments','types','timeline'];
  const out={...row};
  for(const c of JSONCOLS){
    if(out[c] && typeof out[c]==='string'){
      try{ out[c]=JSON.parse(out[c]); }catch(e){}
    }
  }
  return out;
}

function dehydrate(row){
  const out={};
  for(const [k,v] of Object.entries(row)){
    out[k]=(v!==null && typeof v==='object') ? JSON.stringify(v) : v;
  }
  return out;
}

async function upsert(db, table, row){
  row.updated_at = new Date().toISOString();
  if(!row.created_at) row.created_at = row.updated_at;
  const flat = dehydrate(row);
  const cols = Object.keys(flat);
  const vals = Object.values(flat);
  const ph   = cols.map(()=>'?').join(',');
  const upd  = cols.filter(c=>c!=='id').map(c=>`"${c}"=excluded."${c}"`).join(',');
  const sql  = `INSERT INTO "${table}" (${cols.map(c=>`"${c}"`).join(',')}) VALUES (${ph})
                ON CONFLICT(id) DO UPDATE SET ${upd}`;
  await db.prepare(sql).bind(...vals).run();
  return flat;
}

const TABLES = [
  'ml_loads','ml_drivers','ml_customers','ml_trailers','ml_trucks',
  'ml_load_events','ml_ltl_routes','ml_ltl_pickups'
];

export default {
  async fetch(req, env){
    if(req.method==='OPTIONS') return new Response(null,{status:204,headers:CORS});
    const url = req.url;
    const m   = url.match(/\/api\/(ml_\w+)/);
    if(!m) return fail('Not found',404);
    const table = m[1];
    if(!TABLES.includes(table)) return fail('Table not allowed',403);
    const db = env.DB;
    try {
      if(req.method==='GET'){
        const {where,vals,order,limit} = parseQuery(url);
        let sql = `SELECT * FROM "${table}"`;
        if(where.length) sql+=` WHERE ${where.join(' AND ')}`;
        if(order) sql+=` ${order}`;
        if(limit) sql+=` LIMIT ${limit}`;
        const res = await db.prepare(sql).bind(...vals).all();
        return ok((res.results||[]).map(hydrate));
      }
      if(req.method==='POST'){
        const body = await req.json();
        if(Array.isArray(body)){
          const out=[];
          for(const row of body) out.push(await upsert(db,table,row));
          return ok(out,201);
        }
        return ok(await upsert(db,table,body),201);
      }
      if(req.method==='PATCH'){
        const body = await req.json();
        const {where,vals} = parseQuery(url);
        if(!where.length) return fail('PATCH requires filter');
        body.updated_at = new Date().toISOString();
        const flat = dehydrate(body);
        const set  = Object.keys(flat).map(c=>`"${c}"=?`).join(',');
        const sv   = Object.values(flat);
        await db.prepare(`UPDATE "${table}" SET ${set} WHERE ${where.join(' AND ')}`).bind(...sv,...vals).run();
        return ok({success:true});
      }
      if(req.method==='DELETE'){
        const {where,vals} = parseQuery(url);
        if(!where.length) return fail('DELETE requires filter');
        await db.prepare(`DELETE FROM "${table}" WHERE ${where.join(' AND ')}`).bind(...vals).run();
        return ok({success:true});
      }
      return fail('Method not allowed',405);
    } catch(e){
      console.error(e);
      return fail(e.message,500);
    }
  }
};
