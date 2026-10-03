"""Summarise exercise sessions: tool sequence, every note written, each answer.

usage: analyze.py <out-dir> [session-name ...]   (out-dir is .csq/ex-<label>)
"""
import json, glob, os, sys
if len(sys.argv) < 2:
    sys.exit(__doc__)
ex = sys.argv[1]
only = sys.argv[2:]
for f in sorted(glob.glob(ex+'/sessions/*.jsonl')):
    sid=None; rows=[json.loads(l) for l in open(f)]
    sid=rows[0]['session']
    if only and sid not in only: continue
    print('='*100); print('SESSION',sid, os.path.basename(f))
    for r in rows:
        t=r['type']
        if t=='turn':
            print(f"  TURN prompt: {r['prompt'][:140]!r}")
            print(f"       response: {r['response'][:600]!r}")
            print(f"       steps={r.get('steps')} tools={r.get('tool_calls')} presented={r.get('presented')} ms={r.get('duration_ms')} err={r.get('error')}")
        elif t=='tool_call':
            tool=r['tool']; inp=r.get('input','')
            try: i=json.loads(inp)
            except Exception: i={}
            if tool.startswith('scratch_'):
                print(f"  {tool:15s} scope={i.get('scope','session'):7s} key={i.get('key','')}  {('ERR '+r['error'][:80]) if r.get('error') else ''}")
                if tool in('scratch_set','scratch_append'):
                    for ln in (i.get('value') or '').split('\n'): print('        |',ln[:200])
            elif tool in('query_sql','present_table','present_chart'):
                print(f"  {tool:15s} rows={r.get('rows')} {('ERR '+r['error'][:200]) if r.get('error') else ''}")
                print('        sql:',(i.get('sql') or '').replace('\n',' ')[:260])
            else:
                print(f"  {tool:15s} {inp[:110]} {('ERR '+r['error'][:100]) if r.get('error') else ''}")
