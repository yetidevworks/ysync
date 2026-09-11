#!/usr/bin/env python3
"""Benchmark an existing ysync binary over SSH, with isolated disk-backed test roots."""
import argparse
import hashlib
import json
from pathlib import Path
import os
import shlex
import subprocess
import tempfile
import time


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--binary",required=True)
    p.add_argument("--ssh",required=True)
    p.add_argument("--remote-binary",required=True)
    p.add_argument("--files",type=int,default=5000)
    p.add_argument("--size",type=int,default=4096)
    p.add_argument("--scan-workers",type=int,default=8)
    p.add_argument("--output",type=Path)
    p.add_argument("--delta",action="store_true",help="Measure overwrite, insertion, and deletion of the first large file")
    args=p.parse_args()
    if args.files<1 or args.size<1 or not 1<=args.scan_workers<=64:p.error("invalid benchmark size or worker count")
    if args.delta and args.size<2*1024*1024:p.error("--delta requires --size of at least 2 MiB")
    binary=str(Path(args.binary).resolve())
    def ssh(*cmd,input=None):
        return subprocess.check_output(["ssh","-o","BatchMode=yes",args.ssh,shlex.join(cmd)],input=input,text=True)
    def py(code):return ssh("python3","-",input=code)
    remote=py("import tempfile,os;print(tempfile.mkdtemp(prefix='ysync-benchmark-',dir=os.path.expanduser('~/.cache')))\n").strip()
    rs=remote+"/state";rf=remote+"/files"
    remote_proc=None;local_proc=None
    with tempfile.TemporaryDirectory(prefix="ysync-lan-") as tmp:
        base=Path(tmp);state=base/"state";files=base/"files";files.mkdir()
        def cli(*cmd):return subprocess.check_output([binary,"--home",str(state),*cmd],text=True)
        def rcli(*cmd):return ssh(args.remote_binary,"--home",rs,*cmd)
        try:
            port=int(py("import socket\nwith socket.socket() as s:\n s.bind(('0.0.0.0',0));print(s.getsockname()[1])\n").strip())
            cli("init","--name","benchmark-mac","--listen","127.0.0.1:0","--scan-workers",str(args.scan_workers))
            cli("folder","add","bench",str(files))
            aid=cli("id").strip()
            ssh("mkdir","-p",rf)
            rcli("init","--name","benchmark-linux","--listen",f"0.0.0.0:{port}","--scan-workers",str(args.scan_workers))
            rcli("folder","add","bench",rf)
            bid=rcli("id").strip()
            # Resolve SSH aliases using OpenSSH's evaluated configuration.
            settings=subprocess.check_output(["ssh","-G",args.ssh],text=True,stderr=subprocess.DEVNULL)
            host=next(s.split(" ",1)[1] for s in settings.splitlines() if s.startswith("hostname "))
            cli("peer","add",bid,"--address",f"{host}:{port}","--folder","bench")
            peer=dict(id=aid,name="benchmark-mac",address=None,approved=True,folders=["bench"])
            py(f"import json,pathlib\np=pathlib.Path({rs!r})/'config.json'\nc=json.loads(p.read_text());c['peers'].append(json.loads({json.dumps(peer)!r}));c['rescan_secs']=5;p.write_text(json.dumps(c))\n")
            payload=os.urandom(args.size)
            for i in range(args.files):
                d=files/f"d{i//1000:05}";d.mkdir(exist_ok=True);(d/f"f{i:08}.bin").write_bytes(payload)
            started=time.monotonic()
            with (base/"local.log").open("w") as log:
                remote_proc=subprocess.Popen(["ssh","-o","BatchMode=yes",args.ssh,shlex.join([args.remote_binary,"--home",rs,"serve"])],stdout=subprocess.DEVNULL,stderr=(base/"remote.log").open("w"))
                local_proc=subprocess.Popen([binary,"--home",str(state),"serve"],stdout=log,stderr=log)
                last=0
                while True:
                    if remote_proc.poll() is not None or local_proc.poll() is not None:raise RuntimeError("a daemon exited")
                    count=int(py(f"import sqlite3\nwith sqlite3.connect('file:'+{rs!r}+'/index.sqlite?mode=ro',uri=True) as c:\n print(c.execute(\"SELECT count(*) FROM entries WHERE json_extract(data,'$.kind')='File'\").fetchone()[0])\n"))
                    elapsed=time.monotonic()-started
                    if count>=args.files:break
                    if elapsed-last>=5:print(f"{elapsed:.1f}s: {count:,}/{args.files:,} committed files",flush=True);last=elapsed
                    if elapsed>600:raise TimeoutError("bootstrap exceeded 10 minutes")
                    time.sleep(.2)
                seconds=time.monotonic()-started
                # Hash every received file on the server, including shape/count checks.
                digest=hashlib.sha256(payload).hexdigest()
                check=py(f"import pathlib,hashlib\nr=pathlib.Path({rf!r});n={args.files}\nfor i in range(n):\n p=r/f'd{{i//1000:05}}'/f'f{{i:08}}.bin'\n assert hashlib.sha256(p.read_bytes()).hexdigest()=={digest!r},p\nprint(n)\n")
                assert int(check)==args.files
                delta_results=[]
                if args.delta:
                    target=files/'d00000'/'f00000000.bin'
                    remote_target=rf+'/d00000/f00000000.bin'
                    deadline=time.monotonic()+30
                    while True:
                        before=json.loads(rcli('status','--json'))
                        if before['received_bytes']==args.files*args.size:break
                        if time.monotonic()>deadline:raise TimeoutError('baseline counters')
                        time.sleep(.2)
                    data=bytearray(payload)
                    for change in ['4 KiB overwrite','19 byte insertion','4096 byte deletion']:
                        if change=='4 KiB overwrite':data[len(data)//2:len(data)//2+4096]=bytes([0xa1])*4096
                        elif change=='19 byte insertion':data[1234:1234]=b'unaligned insertion'
                        else:del data[len(data)//3:len(data)//3+4096]
                        delta_start=time.monotonic()
                        target.write_bytes(data)
                        expected=hashlib.sha256(data).hexdigest()
                        while True:
                            matches=py(f"import pathlib,hashlib\np=pathlib.Path({remote_target!r});print(hashlib.sha256(p.read_bytes()).hexdigest()=={expected!r})\n").strip()=='True'
                            after=json.loads(rcli('status','--json'))
                            received=after['received_bytes']-before['received_bytes']
                            reused=after.get('delta_reused_bytes',0)-before.get('delta_reused_bytes',0)
                            if matches and received+reused==len(data):break
                            if time.monotonic()-delta_start>120:raise TimeoutError('delta edit convergence')
                            time.sleep(.2)
                        assert received<len(data)//4, 'delta did not substantially reduce payload'
                        delta_results.append(dict(change=change,file_bytes=len(data),payload_bytes=received,delta_reused_bytes=reused,seconds_including_ssh_probes_and_status_refresh=round(time.monotonic()-delta_start,3),sha256_verified=True))
                        before=after
                edit_start=time.monotonic()
                py(f"import pathlib\n(pathlib.Path({rf!r})/'reverse-probe.txt').write_text('linux to mac')\n")
                while not (files/'reverse-probe.txt').exists():
                    if time.monotonic()-edit_start>30:raise TimeoutError("reverse watcher exceeded 30s")
                    time.sleep(.01)
                assert (files/'reverse-probe.txt').read_text()=='linux to mac'
                result=dict(files=args.files,bytes=args.files*args.size,scan_workers=args.scan_workers,
                            bootstrap_seconds=round(seconds,3),files_per_second=round(args.files/seconds,1),
                            payload_MB_per_second=round(args.files*args.size/seconds/1e6,2),
                            reverse_edit_latency_ms=round((time.monotonic()-edit_start)*1000,1),
                            verified_all_payloads=True,environment=f"Mac to {args.ssh}; remote ~/.cache disk-backed roots; existing workloads left running",remote_reconciliation_secs=5)
                if args.delta:result['delta_edits']=delta_results
                print(json.dumps(result,indent=2))
                if args.output:args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(result,indent=2)+"\n")
        except Exception:
            for name in ['local.log','remote.log']:
                log=base/name
                if log.exists():print(log.read_text()[-5000:])
            try:print(rcli("status","--json"))
            except subprocess.CalledProcessError:pass
            raise
        finally:
            if local_proc:
                local_proc.terminate()
                try:local_proc.wait(timeout=5)
                except subprocess.TimeoutExpired:local_proc.kill();local_proc.wait()
            # Stop only the PID whose command line contains this test's exact state path.
            py(f"import pathlib,json,os,signal,shutil,time\nr=pathlib.Path({remote!r});s=r/'state/status.json'\nif s.exists():\n pid=json.loads(s.read_text()).get('pid',0)\n cmd=pathlib.Path(f'/proc/{{pid}}/cmdline')\n if pid>0 and cmd.exists() and {rs!r}.encode() in cmd.read_bytes().split(b'\\0'):os.kill(pid,signal.SIGTERM)\ntime.sleep(.5)\nshutil.rmtree(r)\n")
            if remote_proc:
                try:remote_proc.wait(timeout=5)
                except subprocess.TimeoutExpired:remote_proc.terminate();remote_proc.wait(timeout=5)


if __name__=='__main__':main()
