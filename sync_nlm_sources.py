#!/usr/bin/env python3
"""
将 rclone Google Drive 路径中的 PDF 文件同步到 NotebookLM notebook source。

对比 rclone 远端目录中的 PDF 文件与 nlm notebook 已有 source，
自动将缺失的文件通过 Drive doc ID 添加到 notebook。

依赖: nlm CLI, rclone CLI

用法:
    # dry-run 仅查看差异
    python3 sync_nlm_sources.py NOTEBOOK_ID gdrive:path/to/folder -n

    # 执行同步
    python3 sync_nlm_sources.py NOTEBOOK_ID gdrive:path/to/folder

    # 指定 nlm profile，不递归，调整添加间隔为 3 秒
    python3 sync_nlm_sources.py NOTEBOOK_ID gdrive:path/to/folder -p myprofile --no-recursive --delay 3
"""

import argparse
import json
import re
import subprocess
import sys
import time


def run_cmd(cmd: list[str]) -> subprocess.CompletedProcess:
    """执行子进程命令，返回结果。"""
    return subprocess.run(cmd, capture_output=True, text=True)


def get_nlm_sources(notebook_id: str, profile: str | None = None) -> dict[str, str]:
    """获取 notebook 已有 source，返回 {drive_doc_id: title}。"""
    cmd = ["nlm", "source", "list", "--json", notebook_id]
    if profile:
        cmd.extend(["--profile", profile])

    result = run_cmd(cmd)
    if result.returncode != 0:
        print(f"[ERROR] nlm source list 失败: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)

    sources = json.loads(result.stdout)
    drive_ids: dict[str, str] = {}

    # 兼容 /file/d/ID/ 和 /document/d/ID/ 等格式
    pattern = re.compile(r"/(?:file|document|spreadsheets|presentation)/d/([^/]+)")
    for s in sources:
        url = s.get("url", "")
        m = pattern.search(url)
        if m:
            drive_ids[m.group(1)] = s.get("title", "")

    return drive_ids


def get_rclone_files(remote_path: str, recursive: bool = True) -> dict[str, str]:
    """获取 rclone 远端 PDF 文件列表，返回 {drive_doc_id: filename}。"""
    cmd = ["rclone", "lsjson", remote_path]
    if recursive:
        cmd.append("--recursive")

    result = run_cmd(cmd)
    if result.returncode != 0:
        print(f"[ERROR] rclone lsjson 失败: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)

    files = json.loads(result.stdout)
    drive_files: dict[str, str] = {}

    for f in files:
        if f.get("IsDir", False):
            continue
        name = f.get("Name", "")
        if not name.lower().endswith(".pdf"):
            continue
        doc_id = f.get("ID", "")
        if doc_id:
            drive_files[doc_id] = name

    return drive_files


def main():
    parser = argparse.ArgumentParser(
        description="将 rclone Drive 路径中的 PDF 同步到 NotebookLM notebook source"
    )
    parser.add_argument("notebook_id", help="NotebookLM notebook ID")
    parser.add_argument("remote_path", help="rclone remote:path (如 gdrive:path/to/folder)")
    parser.add_argument("-n", "--dry-run", action="store_true", help="仅打印差异，不执行添加")
    parser.add_argument("-p", "--profile", help="nlm profile 名称")
    parser.add_argument("--no-recursive", action="store_true", help="不递归列出子目录")
    parser.add_argument("--delay", type=float, default=2.0, help="每次添加间隔秒数 (默认 2)")

    args = parser.parse_args()

    # 1. 获取 rclone 文件列表
    print(f"[*] 获取 rclone 文件列表: {args.remote_path}")
    rclone_files = get_rclone_files(args.remote_path, recursive=not args.no_recursive)
    print(f"    找到 {len(rclone_files)} 个 PDF 文件")

    # 2. 获取 nlm 已有 source
    print(f"[*] 获取 nlm notebook source: {args.notebook_id}")
    nlm_ids = get_nlm_sources(args.notebook_id, args.profile)
    print(f"    已有 {len(nlm_ids)} 个 Drive source")

    # 3. 差集
    missing_ids = set(rclone_files.keys()) - set(nlm_ids.keys())
    already = len(rclone_files) - len(missing_ids)

    print(f"\n[*] 对比结果: {already} 个已存在, {len(missing_ids)} 个待添加")

    if not missing_ids:
        print("    全部已同步，无需操作。")
        return

    # 打印待添加列表
    for doc_id in sorted(missing_ids, key=lambda k: rclone_files[k]):
        print(f"    + {rclone_files[doc_id]}  ({doc_id})")

    if args.dry_run:
        print("\n[dry-run] 未执行任何添加操作。")
        return

    # 4. 逐个添加
    print()
    success = 0
    failed = 0
    for i, doc_id in enumerate(sorted(missing_ids, key=lambda k: rclone_files[k]), 1):
        name = rclone_files[doc_id]
        cmd = ["nlm", "source", "add", args.notebook_id, "--drive", doc_id, "--type", "pdf"]
        if args.profile:
            cmd.extend(["--profile", args.profile])

        print(f"[{i}/{len(missing_ids)}] 添加: {name} ...", end=" ", flush=True)
        result = run_cmd(cmd)
        if result.returncode != 0:
            print(f"FAIL\n    {result.stderr.strip()}")
            failed += 1
        else:
            print("OK")
            success += 1

        # 非最后一个时等待
        if i < len(missing_ids):
            time.sleep(args.delay)

    print(f"\n[*] 完成: {success} 成功, {failed} 失败")


if __name__ == "__main__":
    main()
