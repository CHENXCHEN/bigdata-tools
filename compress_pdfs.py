#!/usr/bin/env python3
"""批量压缩 PDF 文件（使用 Ghostscript）"""

import argparse
import shutil
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path


def compress_one(pdf: Path, output_dir: Path) -> str:
    output = output_dir / f"{pdf.stem}_compressed.pdf"
    if output.exists():
        return f"Skipped: {pdf.name} (already exists)"
    cmd = [
        "gs",
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.4",
        "-dPDFSETTINGS=/ebook",
        "-dNOPAUSE",
        "-dQUIET",
        "-dBATCH",
        f"-sOutputFile={output}",
        str(pdf),
    ]
    subprocess.run(cmd, check=True)
    return f"Done: {pdf.name} -> {output.name}"


def main():
    parser = argparse.ArgumentParser(description="批量压缩 PDF 文件")
    parser.add_argument("input_dir", type=Path, help="输入文件夹（绝对路径）")
    parser.add_argument("output_dir", type=Path, help="输出文件夹（绝对路径）")
    parser.add_argument("-j", "--jobs", type=int, default=6, help="并发数（默认 6）")
    args = parser.parse_args()

    if not args.input_dir.is_dir():
        print(f"Error: Input directory '{args.input_dir}' does not exist.", file=sys.stderr)
        sys.exit(1)

    if not shutil.which("gs"):
        print("Error: gs (Ghostscript) is not installed.", file=sys.stderr)
        sys.exit(1)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    pdfs = sorted(args.input_dir.glob("*.pdf"))
    if not pdfs:
        print("No PDF files found.")
        return

    print(f"Found {len(pdfs)} PDF(s), compressing with {args.jobs} workers...")

    failed = 0
    with ProcessPoolExecutor(max_workers=args.jobs) as pool:
        futures = {pool.submit(compress_one, pdf, args.output_dir): pdf for pdf in pdfs}
        for future in as_completed(futures):
            pdf = futures[future]
            try:
                print(future.result())
            except subprocess.CalledProcessError as e:
                failed += 1
                print(f"FAILED: {pdf.name} (exit code {e.returncode})", file=sys.stderr)

    print(f"\nAll done. Success: {len(pdfs) - failed}, Failed: {failed}")


if __name__ == "__main__":
    main()
