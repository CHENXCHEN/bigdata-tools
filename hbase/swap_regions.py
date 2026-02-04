#!/usr/bin/env python3
"""
HBase 智能 Region 平衡工具

自动分析 hot_table 的 region 分布，生成置换计划使其平衡，
同时尽可能让被置换的表也保持平衡。
先执行 echo "scan 'hbase:meta'" | hbase shell > meta_dump.txt

用法:
    python swap_regions.py meta_dump.txt --hot-table my_table
    python swap_regions.py meta_dump.txt --hot-table my_table --dry-run
    python swap_regions.py meta_dump.txt --hot-table my_table -o swap.rb
"""

import argparse
import re
from collections import defaultdict


def parse_args():
    parser = argparse.ArgumentParser(
        description="HBase 智能 Region 平衡工具 - 自动生成置换计划"
    )
    parser.add_argument("meta_file", help="hbase:meta dump 文件 (通过 scan 'hbase:meta' 生成)")
    parser.add_argument("--hot-table", required=True, help="需要平衡的热点表名")
    parser.add_argument("-o", "--output", default="move_plan.rb", help="输出文件 (默认: move_plan.rb)")
    parser.add_argument("--dry-run", action="store_true", help="只打印分析结果，不生成文件")
    return parser.parse_args()


def analyze_distribution(meta_file):
    """
    解析 hbase shell 的 'scan hbase:meta' 输出，统计分布情况。
    
    返回:
    - server_table_count: {server: {table: count}} 每个 server 上每个表的 region 数
    - region_info: {encoded_name: (table, server)} 每个 region 的信息
    - server_table_regions: {server: {table: [encoded_names]}} 每个 server 上每个表的 region 列表
    - server_fullname: {server_host: "host,port,startcode"} 完整 server name 映射
    """
    server_table_count = defaultdict(lambda: defaultdict(int))
    region_info = {}
    server_table_regions = defaultdict(lambda: defaultdict(list))
    server_fullname = {}  # host -> "host,port,startcode"
    
    # 匹配 info:server 行
    # 示例: search:search_dump_ads,,1638534531339.b9eab026abc3d8b05780fbd9fa7e5846. column=info:server, timestamp=1755674711320, value=aws-ir1-...:16020
    server_pattern = re.compile(
        r"^\s*(\S+),.*\.([a-f0-9]+)\.\s+column=info:server,\s*timestamp=\d+,\s*value=([^:]+):(\d+)"
    )
    
    # 匹配 info:serverstartcode 行
    # 示例: search:search_dump_ads,,1638534531339.b9eab026abc3d8b05780fbd9fa7e5846. column=info:serverstartcode, timestamp=1755674711320, value=1740626070375
    startcode_pattern = re.compile(
        r"^\s*(\S+),.*\.([a-f0-9]+)\.\s+column=info:serverstartcode,\s*timestamp=\d+,\s*value=(\d+)"
    )
    
    # 临时存储每个 region 的 server 信息
    region_server_info = {}  # encoded_name -> (table, host, port)
    
    with open(meta_file, 'r') as f:
        for line in f:
            # 解析 info:server 行
            match = server_pattern.search(line)
            if match:
                full_table_name = match.group(1)
                encoded_name = match.group(2)
                server_host = match.group(3)
                server_port = match.group(4)
                
                table_name = full_table_name.split(',')[0]
                
                if table_name.startswith('hbase:'):
                    continue
                
                region_server_info[encoded_name] = (table_name, server_host, server_port)
                continue
            
            # 解析 info:serverstartcode 行
            match = startcode_pattern.search(line)
            if match:
                encoded_name = match.group(2)
                startcode = match.group(3)
                
                if encoded_name in region_server_info:
                    table_name, server_host, server_port = region_server_info[encoded_name]
                    
                    # 构建完整 server name: host,port,startcode
                    full_server_name = f"{server_host},{server_port},{startcode}"
                    server_fullname[server_host] = full_server_name
                    
                    server_table_count[server_host][table_name] += 1
                    region_info[encoded_name] = (table_name, server_host)
                    server_table_regions[server_host][table_name].append(encoded_name)
    
    return server_table_count, region_info, server_table_regions, server_fullname


def compute_table_avg(table_name, server_table_count):
    """计算某个表在所有 server 上的平均 region 数"""
    total = sum(counts.get(table_name, 0) for counts in server_table_count.values())
    num_servers = len(server_table_count)
    return total / num_servers if num_servers > 0 else 0


def compute_balance_plan(hot_table, server_table_count, region_info, server_table_regions):
    """
    计算平衡置换计划。
    
    算法:
    1. 计算 hot_table 在各 server 的分布
    2. 识别 donor (region 过多) 和 receiver (region 过少)
    3. 贪心循环:
       - 每次从 donor 选一个 hot_region 移到 receiver
       - 从 receiver 选一个 "最不平衡" 的其他表 region 移到 donor
       - 立即更新状态
       - 重新评估，继续下一轮
    4. 返回 [(hot_region, cold_region, cold_table, source, target), ...]
    """
    swap_plan = []
    servers = list(server_table_count.keys())
    
    if not servers:
        return swap_plan
    
    num_servers = len(servers)
    
    # 深拷贝状态用于模拟
    stc = {s: dict(counts) for s, counts in server_table_count.items()}
    str_regions = {s: {t: list(regions) for t, regions in tables.items()} 
                   for s, tables in server_table_regions.items()}
    
    # 预计算所有表的平均值 (swap 不改变总数，所以 avg 不变)
    all_tables = set()
    for counts in server_table_count.values():
        all_tables.update(counts.keys())
    
    table_avg = {}
    for table in all_tables:
        total = sum(counts.get(table, 0) for counts in server_table_count.values())
        table_avg[table] = total / num_servers
    
    hot_avg = table_avg.get(hot_table, 0)
    
    print(f"\n计算置换计划中...")
    iteration = 0
    max_iterations = 1000  # 安全限制
    
    while iteration < max_iterations:
        iteration += 1
        
        # 找出 donor 和 receiver
        donors = []
        receivers = []
        
        for server in servers:
            count = stc[server].get(hot_table, 0)
            if count > hot_avg:
                donors.append((server, count - hot_avg))
            elif count < hot_avg:
                receivers.append((server, hot_avg - count))
        
        # 按差距排序
        donors.sort(key=lambda x: -x[1])
        receivers.sort(key=lambda x: -x[1])
        
        if not donors or not receivers:
            break
        
        # 尝试找到可行的 donor-receiver 配对
        found_swap = False
        
        for donor_server, _ in donors:
            if found_swap:
                break
                
            hot_regions_on_donor = str_regions.get(donor_server, {}).get(hot_table, [])
            if not hot_regions_on_donor:
                continue
            
            hot_region = hot_regions_on_donor[0]
            
            for receiver_server, _ in receivers:
                # 从 receiver 选择最不平衡的其他表 region
                best_cold_region = None
                best_cold_table = None
                best_score = float('-inf')
                
                for table, regions in str_regions.get(receiver_server, {}).items():
                    if table == hot_table or not regions:
                        continue
                    
                    t_avg = table_avg.get(table, 0)
                    t_count_on_receiver = stc[receiver_server].get(table, 0)
                    score = t_count_on_receiver - t_avg
                    
                    # 额外考虑: 移动后 donor 上该表是否会过多
                    t_count_on_donor = stc[donor_server].get(table, 0)
                    if t_count_on_donor + 1 > t_avg + 1:
                        score -= 0.5
                    
                    if score > best_score:
                        best_score = score
                        best_cold_table = table
                        best_cold_region = regions[0]
                
                if best_cold_region is not None:
                    # 找到可行的置换
                    swap_plan.append((
                        hot_region,
                        best_cold_region,
                        best_cold_table,
                        donor_server,
                        receiver_server
                    ))
                    
                    # 更新状态
                    stc[donor_server][hot_table] -= 1
                    stc[receiver_server][hot_table] = stc[receiver_server].get(hot_table, 0) + 1
                    str_regions[donor_server][hot_table].remove(hot_region)
                    if hot_table not in str_regions[receiver_server]:
                        str_regions[receiver_server][hot_table] = []
                    str_regions[receiver_server][hot_table].append(hot_region)
                    
                    stc[receiver_server][best_cold_table] -= 1
                    stc[donor_server][best_cold_table] = stc[donor_server].get(best_cold_table, 0) + 1
                    str_regions[receiver_server][best_cold_table].remove(best_cold_region)
                    if best_cold_table not in str_regions[donor_server]:
                        str_regions[donor_server][best_cold_table] = []
                    str_regions[donor_server][best_cold_table].append(best_cold_region)
                    
                    found_swap = True
                    
                    # 进度输出
                    if len(swap_plan) % 5 == 0:
                        print(f"  已计算 {len(swap_plan)} 对置换...")
                    break
        
        if not found_swap:
            # 所有 donor-receiver 配对都无法找到可置换的 region
            break
    
    print(f"  完成，共 {len(swap_plan)} 对置换")
    return swap_plan


def print_distribution(table_name, server_table_count, label="当前"):
    """打印某个表的分布情况"""
    avg = compute_table_avg(table_name, server_table_count)
    print(f"\n=== {label} {table_name} 分布 (avg={avg:.1f}) ===")
    
    items = []
    for server, counts in sorted(server_table_count.items()):
        count = counts.get(table_name, 0)
        diff = count - avg
        sign = "+" if diff > 0 else ""
        items.append((server, count, diff, sign))
    
    for server, count, diff, sign in sorted(items, key=lambda x: -x[1]):
        print(f"  {server}: {count} regions ({sign}{diff:.1f})")


def generate_plan(swap_plan, output_file, hot_table, server_fullname):
    """生成 HBase shell 移动计划文件"""
    with open(output_file, 'w') as f:
        f.write("# Auto-generated swap plan for balancing\n")
        f.write(f"# Hot table: {hot_table}\n")
        f.write(f"# Total swaps: {len(swap_plan)}\n\n")
        f.write("balance_switch false\n\n")
        
        for i, (hot_region, cold_region, cold_table, source, target) in enumerate(swap_plan, 1):
            # 获取完整的 server name (host,port,startcode)
            source_full = server_fullname.get(source, source)
            target_full = server_fullname.get(target, target)
            
            f.write(f"# Pair {i}: {hot_table} ({source} -> {target}), {cold_table} ({target} -> {source})\n")
            f.write(f"move '{hot_region}', '{target_full}'\n")
            f.write(f"move '{cold_region}', '{source_full}'\n")
            f.write("\n")
        
        f.write("# balance_switch true # Uncomment to enable after verification\n")
    
    print(f"\nDone! Plan saved to {output_file}")
    print(f"Run it with: hbase shell {output_file}")


def main():
    args = parse_args()
    
    print(f"Analyzing {args.meta_file}...")
    server_table_count, region_info, server_table_regions, server_fullname = analyze_distribution(args.meta_file)
    
    if not server_table_count:
        print("Error: No regions found in meta dump file")
        return 1
    
    hot_table = args.hot_table
    
    # 检查 hot_table 是否存在
    total_hot_regions = sum(
        counts.get(hot_table, 0) for counts in server_table_count.values()
    )
    if total_hot_regions == 0:
        print(f"Error: Table '{hot_table}' not found in meta dump")
        return 1
    
    print(f"\nFound {len(server_table_count)} servers, {len(region_info)} regions")
    print(f"Hot table '{hot_table}' has {total_hot_regions} regions")
    
    # 打印当前分布
    print_distribution(hot_table, server_table_count, "当前")
    
    # 计算置换计划
    swap_plan = compute_balance_plan(
        hot_table, server_table_count, region_info, server_table_regions
    )
    
    if not swap_plan:
        print(f"\n{hot_table} 已经平衡，无需置换")
        return 0
    
    # 打印置换计划
    print(f"\n=== 置换计划 ({len(swap_plan)} pairs) ===")
    
    # 统计被置换的表
    cold_table_stats = defaultdict(int)
    for _, _, cold_table, _, _ in swap_plan:
        cold_table_stats[cold_table] += 1
    
    for i, (hot_region, cold_region, cold_table, source, target) in enumerate(swap_plan, 1):
        print(f"  {i}. {hot_table}:{hot_region[:8]}... ({source} -> {target})")
        print(f"     置换: {cold_table}:{cold_region[:8]}... ({target} -> {source})")
    
    # 打印被置换表的统计
    print(f"\n=== 被置换表统计 ===")
    for table, count in sorted(cold_table_stats.items(), key=lambda x: -x[1]):
        print(f"  {table}: {count} regions")
    
    # 模拟执行后的分布
    print("\n=== 执行后预期分布 ===")
    # 创建模拟后的状态
    simulated_stc = {s: dict(counts) for s, counts in server_table_count.items()}
    for hot_region, cold_region, cold_table, source, target in swap_plan:
        simulated_stc[source][hot_table] -= 1
        simulated_stc[target][hot_table] = simulated_stc[target].get(hot_table, 0) + 1
        simulated_stc[target][cold_table] -= 1
        simulated_stc[source][cold_table] = simulated_stc[source].get(cold_table, 0) + 1
    
    # 打印 hot_table 执行后分布
    print_distribution(hot_table, simulated_stc, "执行后")
    
    # 打印被置换表的执行前后分布对比
    print("\n=== 被置换表分布变化 ===")
    for cold_tbl, swap_count in sorted(cold_table_stats.items(), key=lambda x: -x[1]):
        print(f"\n[{cold_tbl}] (置换 {swap_count} 个 region)")
        
        # 计算平均值
        total = sum(counts.get(cold_tbl, 0) for counts in server_table_count.values())
        avg = total / len(server_table_count) if server_table_count else 0
        
        # 打印每个 server 的前后对比
        for server in sorted(server_table_count.keys()):
            before = server_table_count[server].get(cold_tbl, 0)
            after = simulated_stc[server].get(cold_tbl, 0)
            diff_before = before - avg
            diff_after = after - avg
            sign_before = "+" if diff_before > 0 else ""
            sign_after = "+" if diff_after > 0 else ""
            
            # 只显示有变化的或不平衡的
            if before != after or abs(diff_after) > 0.5:
                change = ""
                if before != after:
                    change = f" -> {after} ({sign_after}{diff_after:.1f})"
                print(f"  {server}: {before} ({sign_before}{diff_before:.1f}){change}")
    
    if not args.dry_run:
        generate_plan(swap_plan, args.output, hot_table, server_fullname)
    else:
        print("\n[Dry run mode - no file generated]")
    
    return 0


if __name__ == "__main__":
    exit(main())
