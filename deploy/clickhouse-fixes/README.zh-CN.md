> **语言：** [English](README.md) | 简体中文

# ClickHouse 修复脚本

存放对**已部署**的 ClickHouse 做修复的脚本。`../clickhouse/` 是“新装时怎么建”，这里是“已经建错了怎么改回来”。新增修复时在本目录继续追加编号脚本，并更新下面的索引。

## 索引

| 脚本 | 修复什么 | 触发条件 |
|---|---|---|
| [`001_fix_kline_rollup_lookback.sh`](001_fix_kline_rollup_lookback.sh) | `fapi_kline_{5m,15m,1h,4h,1d}` 里落在 MV 回看窗口之外的行只包含桶的一部分数据（例如某小时的成交量等于该小时最后一根 1m 的成交量） | 曾使用过旧版 002（`WHERE start_time >= now() - INTERVAL N DAY`，下界没有对齐到桶边界）。**不带参数 `apply` = 删除全部 5m+ 衍生表和 MV 后，从 1m 完全重建整个历史** |

公共的连接配置与函数在 [`lib.sh`](lib.sh)。

## 用法

```bash
export CH_HOST=127.0.0.1 CH_PORT=9000 CH_USER=default CH_PASSWORD=...   # CH_DB 默认 market

# 1. 只读检查：MV 是否还是旧版；汇总表与 1m 重新聚合是否一致（默认最近 10 天）
./001_fix_kline_rollup_lookback.sh check

# 2. 先在演练库里走一遍（建议）：脚本会把 SQL 里的 `market.` 前缀改写成 $CH_DB
CH_DB=market_rehearsal ./001_fix_kline_rollup_lookback.sh apply --yes

# 3. 正式修复：不带时间段 = 完全重建（会要求手动输入库名确认）
./001_fix_kline_rollup_lookback.sh apply

# 4. 局部重算 / 断点续跑：只重建 MV、保留现有表，重算指定月份
./001_fix_kline_rollup_lookback.sh apply --from 2026-08 [--to 2026-10]
```

**完全重建**（`apply` 不带 `--from/--to`）做的事：

1. 应用 [`../clickhouse/002_kline_rollups.sql`](../clickhouse/002_kline_rollups.sql)，它开头的 `REBUILD-DROPS` 块会删除 5 个可刷新 MV 和 5 个衍生表（5m/15m/1h/4h/1d），再空表重建。**`fapi_kline_1m` 不会被动。**
2. 按月重跑 003，用整个 1m 历史把所有衍生表重新灌满。
3. 校验汇总表与 1m 重新聚合的结果一致。

重建期间 5m 及以上的表是空的或只填了一部分，读这些表的下游会读到不完整的数据。**不需要停止 chomosyncer-go**（它只写 1m），但停掉也无妨。中途被打断时，用 `apply --from <最后一个 chunk 所在的月份>` 继续（该模式不删表）。

## 必须先理解的 ClickHouse 物化视图（MV）特性

本仓库的汇总层用的是**可刷新物化视图（refreshable MV）**：`REFRESH EVERY N SECOND APPEND TO <目标表> AS SELECT ...`。

1. **MV 只是一条保存下来的 SELECT，数据在目标表里。** 用 `TO` / `APPEND TO` 建的 MV，`DROP VIEW` 只删除这条查询，**目标表和里面的数据保留**（001 的演练里验证过：删除并重建 MV 后，目标表里预先放的标记行仍在）。注意：不带 `TO` 的 MV 使用隐式内表，`DROP` 时数据会一起删掉，本仓库不使用这种形式。
2. **`CREATE MATERIALIZED VIEW IF NOT EXISTS` 遇到同名 MV 什么都不做。** 改了 `.sql` 文件里的查询，再重新执行，线上的 MV 仍然是旧查询。**修改 MV 的标准做法是先 `DROP VIEW`、再 `CREATE`**。`ALTER TABLE ... MODIFY QUERY` 是否可用取决于 MV 类型与版本（compose 里的镜像是 24.8，而刷新 MV 的 APPEND 至少要 24.10），为了可移植，统一使用删除后重建。
3. **MV 只“向前”工作，不会回头修历史。** 普通（增量）MV 只处理新插入的块；可刷新 MV 每次只重算它 `WHERE` 覆盖的范围。所以**替换 MV 之后，历史行仍是旧的（错的）**，必须再跑一次回填（003）把历史行重算并覆盖。
4. **目标表是 `ReplacingMergeTree(rollup_version)`，“修复”就是写入更新版本的行。** 同一个 `(symbol, start_time)` 版本号最大的行胜出，读取一律加 `FINAL`。所以回填可以反复执行；被取代的旧行在后台合并时才会真正清理，需要立刻回收空间时再 `OPTIMIZE TABLE ... FINAL`。
5. **修复顺序：先删旧 MV → 建新 MV → 回填 → 校验。** 如果先回填再换 MV，旧 MV 还在，随着回看下界继续滑动，它会把刚修好的桶再次污染。
6. **`DROP` 到 `CREATE` 之间会有一个短暂的空窗**（新 1m 不会被汇总）。新 MV 第一次刷新会覆盖整个回看窗口（3–10 天），空窗产生的缺口会自动补上。
7. **时区。** `now()` 带的是服务器时区。与 UTC 类型的时间列比较或按 `toStartOfInterval(..., INTERVAL 1 DAY)` 对齐时，要写 `toTimeZone(now(), 'UTC')`，否则日桶会在服务器本地零点被切开。

## 新增一个修复脚本的约定

1. 文件名 `NNN_fix_<主题>.sh`（或 `.sql`），编号递增，并在上面的索引里登记。
2. 文件头写清四件事：**症状、原因、脚本做什么（按顺序）、用法**。
3. 至少提供只读的 `check`（失败时以非 0 退出）和会改动数据的 `apply`；`apply` 结束时自动调用 `check` 验证。
4. 修改 MV 一律“先 DROP 再 CREATE”，并先删、再建、再回填。
5. 破坏性步骤前要有确认（输入库名，或 `--yes`），所有 SQL 走 `lib.sh` 的 `ch` / `ch_file`，以支持 `CH_DB` 演练。
6. 尽量幂等；不要写死密码。
7. 先在演练库里跑通，再动生产库。
