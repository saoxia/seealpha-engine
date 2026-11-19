# 多腿组合阶段核对日志（研究态；不具备下单权）

关联 Platform #107。代码：engine/strategies/basket_reconciliation.py。
测试：tests/engine/basket_reconciliation_test.py。

## 功能范围

这是通用于现有 BasketPlan 的纯内存、可重放状态规约，供未来受控多产品协调器接入。
它不是新的交易命令、订单状态数据库、交易所适配器或生产风控授权。

1. new_checkpoint(plan, starting_positions)：验证所有腿、阶段增量总和与目标一致，且计划为 executable=False。
2. begin_phase(checkpoint, request_id)：以唯一请求标识登记阶段，不提交订单；重复请求保持幂等。
3. record_cumulative_fill：只接收可信执行器验证后的已成交累计值，拒绝跨腿、反向、超量、倒退、ID 冲突及重复计数。
4. confirm_phase：同时验证阶段每条腿最终 FILLED 状态、完整累计成交数量及权威账户仓位逐腿完全一致，才允许进入下一阶段。期权保护先完成 PUT/CALL 才能进入永续增仓；Spot/UM 必须同时核对两腿，但不保证交易所原子成交。
5. freeze_checkpoint：遇到超时、拒单、未知订单、腿偏差或网络异常应冻结；冻结不能自动解冻。仅可重新查询权威状态、明确清理未决命令后由上层重新规划。
6. checkpoint_to_record / restore_checkpoint：将事件日志序列化为 JSON 并按计划指纹重放。重启未完成阶段仍为 awaiting_confirmation，不自动发单。计划指纹不是持久化数据的密码学签名。

## 生产接线前必须完成

- 目前没有任何自动订单出口，不调用 NautilusTrader、交易所 HTTP 或 Platform API，也不提升生产权限。
- 上层要在任何真实订单副作用之前，原子写入持久化命令、客户端订单号和阶段状态，保存交易所订单号及成交、资金流水和异常审计；本规约不会自行证明外部证据可信。
- 每进入新的执行阶段，重新验证最新行情、期权覆盖与到期、保证金、资金预算、费用、权限和交易所风控。核对历史成交不代替当前风险检查。
- 重启后先只读核对全部未决订单及账户仓位，未知结果绝不凭“查询不到”盲目重发。真实跨产品的部分成交对冲、超时补偿、可靠冻结报警和长时间运行仍未完成。
- 接入 Engine 运行时、Platform 只读状态 API、生产权限及真实交易所沙盒，应在单独 PR 完成；不得未经明确授权启用真实交易。
- 本次仅有纯离线自动化测试，不等于已通过生产实盘或真实沙盒验收。

状态：ready → awaiting_confirmation → ready（下一阶段）→ completed；遇异常转 frozen，不允许继续。
