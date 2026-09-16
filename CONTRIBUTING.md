# 贡献指南

感谢贡献。提交前请先确认变更属于本仓库的职责边界：`cordis-core` 保持运行时内核，配置与扩展
实例编排放在 `cordis-host`，测试辅助放在 `cordis-testkit`。

## 提交变更

1. 从最新 `main` 创建主题分支。
2. 为行为变更补充或更新测试；修复并发、生命周期或释放逻辑时，测试应覆盖失败与取消路径。
3. 运行以下命令：

   ```bash
   cargo fmt --check
   cargo test --workspace
   cargo clippy --workspace --all-targets -- -D warnings
   ```

4. 提交 Pull Request，说明问题、设计取舍、验证结果及任何 API 或行为变化。

## 设计约束

- 不为未验证的兼容场景加入静默降级或双轨逻辑。
- 配置、类型和生命周期错误应明确失败。
- 插件的长期任务、监听器和清理资源必须由 Effect/Fiber 生命周期拥有。
- 公共 API 改动须同步更新 README、相关 `docs/` 和测试。

## 行为准则

请以尊重、建设性的方式讨论问题和代码。骚扰、歧视或人身攻击不被接受；维护者可移除不符合该原则的
内容或参与者。
