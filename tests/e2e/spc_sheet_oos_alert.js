async page => {
  // SPC 单片异常预警中心 E2E：预警 Expander 存在性 + 状态/图像联动
  await page.goto("http://localhost:8503/SPC监控报表");
  await page
    .getByText("正在加载 SPC 分布数据...")
    .waitFor({ state: "hidden", timeout: 600_000 });
  await page
    .getByRole("heading", { name: "筛选", exact: true })
    .waitFor({ timeout: 300_000 });

  const statusRegex =
    /检测到 \d+ 条单片异常预警|上一周未发现已确认真实超规|当前产品暂无单片异常明细数据/;

  // 单片异常预警中心 Expander 必须存在（标题含上一周 ISO 标签）
  const centerSummary = page
    .locator("summary")
    .filter({ hasText: "单片异常预警中心（上一周" });
  await centerSummary.waitFor({ timeout: 120_000 });

  // 折叠的 Expander 内容不在 DOM 中：无警时需点击展开。
  // 用 eval 点击绕过 locator actionability（rerun 替换 DOM 时 locator 点击会静默失败）；
  // rerun 可能重置折叠态，重试至多 5 次。
  let statusVisible = (await page.getByText(statusRegex).count()) > 0;
  for (let attempt = 0; attempt < 5 && !statusVisible; attempt += 1) {
    await page.evaluate(() => {
      const s = [...document.querySelectorAll("summary")].find(x =>
        x.innerText.includes("单片异常预警中心"),
      );
      if (s) s.click();
    });
    statusVisible = await page
      .waitForFunction(
        re => new RegExp(re).test(document.body.innerText),
        statusRegex.source,
        { timeout: 60_000 },
      )
      .then(() => true)
      .catch(() => false);
  }
  if (!statusVisible) {
    throw new Error("单片异常预警中心未渲染任何状态文案（警报/正常/无数据）。");
  }

  const alertMessage = page.getByText(/检测到 \d+ 条单片异常预警/);
  const hasAlerts = (await alertMessage.count()) > 0;
  const imageExpander = page.getByText(/🚨 单片异常预警指标图像（\d+ 个指标）/);
  if (hasAlerts) {
    await imageExpander.first().waitFor({ timeout: 300_000 });
  } else if ((await imageExpander.count()) !== 0) {
    throw new Error("无单片异常预警时仍渲染了预警指标图像。");
  }

  // CPK 预警回归：CPK预警中心 Expander 仍在
  await page
    .locator("summary")
    .filter({ hasText: "CPK预警中心" })
    .first()
    .waitFor({ timeout: 60_000 });

  await page.screenshot({
    path: "output/test-results/spc_sheet_oos_alert_e2e.png",
    fullPage: false,
  });

  return {
    hasAlerts,
    status: hasAlerts
      ? await alertMessage.first().innerText()
      : "no-alert state rendered",
  };
}
