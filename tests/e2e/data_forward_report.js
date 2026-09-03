async page => {
  await page.setViewportSize({ width: 1365, height: 768 });
  await page.goto("http://localhost:8512");
  await page.getByRole("heading", { name: "报表数据日期前推验证" }).waitFor({ timeout: 120_000 });

  await page.getByText("当前模式开启", { exact: false }).waitFor();
  await page.getByText("页面显示时间 2026-09-02 08:30:00", { exact: false }).waitFor();
  await page.getByText("快照读取起点 2026-06-01", { exact: false }).waitFor();

  await page.getByRole("tab", { name: "直接查询型页面" }).click();
  await page.getByText("数据库查询开始 2026-08-29 00:00:00", { exact: false }).waitFor();
  await page.getByText("查询结果显示时间 2026-09-02 00:00:00", { exact: false }).waitFor();
  await page.screenshot({ path: "output/test-results/data-forward/enabled.png", fullPage: true });

  await page.getByRole("switch", { name: "启用日期前推" }).click({ force: true });
  await page.getByText("当前模式关闭", { exact: false }).waitFor();
  await page.getByText("数据库查询开始 2026-09-02 00:00:00", { exact: false }).waitFor();
  await page.getByText("查询结果显示时间 2026-09-02 00:00:00", { exact: false }).waitFor();
  await page.getByRole("tab", { name: "快照型页面" }).click();
  await page.getByText("页面显示时间 2026-08-29 08:30:00", { exact: false }).waitFor();
  await page.getByText("快照读取起点 2026-06-01", { exact: false }).waitFor();
  await page.screenshot({ path: "output/test-results/data-forward/disabled.png", fullPage: true });

  return "数据前推 E2E 通过：快照型、直接查询型、关闭回归均符合预期";
}
