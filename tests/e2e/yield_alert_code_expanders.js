async page => {
  // 入库不良率分析看板：预警中心回归 + 自动预警缺陷图像 Expander
  await page.goto("http://localhost:8503/入库不良率分析看板?admin=true");
  await page
    .getByText(/正在加载|正在分析|加载中/)
    .first()
    .waitFor({ state: "hidden", timeout: 600_000 })
    .catch(() => {});

  await page.getByRole("button", { name: "🔄 刷新缓存" }).click();
  await page
    .getByText(/正在加载|正在分析|加载中/)
    .first()
    .waitFor({ state: "hidden", timeout: 600_000 })
    .catch(() => {});

  // 既有智能预警中心必须仍在（回归）
  const alertCenter = page
    .locator("summary")
    .filter({ hasText: "智能预警中心" });
  await alertCenter.first().waitFor({ timeout: 300_000 });

  // 自动预警缺陷图像：有命中 Code 时存在，无命中时不渲染
  await page.waitForFunction(
    () => document.body.innerText.length > 0,
    null,
    { timeout: 60_000 },
  );
  const defectImageExpander = page.getByText(/🚨 自动预警缺陷图像（\d+ 个 Code）/);
  const defectImageCount = await defectImageExpander.count();
  const trendAlertCount = await page
    .getByText(/预警 \[/).count();

  if (defectImageCount > 0) {
    await defectImageExpander.first().waitFor({ timeout: 60_000 });
  }

  await page.screenshot({
    path: "output/test-results/yield_alert_code_expanders_e2e.png",
    fullPage: false,
  });

  return { defectImageCount, trendAlertCount };
}
