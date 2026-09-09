async page => {
  const base = "http://127.0.0.1:8529/";
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  const query = async () => {
    await page.keyboard.press("Escape");
    const button = page.getByRole("button", { name: "查询", exact: true });
    await button.waitFor();
    if (await button.isDisabled()) {
      await page.getByTestId("stMultiSelect").nth(1).getByRole("combobox").click();
      await page.getByRole("option").first().click();
      await page.keyboard.press("Escape");
    }
    await button.click();
  };
  await page.goto(base + "?fixture_health=stale");
  await query();
  await page.getByText("数据库刷新失败，当前显示旧快照，不能据此确认当前无异常。", { exact: true }).waitFor();
  const sizes = [[1440, 1000], [768, 1000], [375, 900]];
  const layouts = [];
  for (const [width, height] of sizes) {
    await page.setViewportSize({ width, height });
    await page.screenshot({ path: `backend-health-stale-${width}.png`, fullPage: false });
    layouts.push(await page.evaluate(() => ({ viewport: innerWidth, body: document.documentElement.scrollWidth })));
  }
  await page.goto(base + "?fixture_health=stale&fixture_empty=true");
  await query();
  await page.getByText("数据库刷新失败，当前显示旧快照，不能据此确认当前无异常。", { exact: true }).waitFor();
  if (await page.getByTestId("stAlertContainer").filter({ hasText: "系统监测正常" }).count()) {
    throw new Error("stale source produced a normal result");
  }
  if (await page.getByText("当前筛选条件下暂无 Q-Time 数据。", { exact: true }).count()) {
    throw new Error("empty stale data masqueraded as a successful empty query");
  }
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.screenshot({ path: "backend-health-stale-empty.png", fullPage: false });
  await page.goto(base);
  await query();
  await page.getByTestId("stPlotlyChart").first().waitFor();
  await page.screenshot({ path: "backend-health-fresh.png", fullPage: false });
  return { ok: true, pageErrors: errors, layouts, visualBaseline: "not available" };
}
