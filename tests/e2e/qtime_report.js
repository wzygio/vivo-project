async page => {
  // Q-Time监控报表 E2E 回归（隔离 fixture，tests/e2e/fixtures/qtime_app.py，端口 8513）
  // 覆盖：初始查询门控、查询交互、预警中心 + 图像渲染、TP 失败时不显示功能异常。
  await page.goto("http://localhost:8513/");
  await page
    .getByText("重点站点QTime监控", { exact: false })
    .first()
    .waitFor({ timeout: 120_000 });

  // 初始未查询时保持空白，不显示操作提示。
  if (await page.getByText(/请选择筛选条件并点击/).count()) {
    throw new Error("Unexpected query prompt");
  }

  // Streamlit may restore an empty keyed multiselect from session state. Make
  // the precondition explicit instead of assuming the first station default.
  const queryButton = page.getByRole("button", { name: "查询" });
  const ensureStationSelected = async () => {
    if (await queryButton.isDisabled()) {
      await page.locator('div[data-testid="stMultiSelect"]').nth(1).click();
      await page.getByRole("option").first().click();
    }
  };
  await ensureStationSelected();

  // 查询（默认 ARRAY + 首个站点）→ 预警中心 + 图表
  await queryButton.click();
  await page
    .getByText(/检测到 \d+ 条已确认超规/)
    .waitFor({ timeout: 180_000 });
  await page
    .locator('div[data-testid="stPlotlyChart"]')
    .first()
    .waitFor({ timeout: 180_000 });

  // 再点一次查询：走缓存层，页面保持稳定渲染、无异常
  await queryButton.click();
  await page
    .getByText(/检测到 \d+ 条已确认超规/)
    .waitFor({ timeout: 180_000 });
  await page.screenshot({
    path: "output/test-results/qtime_report_e2e.png",
    fullPage: false,
  });

  // 切换厂别为 TP（fixture 抛 QTimeDataAccessError），界面不展示技术或功能异常。
  await page.locator('div[data-testid="stSelectbox"]').first().click();
  await page
    .getByRole("option", { name: "TP" })
    .click();
  await ensureStationSelected();
  await queryButton.click();
  await page.locator('[data-testid="stStatusWidget"]').waitFor({ state: "hidden" });
  if (await page.locator('[data-testid="stException"], [data-testid="stPlotlyChart"]').count()
      || await page.getByText(/Q-Time 数据读取失败|预警结果暂不可用/).count()) {
    throw new Error("Unexpected failure display or stale chart");
  }
  await page.screenshot({
    path: "output/test-results/qtime_report_tp_error_e2e.png",
    fullPage: false,
  });

  return { ok: true };
}
