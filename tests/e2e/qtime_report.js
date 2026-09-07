async page => {
  // Q-Time监控报表 E2E 回归（隔离 fixture，tests/e2e/fixtures/qtime_app.py，端口 8513）
  // 覆盖：初始门控提示、查询交互、预警中心 + 图像渲染、TP 厂别数据访问失败降级。
  await page.goto("http://localhost:8513/");
  await page
    .getByText("北极星QTime监控", { exact: false })
    .first()
    .waitFor({ timeout: 120_000 });

  // 初始：未查询时的门控提示
  await page
    .getByText(/请选择筛选条件并点击/)
    .waitFor({ timeout: 60_000 });

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
    .getByText(/检测到 \d+ 条已确认真实超规/)
    .waitFor({ timeout: 180_000 });
  await page
    .locator('div[data-testid="stPlotlyChart"]')
    .first()
    .waitFor({ timeout: 180_000 });

  // 再点一次查询：走缓存层，页面保持稳定渲染、无异常
  await queryButton.click();
  await page
    .getByText(/检测到 \d+ 条已确认真实超规/)
    .waitFor({ timeout: 180_000 });
  await page.screenshot({
    path: "output/test-results/qtime_report_e2e.png",
    fullPage: false,
  });

  // 切换厂别为 TP（fixture 抛 QTimeDataAccessError）→ 安全错误文案降级
  await page.locator('div[data-testid="stSelectbox"]').first().click();
  await page
    .getByRole("option", { name: "TP" })
    .click();
  await ensureStationSelected();
  await queryButton.click();
  await page
    .getByText(/Q-Time 数据读取失败，请联系系统管理员确认数据库权限。/)
    .waitFor({ timeout: 120_000 });
  await page.screenshot({
    path: "output/test-results/qtime_report_tp_error_e2e.png",
    fullPage: false,
  });

  return { ok: true };
}
