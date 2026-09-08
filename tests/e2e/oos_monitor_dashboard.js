async page => {
  const output = "D:/wzy/Python/vivo-project/output/test-results/monitor-live";
  const base = "http://localhost:8516/";
  const errors = [];
  page.on("console", message => {
    const text = message.text();
    const sourceUrl = message.location().url || "";
    const telemetry = sourceUrl.includes("data.streamlit.io/metrics.json") ||
      text.includes("metrics config") || text.includes("metrics tracking");
    if (message.type() === "error" && !telemetry) errors.push(text);
  });
  const oosQuery = () => page.locator(".st-key-btn_monitor_query_submit button:visible").first();
  const cpkQuery = () => page.locator(".st-key-cpk_monitor_query_submit button:visible").first();
  const count = async () => Number((await page.getByText(/^Fixture calculations:/).textContent()).split(":")[1]);
  const absent = async text => {
    await page.getByText(text, { exact: true }).waitFor({ state: "hidden", timeout: 10000 });
  };

  await page.goto(base);
  await cpkQuery().waitFor({ timeout: 60000 });
  await page.getByText("Fixture calculations: 0", { exact: true }).waitFor();
  await absent("超规趋势");
  await absent("CPK 汇总表");
  await absent("数据最后更新时间（管理员）");
  await oosQuery().click();
  await page.getByText("超规趋势", { exact: true }).waitFor({ timeout: 60000 });
  await page.getByText(/^Fixture calculations: [1-9][0-9]*$/).waitFor();
  await absent("CPK 汇总表");
  const first = await count();
  if (first <= 0) throw new Error("OOS query did not calculate");
  await cpkQuery().click();
  await page.getByText("CPK 汇总表", { exact: true }).waitFor({ timeout: 60000 });
  await page.getByText("Fixture CPK workbook rows: 4", { exact: true }).waitFor();
  await page.getByText("Fixture CPK warning periods: 4", { exact: true }).waitFor();
  if (await count() !== first) throw new Error("CPK did not reuse cached raw-feature calculation");
  await absent("数据最后更新时间（管理员）");
  await page.screenshot({ path: `${output}/regular.png`, fullPage: true });

  await page.locator(".st-key-fixture_refresh_cache button:visible").first().click();
  await page.getByText("选择产品和厂别后，点击查询生成 CPK 预警看板。", { exact: true }).waitFor();
  await absent("超规趋势");
  await absent("CPK 汇总表");
  await oosQuery().click();
  await page.getByText("超规趋势", { exact: true }).waitFor({ timeout: 60000 });
  await page.getByText(`Fixture calculations: ${first * 2}`, { exact: true }).waitFor();
  if (await count() <= first) throw new Error("Refresh did not recompute without snapshot deletion");

  await page.goto(`${base}?admin=true`);
  await oosQuery().click();
  await page.getByText("数据最后更新时间（管理员）", { exact: true }).waitFor({ timeout: 60000 });
  await cpkQuery().click();
  await page.getByText("CPK 汇总表", { exact: true }).waitFor({ timeout: 60000 });
  await page.screenshot({ path: `${output}/admin.png`, fullPage: true });
  await page.setViewportSize({ width: 768, height: 900 });
  const overflow = await page.evaluate(() => ({ scroll: document.documentElement.scrollWidth, client: document.documentElement.clientWidth }));
  if (overflow.scroll > overflow.client + 2) throw new Error(`Horizontal overflow: ${JSON.stringify(overflow)}`);
  await page.screenshot({ path: `${output}/admin-768.png`, fullPage: true });
  if (errors.length) throw new Error(`Browser console errors: ${errors.join(" | ")}`);
  return "OOS/CPK shared-cache, query-gating, workbook and admin E2E passed";
}
