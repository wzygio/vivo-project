async page => {
  // Run playwright-cli from output/test-results/warning-yield-fragments so
  // screenshots and automatic browser snapshots stay in the approved directory.
  await page.setViewportSize({ width: 1440, height: 1300 });
  await page.goto("http://127.0.0.1:8522");
  const visible = text => page.getByText(text, { exact: true }).waitFor();
  const button = key => page.locator(`.st-key-${key} button:visible`).first();
  await button("btn_load_alert_matrix").click();
  await visible("Matrix runs: 1");
  await button("all_product_yield_query").click();
  await visible("Yield loads: 1");
  await page.waitForFunction(() => document.querySelectorAll(".js-plotly-plot").length === 3);
  const before = await page.locator("body").innerText();
  const initialFullRuns = before.match(/Full runs: (\d+)/)[1];
  await button("matrix_cell_spc_cpk_trend_M678").click();
  await visible("Detail loading");
  // Sample after Streamlit's stale fade delay, while the fixture is still loading.
  await page.waitForTimeout(1000);
  const heading = page.getByRole("heading", { name: "全产品良率看板", exact: true });
  const hasFadedAncestor = node => {
    for (let el = node; el; el = el.parentElement) {
      if (Number(getComputedStyle(el).opacity) < .99 || el.getAttribute("data-stale") === "true") return true;
    }
    return false;
  };
  const faded = await heading.evaluate(hasFadedAncestor);
  const fadedChart = await page.locator(".js-plotly-plot").first().evaluate(hasFadedAncestor);
  await page.screenshot({ path: "detail-loading.png", fullPage: true });
  if (faded || fadedChart) throw new Error("Yield board became stale/faded during matrix detail loading");
  await visible("当前已无上一周 CPK 预警（数据可能已更新）。");
  await visible(`Full runs: ${initialFullRuns}`);
  await visible("Yield loads: 1");
  await button("matrix_detail_close").click();
  await page.getByText("当前已无上一周 CPK 预警（数据可能已更新）。", { exact: true }).waitFor({ state: "hidden" });
  await visible("Yield loads: 1");
  const matrixRuns = (await page.locator("body").innerText()).match(/Matrix runs: (\d+)/)[1];
  await button("all_product_yield_query").click();
  await visible("Yield loads: 2");
  await visible(`Matrix runs: ${matrixRuns}`);
  await button("btn_collapse_alert_matrix").click();
  await button("btn_load_alert_matrix").waitFor();
  await visible("Yield loads: 2");
  await visible(`Full runs: ${initialFullRuns}`);
  if (await page.getByTestId("stException").count()) throw new Error("Streamlit exception");
  return { passed: true, fadedDuringDetail: faded, fadedChart, initialFullRuns,
    bidirectionalIsolation: true, detailCloseAndMatrixCollapse: true };
}
