async page => {
  const qa = {
    functional: [],
    visual: [],
    viewport: [],
    exploratory: [],
  };
  const consoleErrors = [];
  page.on("console", message => {
    if (message.type() === "error") {
      consoleErrors.push({
        text: message.text(),
        url: message.location().url || "",
      });
    }
  });

  await page.goto("http://localhost:8503/SPC监控报表");
  await page
    .getByText("正在加载 SPC 分布数据...")
    .waitFor({ state: "hidden", timeout: 240_000 });
  await page
    .getByRole("heading", { name: "筛选", exact: true })
    .waitFor({ timeout: 240_000 });
  qa.functional.push("SPC page loaded without a Streamlit error boundary");

  const productSelector = page.getByRole("combobox").first();
  if (!(await productSelector.getAttribute("aria-label"))?.includes("M626")) {
    await productSelector.click();
    await page.getByRole("option", { name: "M626", exact: true }).click();
    await page
      .getByText("正在加载 SPC 分布数据...")
      .waitFor({ state: "hidden", timeout: 240_000 });
  }

  const factorySelector = page.getByRole("combobox", { name: "厂别" });
  if (!(await factorySelector.getAttribute("aria-label"))?.includes("OLED")) {
    await factorySelector.click();
    await page.getByRole("option", { name: "OLED", exact: true }).click();
  }

  const stationSelector = page.getByRole("combobox", { name: "站点" });
  await stationSelector.fill("21200");
  await page.getByRole("option", { name: "21200", exact: true }).click();
  await page.keyboard.press("Escape");
  await page
    .getByRole("combobox", { name: /Selected 21200/ })
    .waitFor({ timeout: 30_000 });

  await page.getByRole("button", { name: "查询" }).click();
  await page
    .getByText(/OLED \| 21200 \| /, { exact: false })
    .last()
    .waitFor({ timeout: 240_000 });

  const chartInventory = await page.evaluate(() =>
    [...document.querySelectorAll(".js-plotly-plot")].map(graph => ({
      title: graph.layout?.title?.text || "",
      traceNames: (graph.data || []).map(trace => trace.name).filter(Boolean),
    })),
  );
  const mainProcessCharts = chartInventory.filter(chart =>
    chart.title.includes("By主站点设备/腔室"),
  );
  const timeCharts = chartInventory.filter(chart =>
    chart.title.includes("By过货时间"),
  );
  if (mainProcessCharts.length === 0) {
    throw new Error(`未找到主站点设备/腔室图。图表清单: ${JSON.stringify(chartInventory)}`);
  }
  if (timeCharts.length === 0) {
    throw new Error("第三幅按过货时间排序图未保留。");
  }
  if (mainProcessCharts.some(chart => chart.traceNames.length === 0)) {
    throw new Error(`主站点图缺少设备/腔室分组: ${JSON.stringify(mainProcessCharts)}`);
  }
  qa.functional.push(
    `${mainProcessCharts.length} main-process charts and ${timeCharts.length} time charts rendered`,
  );
  qa.exploratory.push(
    `trace labels: ${JSON.stringify(mainProcessCharts.flatMap(chart => chart.traceNames).slice(0, 12))}`,
  );

  await page.setViewportSize({ width: 1440, height: 1000 });
  const wideOverflow = await page.evaluate(
    () => document.documentElement.scrollWidth - window.innerWidth,
  );
  if (wideOverflow > 2) {
    throw new Error(`宽屏出现水平溢出: ${wideOverflow}px`);
  }
  qa.visual.push("wide layout has no horizontal overflow");

  await page.setViewportSize({ width: 900, height: 900 });
  const narrowOverflow = await page.evaluate(
    () => document.documentElement.scrollWidth - window.innerWidth,
  );
  if (narrowOverflow > 2) {
    throw new Error(`窄视口出现水平溢出: ${narrowOverflow}px`);
  }
  qa.viewport.push("900px viewport fits without horizontal overflow");

  const errorText = await page.getByText(/StreamlitDuplicateElementId|Traceback:/).count();
  if (errorText !== 0) {
    throw new Error("页面出现 Streamlit 错误或 traceback。");
  }

  const expectedInfrastructureError = error =>
    error.url.includes("/_stcore/health") ||
    error.url.includes("/_stcore/host-config") ||
    error.url.includes("data.streamlit.io/metrics.json") ||
    error.text.includes("metrics config") ||
    error.text.includes("Undefined metrics config");
  const unexpectedConsoleErrors = consoleErrors.filter(
    error => !expectedInfrastructureError(error),
  );
  if (unexpectedConsoleErrors.length > 0) {
    throw new Error(`出现未预期的控制台错误: ${JSON.stringify(unexpectedConsoleErrors)}`);
  }
  qa.exploratory.push(
    `${consoleErrors.length} known Streamlit infrastructure console errors ignored`,
  );

  await page.screenshot({
    path: "output/screenshots/spc_main_process_chamber_e2e.png",
    fullPage: true,
  });
  qa.visual.push("full-page screenshot captured");

  return {
    product: "M626",
    factory: "OLED",
    station: "21200",
    mainProcessCharts: mainProcessCharts.length,
    mainProcessTraceNames: mainProcessCharts.flatMap(chart => chart.traceNames).slice(0, 12),
    consoleErrors,
    qa,
  };
}
