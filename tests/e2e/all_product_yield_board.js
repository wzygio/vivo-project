async page => {
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  await page.setViewportSize({ width: 1880, height: 1080 });
  await page.goto("http://127.0.0.1:8521");
  const visible = text => page.getByText(text, { exact: true }).waitFor();
  const charts = page.locator(".js-plotly-plot");
  const waitCharts = count => page.waitForFunction(
    n => document.querySelectorAll(".js-plotly-plot").length === n, count,
  );
  const assertClean = async () => {
    if (await page.getByTestId("stException").count()) throw new Error("Streamlit exception");
    const text = await page.locator("body").innerText();
    if (/PRIVATE_CONFIG_PATH|admin=true/.test(text)) throw new Error("Private details leaked");
  };
  await visible("Full runs: 1");
  if (await charts.count()) throw new Error("Query gate loaded early");
  await page.getByRole("button", { name: "查询", exact: true }).click();
  await waitCharts(6);
  await visible("M678 loads: 1");
  await visible("M626 loads: 1");
  await visible("Full runs: 1");
  await assertClean();
  const initial = await charts.evaluateAll(nodes => nodes.map(node => ({
    title: node.layout.title.text,
    periods: node.data[0].x.length,
    bars: node.data.filter(trace => trace.type === "bar").map(trace => trace.name),
    countLine: node.data.some(trace => trace.name === "入库数"),
  })));
  if (JSON.stringify(initial.map(chart => chart.periods)) !== "[3,3,7,3,3,7]") {
    throw new Error("Unexpected time windows");
  }
  if (!initial.every(chart => chart.countLine)) throw new Error("Missing count line");
  await page.screenshot({ path: "desktop.png", fullPage: true });

  const group = page.getByRole("combobox", { name: /选择Group/ }).first();
  await group.click();
  await group.press("Backspace");
  await page.keyboard.press("Escape");
  await visible("M678 loads: 2");
  await visible("M626 loads: 1");
  await visible("Full runs: 1");
  await page.waitForFunction(() => {
    const plots = [...document.querySelectorAll(".js-plotly-plot")];
    return plots.length === 6 && plots[0].data.filter(t => t.type === "bar").length === 2;
  });
  const secondBars = await charts.nth(3).evaluate(node => node.data.filter(t => t.type === "bar").length);
  if (secondBars !== 3) throw new Error("Group filter changed another product");

  const product = page.getByRole("combobox", { name: /产品型号$/ });
  await product.click();
  await page.getByRole("option", { name: "M626", exact: true }).click();
  await page.keyboard.press("Escape");
  await waitCharts(3);
  await visible("Full runs: 1");
  await product.click();
  await product.press("Backspace");
  await page.keyboard.press("Escape");
  await waitCharts(6);

  const viewports = [];
  for (const width of [1440, 768, 375]) {
    await page.setViewportSize({ width, height: 1000 });
    await page.waitForFunction(() => [...document.querySelectorAll(".js-plotly-plot")].every(
      node => node.getBoundingClientRect().right <= window.innerWidth + 2,
    ));
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth + 2);
    if (overflow) throw new Error(`Page overflow at ${width}px`);
    await page.screenshot({ path: `width-${width}.png`, fullPage: true });
    viewports.push(width);
  }
  await page.setViewportSize({ width: 1440, height: 1000 });
  for (const [mode, message, count] of [
    ["empty", "该产品暂无入库良率趋势数据。", 3],
    ["failed", "该产品良率数据加载失败，请稍后重新查询。", 3],
    ["stale", "数据更新或完整性尚未确认，以下趋势仅供参考。", 6],
    ["unavailable", "该产品入库数据暂不可用，请稍后重新查询。", 3],
  ]) {
    await page.getByRole("combobox", { name: "Scenario", exact: true }).click();
    await page.getByRole("option", { name: mode, exact: true }).click();
    await visible(message);
    await waitCharts(count);
    await assertClean();
  }
  await page.screenshot({ path: "unavailable.png", fullPage: true });
  if (errors.length) throw new Error(errors.join("\n"));
  return { passed: true, initial, groupFragmentIsolation: true, productScope: true,
    viewports, healthBranches: ["empty", "failed", "stale", "unavailable"], pageErrors: errors };
}
