async page => {
  await page.goto(
    "http://localhost:8503/SPC%E7%9B%91%E6%8E%A7%E6%8A%A5%E8%A1%A8?admin=true",
  );
  await page.getByRole("combobox").first().waitFor({ timeout: 120_000 });
  await page
    .getByText("当前筛选条件尚未查询。")
    .waitFor({ timeout: 120_000 });

  const productSelector = page.getByRole("combobox").first();
  const selectedProduct = await productSelector.getAttribute("aria-label");
  if (!selectedProduct?.includes("Z571")) {
    await productSelector.click();
    await page.getByRole("option", { name: "Z571" }).click();
    await page
      .getByRole("combobox", { name: /Selected Z571/ })
      .waitFor({ timeout: 120_000 });
  }
  // 就绪指示：CPK 预警中心已渲染（预警周次随当前日期滚动，不断言具体指标名）
  const readyIndicator = page
    .getByText(/🚨 自动预警指标图像（\d+ 个指标）/)
    .first();
  await readyIndicator.waitFor({ timeout: 120_000 });

  // 不等待 toast / spinner 的可见性：重渲染高峰期短生命周期元素可能被轮询错过；
  // 刷新生效的判定以随后「就绪指示重新出现 + 能力值网格可读」为准。
  await page.getByRole("button", { name: "🔄 刷新缓存" }).click();
  await page
    .getByText("正在加载 SPC 分布数据...")
    .waitFor({ state: "hidden", timeout: 300_000 })
    .catch(() => {});
  await readyIndicator.waitFor({ timeout: 300_000 });

  for (const correctedIndicator of [
    "ARRAY | 1L650 | CD1",
    "ARRAY | 1B990 | TFT_7_SS_S_V2",
  ]) {
    const staleAlertCount = await page
      .getByText(correctedIndicator, { exact: true })
      .count();
    if (staleAlertCount !== 0) {
      throw new Error(`${correctedIndicator} 仍被显示为 CPK 预警`);
    }
  }

  const stationSelector = page.getByRole("combobox", { name: "站点" });
  if (!(await stationSelector.getAttribute("aria-label"))?.includes("1L650")) {
    await stationSelector.fill("1L650");
    await page.getByRole("option", { name: "1L650", exact: true }).click();
    await page.keyboard.press("Escape");
    await page
      .getByRole("combobox", { name: /Selected 1L650/ })
      .waitFor({ timeout: 30_000 });
  }

  const parameterSelector = page.getByRole("combobox", { name: "参数名称" });
  if (!(await parameterSelector.getAttribute("aria-label"))?.includes("CD1")) {
    // 参数名称在站点选择应用的 rerun 完成前处于 disabled，先等其启用
    await page.waitForFunction(
      () => {
        const el = [...document.querySelectorAll('[role="combobox"]')].find(
          e => e.getAttribute("aria-label") === "参数名称",
        );
        return el && el.getAttribute("aria-disabled") !== "true";
      },
      { timeout: 300_000 },
    );
    await parameterSelector.fill("CD1");
    const parameterOption = page.getByRole("option", { name: "CD1", exact: true });
    if (await parameterOption.isVisible()) {
      await parameterOption.click();
    }
    await page.keyboard.press("Escape");
    await page
      .getByRole("combobox", { name: /Selected CD1/ })
      .waitFor({ timeout: 30_000 });
  }

  await page.getByRole("button", { name: "查询" }).click();
  const indicator = page.getByText("ARRAY | 1L650 | CD1", { exact: true }).last();
  await indicator.waitFor({ timeout: 120_000 });

  const indicatorExpander = indicator.locator(
    'xpath=ancestor::div[@data-testid="stExpander"]',
  );
  const capabilityGrid = indicatorExpander.locator(
    '[data-testid="stDataFrame"] table',
  );
  await capabilityGrid.waitFor({ state: "attached", timeout: 30_000 });
  const rows = await capabilityGrid.locator("tbody tr").allTextContents();

  const expectedRows = [
    ["月 2026-07", "1.663"],
    ["周 2026-W28", "1.554"],
    ["周 2026-W29", "1.385"],
    ["周 2026-W30", "1.365"],
    ["日 2026-07-20", "1.441"],
    ["日 2026-07-21", "1.381"],
    ["日 2026-07-25", "1.389"],
    ["日 2026-07-26", "1.396"],
  ];
  for (const [period, cpk] of expectedRows) {
    const matchingRow = rows.find((row) => row.includes(period));
    if (!matchingRow || !matchingRow.endsWith(cpk)) {
      throw new Error(
        `CPK 修饰值不匹配: ${period}, expected=${cpk}, row=${matchingRow}`,
      );
    }
  }

  await page.screenshot({
    path: "output/test-results/spc-cpk-decoration/e2e-pass.png",
    fullPage: true,
  });

  return {
    product: await productSelector.getAttribute("aria-label"),
    indicator: await indicator.innerText(),
    verifiedRows: expectedRows.length,
  };
}
