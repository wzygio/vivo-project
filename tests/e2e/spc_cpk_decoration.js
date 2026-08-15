async page => {
  // Streamlit 1.60 兼容：单选 combobox 选中值在 input value 属性，
  // 多选选中项在 stMultiSelect 容器文本中（aria-label 不再含 "Selected X"）
  const comboInputValue = label =>
    page.evaluate(l => {
      const el = [...document.querySelectorAll('[role="combobox"]')].find(
        e => e.getAttribute("aria-label") === l,
      );
      return el?.value || "";
    }, label);
  const waitComboValue = (label, value, timeout = 600_000) =>
    page.waitForFunction(
      ([l, v]) => [...document.querySelectorAll('[role="combobox"]')].some(
        e => e.getAttribute("aria-label") === l && (e.value || "").includes(v),
      ),
      [label, value],
      { timeout },
    );
  const waitMultiValue = (label, value, timeout = 600_000) =>
    page.waitForFunction(
      ([l, v]) => [...document.querySelectorAll('[data-testid="stMultiSelect"]')].some(
        c => (c.querySelector('[data-testid="stWidgetLabel"]')?.textContent || "").includes(l)
          && c.textContent.includes(v),
      ),
      [label, value],
      { timeout },
    );
  const waitQueryEnabled = (timeout = 600_000) =>
    page.waitForFunction(
      () => {
        const btn = [...document.querySelectorAll("button")].find(
          b => b.textContent?.trim() === "查询",
        );
        return btn && !btn.disabled;
      },
      undefined,
      { timeout },
    );

  await page.goto(
    "http://localhost:8503/SPC%E7%9B%91%E6%8E%A7%E6%8A%A5%E8%A1%A8?admin=true",
  );
  await page.getByRole("combobox").first().waitFor({ timeout: 300_000 });
  await page
    .getByText("当前筛选条件尚未查询。")
    .waitFor({ timeout: 300_000 });

  const productSelector = page.getByRole("combobox").first();
  if ((await productSelector.inputValue()) !== "Z571") {
    await productSelector.click();
    await page.getByRole("option", { name: "Z571" }).click();
    await waitComboValue("📦 当前产品型号", "Z571");
  }
  // 就绪指示：CPK 预警中心已渲染（预警周次随当前日期滚动，不断言具体指标名）
  const readyIndicator = page
    .getByText(/🚨 自动预警指标图像（\d+ 个指标）/)
    .first();
  await readyIndicator.waitFor({ timeout: 300_000 });

  // 不等待 toast / spinner 的可见性：重渲染高峰期短生命周期元素可能被轮询错过；
  // 刷新生效的判定以随后「就绪指示重新出现 + 能力值网格可读」为准。
  await page.getByRole("button", { name: "🔄 刷新缓存" }).click();
  await page
    .getByText("正在加载 SPC 分布数据...")
    .waitFor({ state: "hidden", timeout: 600_000 })
    .catch(() => {});
  await readyIndicator.waitFor({ timeout: 600_000 });

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

  if (!(await waitMultiValue("站点", "1L650", 5_000).then(() => true).catch(() => false))) {
    const stationSelector = page.getByRole("combobox", { name: "站点" });
    await stationSelector.fill("1L650");
    await page.getByRole("option", { name: "1L650", exact: true }).click();
    await page.keyboard.press("Escape");
    await waitMultiValue("站点", "1L650", 300_000);
  }

  if (!(await waitMultiValue("参数名称", "CD1", 5_000).then(() => true).catch(() => false))) {
    // 参数名称在站点选择应用的 rerun 完成前处于 disabled，先等其启用
    await page.waitForFunction(
      () => {
        const el = [...document.querySelectorAll('[role="combobox"]')].find(
          e => e.getAttribute("aria-label") === "参数名称",
        );
        return el && el.getAttribute("aria-disabled") !== "true";
      },
      undefined,
      { timeout: 300_000 },
    );
    await page.getByRole("combobox", { name: "参数名称" }).fill("CD1");
    const parameterOption = page.getByRole("option", { name: "CD1", exact: true });
    if (await parameterOption.isVisible()) {
      await parameterOption.click();
    }
    await page.keyboard.press("Escape");
    await waitMultiValue("参数名称", "CD1", 300_000);
  }

  await waitQueryEnabled(300_000);
  await page.getByRole("button", { name: "查询" }).click();
  const indicator = page.getByText("ARRAY | 1L650 | CD1", { exact: true }).last();
  await indicator.waitFor({ timeout: 300_000 });

  const indicatorExpander = indicator.locator(
    'xpath=ancestor::div[@data-testid="stExpander"]',
  );
  const capabilityGrid = indicatorExpander.locator(
    '[data-testid="stDataFrame"] table',
  );
  await capabilityGrid.waitFor({ state: "attached", timeout: 60_000 });
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
    product: await productSelector.inputValue(),
    indicator: await indicator.innerText(),
    verifiedRows: expectedRows.length,
  };
}
