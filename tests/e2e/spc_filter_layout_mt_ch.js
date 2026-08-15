async page => {
  // Streamlit 1.60 兼容：选中值从 input value / 容器文本读取，不依赖 "Selected X" aria-label
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

  await page.goto("http://localhost:8503/SPC监控报表");
  await page
    .getByText("正在加载 SPC 分布数据...")
    .waitFor({ state: "hidden", timeout: 600_000 });

  const title = page.getByRole("heading", {
    name: "SPC监控报表",
    exact: true,
  });
  const productSelector = page.getByRole("combobox").first();
  const filterHeading = page.getByRole("heading", {
    name: "筛选",
    exact: true,
  });
  const alertCenter = page.getByText(/CPK预警中心（CPK < 1\.33）/).first();

  await filterHeading.waitFor({ timeout: 300_000 });
  await alertCenter.waitFor({ timeout: 300_000 });

  const [titleBox, productBox, filterBox, alertBox] = await Promise.all([
    title.boundingBox(),
    productSelector.boundingBox(),
    filterHeading.boundingBox(),
    alertCenter.boundingBox(),
  ]);
  if (!titleBox || !productBox || !filterBox || !alertBox) {
    throw new Error("无法取得页头、筛选区或自动预警模块的位置。");
  }
  if (
    !(
      titleBox.y < productBox.y &&
      productBox.y < filterBox.y &&
      filterBox.y < alertBox.y
    )
  ) {
    throw new Error(
      `页面顺序错误: title=${titleBox.y}, product=${productBox.y}, ` +
        `filter=${filterBox.y}, alert=${alertBox.y}`,
    );
  }

  if ((await productSelector.inputValue()) !== "M626") {
    await productSelector.click();
    await page.getByRole("option", { name: "M626", exact: true }).click();
    await waitComboValue("📦 当前产品型号", "M626");
    await page
      .getByText("正在加载 SPC 分布数据...")
      .waitFor({ state: "hidden", timeout: 600_000 });
  }

  if ((await comboInputValue("厂别")) !== "OLED") {
    await page.getByRole("combobox", { name: "厂别" }).click();
    await page.getByRole("option", { name: "OLED", exact: true }).click();
  }

  const stationSelector = page.getByRole("combobox", { name: "站点" });
  await stationSelector.fill("21200");
  await page.getByRole("option", { name: "21200", exact: true }).click();
  await page.keyboard.press("Escape");
  await waitMultiValue("站点", "21200", 300_000);

  const expectedParameters = [
    "MT_CH_PRESS_EXPONENT",
    "MT_CH_PRESS_MANTISSA",
  ];
  for (const parameter of expectedParameters) {
    await waitMultiValue("参数名称", parameter, 300_000);
  }

  const selectedParameters = await page.evaluate(() => {
    const c = [...document.querySelectorAll('[data-testid="stMultiSelect"]')].find(
      x => (x.querySelector('[data-testid="stWidgetLabel"]')?.textContent || "").includes("参数名称"),
    );
    return c?.textContent || "";
  });
  for (const parameter of expectedParameters) {
    if (!selectedParameters.includes(parameter)) {
      throw new Error(`参数筛选框中缺少 ${parameter}`);
    }
  }

  await waitQueryEnabled(300_000);
  await page.getByRole("button", { name: "查询" }).click();
  for (const parameter of expectedParameters) {
    await page
      .getByText(`OLED | 21200 | ${parameter}`, { exact: true })
      .last()
      .waitFor({ timeout: 300_000 });
  }

  return {
    order: ["page_header", "filters", "cpk_alert_center"],
    product: "M626",
    factory: "OLED",
    station: "21200",
    parameters: expectedParameters,
  };
}
