async page => {
  await page.goto("http://localhost:8503/SPC监控报表");
  await page
    .getByText("正在加载 SPC 分布数据...")
    .waitFor({ state: "hidden", timeout: 120_000 });

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

  await filterHeading.waitFor({ timeout: 120_000 });
  await alertCenter.waitFor({ timeout: 120_000 });

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

  const selectedProduct = await productSelector.getAttribute("aria-label");
  if (!selectedProduct?.includes("M626")) {
    await productSelector.click();
    await page.getByRole("option", { name: "M626", exact: true }).click();
    await page
      .getByRole("combobox", { name: /Selected M626/ })
      .waitFor({ timeout: 120_000 });
    await page
      .getByText("正在加载 SPC 分布数据...")
      .waitFor({ state: "hidden", timeout: 120_000 });
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

  const parameterSelector = page.getByRole("combobox", {
    name: "参数名称",
  });
  const expectedParameters = [
    "MT_CH_PRESS_EXPONENT",
    "MT_CH_PRESS_MANTISSA",
  ];
  await page
    .getByRole("combobox", {
      name: /Selected .*MT_CH_PRESS_EXPONENT.*MT_CH_PRESS_MANTISSA.*参数名称/,
    })
    .waitFor({ timeout: 30_000 });

  const selectedParameters =
    (await parameterSelector.getAttribute("aria-label")) || "";
  for (const parameter of expectedParameters) {
    if (!selectedParameters.includes(parameter)) {
      throw new Error(`参数筛选框中缺少 ${parameter}`);
    }
  }

  await page.getByRole("button", { name: "查询" }).click();
  for (const parameter of expectedParameters) {
    await page
      .getByText(`OLED | 21200 | ${parameter}`, { exact: true })
      .last()
      .waitFor({ timeout: 120_000 });
  }

  return {
    order: ["page_header", "filters", "cpk_alert_center"],
    product: "M626",
    factory: "OLED",
    station: "21200",
    parameters: expectedParameters,
  };
}
