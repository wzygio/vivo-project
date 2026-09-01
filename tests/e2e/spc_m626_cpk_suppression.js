async page => {
  const waitComboValue = (label, value, timeout = 600_000) =>
    page.waitForFunction(
      ([l, v]) => [...document.querySelectorAll('[role="combobox"]')].some(
        element => element.getAttribute("aria-label") === l
          && (element.value || "").includes(v),
      ),
      [label, value],
      { timeout },
    );

  await page.goto("http://localhost:8503/SPC监控报表");
  const productSelector = page.getByRole("combobox").first();
  await productSelector.waitFor({ timeout: 300_000 });
  if ((await productSelector.inputValue()) !== "M626") {
    await productSelector.click();
    await page.getByRole("option", { name: "M626", exact: true }).click();
  }
  await waitComboValue("📦 当前产品型号", "M626");
  await page
    .getByText("正在加载 SPC 分布数据...")
    .waitFor({ state: "hidden", timeout: 600_000 })
    .catch(() => {});
  await page
    .getByRole("heading", { name: "筛选", exact: true })
    .waitFor({ timeout: 300_000 });

  const alertCenter = page
    .locator('[data-testid="stExpander"]')
    .filter({ has: page.locator("summary").filter({ hasText: "CPK预警中心" }) })
    .first();
  await alertCenter.locator("summary").click();
  await page.waitForFunction(
    () => /检测到 \d+ 条 CPK 预警/.test(document.body.innerText)
      || document.body.innerText.includes("未发现低于 1.33 的 CPK"),
    null,
    { timeout: 300_000 },
  );

  const alertText = await alertCenter.innerText();
  for (const hiddenStep of ["1L650", "41450"]) {
    if (alertText.includes(hiddenStep)) {
      throw new Error(`M626 CPK 预警表仍显示临时抑制站点: ${hiddenStep}`);
    }
  }

  for (const hiddenIndicator of [
    "ARRAY | 1L650 | CD1",
    "TP | 41450 | OVL1_Y",
  ]) {
    if ((await page.getByText(hiddenIndicator, { exact: true }).count()) !== 0) {
      throw new Error(`自动预警指标图像仍显示: ${hiddenIndicator}`);
    }
  }

  await page.screenshot({
    path: "output/test-results/spc-m626-cpk-suppression/e2e-pass.png",
    fullPage: true,
  });

  return {
    product: await productSelector.inputValue(),
    alertCenterText: alertText,
    hiddenIndicators: 2,
  };
}
