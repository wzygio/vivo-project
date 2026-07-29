async page => {
  await page.goto("http://localhost:8503/SPC监控报表");
  await page
    .getByText("正在加载 SPC 分布数据...")
    .waitFor({ state: "hidden", timeout: 120_000 });

  const productSelector = page.getByRole("combobox").first();
  const selectedProduct = await productSelector.getAttribute("aria-label");
  if (!selectedProduct?.includes("M673")) {
    await productSelector.click();
    await page.getByRole("option", { name: "M673" }).click();
  }

  await page
    .getByRole("combobox", { name: /Selected M673/ })
    .waitFor({ timeout: 120_000 });
  await page
    .getByRole("heading", { name: "筛选", exact: true })
    .waitFor({ timeout: 120_000 });

  const refreshedToast = page
    .getByText(/M673 缓存已刷新/)
    .waitFor({ timeout: 30_000 });
  await page.getByRole("button", { name: "🔄 刷新缓存" }).click();
  await refreshedToast;
  await page
    .getByText("正在加载 SPC 分布数据...")
    .waitFor({ state: "hidden", timeout: 120_000 });
  await page
    .getByRole("heading", { name: "筛选", exact: true })
    .waitFor({ timeout: 120_000 });

  const alertCenterSummary = page
    .locator("summary")
    .filter({ hasText: "CPK预警中心" });
  await alertCenterSummary.click();
  await page.waitForFunction(
    () => {
      const text = document.body.innerText;
      return (
        /检测到 \d+ 条 CPK 预警/.test(text) ||
        text.includes("未发现低于 1.33 的 CPK")
      );
    },
    null,
    { timeout: 120_000 },
  );

  const alertMessage = page.getByText(/检测到 \d+ 条 CPK 预警/);
  const allClearMessage = page.getByText(/未发现低于 1.33 的 CPK/);
  const hasAlerts = (await alertMessage.count()) > 0;
  if (hasAlerts) {
    await page
      .getByRole("heading", { name: "自动预警指标图像", exact: true })
      .waitFor({ timeout: 120_000 });
    const expanderCount = await page
      .locator('[data-testid="stExpander"]')
      .count();
    if (expanderCount < 2) {
      throw new Error("存在 CPK 预警，但没有渲染对应的自动预警指标。");
    }
  } else {
    await allClearMessage.waitFor({ timeout: 30_000 });
    if (
      (await page
        .getByRole("heading", {
          name: "自动预警指标图像",
          exact: true,
        })
        .count()) !== 0
    ) {
      throw new Error("CPK 全部合规时仍渲染了自动预警指标。");
    }
  }

  const callbackRerunWarningCount = await page
    .getByText(/Calling st\.rerun\(\) within a callback is a no-op\./)
    .count();
  if (callbackRerunWarningCount !== 0) {
    throw new Error("刷新缓存回调仍在调用无效的 st.rerun()。");
  }

  return {
    product: await productSelector.getAttribute("aria-label"),
    status: hasAlerts ? await alertMessage.innerText() : await allClearMessage.innerText(),
  };
}
