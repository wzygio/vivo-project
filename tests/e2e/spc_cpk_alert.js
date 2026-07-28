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

  // Wait for the M673 page rerun to finish before clicking the cache button.
  // Waiting only for a spinner to be hidden can resolve before that spinner
  // appears, causing the click to hit the preceding product's page instance.
  await page
    .getByText("TP | 41260 | 4PP_Rs", { exact: true })
    .first()
    .waitFor({ timeout: 120_000 });

  const refreshedToast = page
    .getByText(/M673 缓存已刷新/)
    .waitFor({ timeout: 30_000 });
  await page.getByRole("button", { name: "🔄 刷新缓存" }).click();
  await refreshedToast;
  await page.getByText(/检测到 \d+ 条 CPK 预警/).waitFor({ timeout: 120_000 });
  await page
    .getByText("TP | 41260 | 4PP_Rs", { exact: true })
    .first()
    .waitFor({ timeout: 120_000 });
  await page
    .getByText(/日 \| 2026-07-22/)
    .first()
    .waitFor({ timeout: 120_000 });

  const staleAllClearCount = await page
    .getByText(/未发现低于 1.33 的 CPK/)
    .count();
  if (staleAllClearCount !== 0) {
    throw new Error("刷新缓存后仍显示无 CPK 预警。");
  }
  const callbackRerunWarningCount = await page
    .getByText(/Calling st\.rerun\(\) within a callback is a no-op\./)
    .count();
  if (callbackRerunWarningCount !== 0) {
    throw new Error("刷新缓存回调仍在调用无效的 st.rerun()。");
  }

  return {
    product: await productSelector.getAttribute("aria-label"),
    alert: await page.getByText(/检测到 \d+ 条 CPK 预警/).innerText(),
    indicator: await page
      .getByText("TP | 41260 | 4PP_Rs", { exact: true })
      .first()
      .innerText(),
    alertDate: "2026-07-22",
  };
}
