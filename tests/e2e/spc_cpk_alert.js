async page => {
  // Streamlit 1.60 兼容：选中值从 input value 读取，不依赖 "Selected X" aria-label；
  // 不等待 toast（短生命周期提示在重渲染高峰可能被错过）。
  const waitComboValue = (label, value, timeout = 600_000) =>
    page.waitForFunction(
      ([l, v]) => [...document.querySelectorAll('[role="combobox"]')].some(
        e => e.getAttribute("aria-label") === l && (e.value || "").includes(v),
      ),
      [label, value],
      { timeout },
    );

  await page.goto("http://localhost:8503/SPC监控报表?admin=true");
  await page
    .getByText("正在加载 SPC 分布数据...")
    .waitFor({ state: "hidden", timeout: 600_000 });

  const productSelector = page.getByRole("combobox").first();
  if ((await productSelector.inputValue()) !== "M673") {
    await productSelector.click();
    await page.getByRole("option", { name: "M673" }).click();
  }

  await waitComboValue("📦 当前产品型号", "M673");
  await page
    .getByRole("heading", { name: "筛选", exact: true })
    .waitFor({ timeout: 300_000 });

  await page.getByRole("button", { name: "🔄 刷新缓存" }).click();
  await page
    .getByText("正在加载 SPC 分布数据...")
    .waitFor({ state: "hidden", timeout: 600_000 })
    .catch(() => {});
  await page
    .getByRole("heading", { name: "筛选", exact: true })
    .waitFor({ timeout: 300_000 });

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
    { timeout: 300_000 },
  );

  const alertMessage = page.getByText(/检测到 \d+ 条 CPK 预警/);
  const allClearMessage = page.getByText(/未发现低于 1.33 的 CPK/);
  const hasAlerts = (await alertMessage.count()) > 0;
  if (hasAlerts) {
    await page
      .getByText(/🚨 自动预警指标图像（\d+ 个指标）/)
      .first()
      .waitFor({ timeout: 300_000 });
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
        .getByText(/🚨 自动预警指标图像（\d+ 个指标）/)
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
    product: await productSelector.inputValue(),
    status: hasAlerts ? await alertMessage.innerText() : await allClearMessage.innerText(),
  };
}
