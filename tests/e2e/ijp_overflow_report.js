async page => {
  // IJP 溢流监控报表 E2E：门控 → 正常查询 → 图表/表格 → 空分支 → 错误分支 → viewport-fit
  await page.setViewportSize({ width: 1365, height: 768 });
  await page.goto("http://localhost:8511");
  await page
    .getByRole("heading", { name: "OLED IJP 溢流监控" })
    .waitFor({ timeout: 120_000 });

  // 1. 查询门控：未点击前只有提示
  await page.getByText("请选择筛选条件并点击“查询”。").waitFor({ timeout: 60_000 });

  // 2. 默认筛选直接查询：图表 + 明细表出现
  await page.getByRole("button", { name: "查询" }).click();
  await page.locator(".js-plotly-plot").waitFor({ timeout: 120_000 });
  await page.locator('[data-testid="stDataFrame"]').waitFor({ timeout: 60_000 });

  const traces = await page.evaluate(() =>
    (document.querySelector(".js-plotly-plot")?.data || []).map(t => t.type),
  );
  const barTraces = traces.filter(t => t === "bar").length;
  if (barTraces < 3) {
    throw new Error(`By天 堆叠图 traces 不足：期望 >=3 条 bar，实际 ${traces}`);
  }

  // 3. viewport-fit：1365×768 下不允许页面级横向滚动
  const overflowX = await page.evaluate(
    () => document.documentElement.scrollWidth - document.documentElement.clientWidth,
  );
  if (overflowX > 0) {
    throw new Error(`页面级横向滚动超出 ${overflowX}px`);
  }
  await page.screenshot({
    path: "output/test-results/ijp/ijp_report_main.png",
    fullPage: true,
  });

  // 4. 修改筛选（CODE=C3BH2）后结果失效，回到门控提示
  // Streamlit 1.60 多选下拉为虚拟列表，仅渲染前 10 项：键入过滤后再点选；
  // 选中后 aria-label 变为 "Selected X. CODE"，故用属性包含匹配
  const codeCombo = page.locator('[role="combobox"][aria-label*="CODE"]');
  await codeCombo.click();
  await codeCombo.pressSequentially("C3BH2");
  await page.getByRole("option", { name: "C3BH2", exact: true }).click();
  await page.keyboard.press("Escape");
  await page.getByText("请选择筛选条件并点击“查询”。").waitFor({ timeout: 60_000 });

  // 5. 空数据分支
  await page.getByRole("button", { name: "查询" }).click();
  await page
    .getByText("当前筛选条件下暂无 IJP 溢流数据。")
    .waitFor({ timeout: 60_000 });
  await page.screenshot({ path: "output/test-results/ijp/ijp_report_empty.png" });

  // 6. 错误分支：取消 CODE（Baseweb tag 支持 Backspace 移除），选择产品型号 M678 触发安全错误文案
  await codeCombo.click();
  await page.keyboard.press("Backspace");
  await page.keyboard.press("Escape");
  await page.locator('[role="combobox"][aria-label*="产品型号"]').click();
  await page.getByRole("option", { name: "M678", exact: true }).click();
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "查询" }).click();
  await page
    .getByText("IJP 溢流数据读取失败，请联系系统管理员确认数据库权限。")
    .waitFor({ timeout: 60_000 });
  await page.screenshot({ path: "output/test-results/ijp/ijp_report_error.png" });

  return `IJP E2E 通过：bar traces=${barTraces}，viewport 无横向滚动`;
}
