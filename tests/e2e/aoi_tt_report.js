async page => {
  // Streamlit 1.60：单选 combobox 的选中值在 input 的 value 属性（aria-label 不再含 "Selected X"）；
  // 多选选中项渲染为容器内标签文本。
  const comboInputValue = label =>
    page.evaluate(l => {
      const el = [...document.querySelectorAll('[role="combobox"]')].find(
        e => e.getAttribute("aria-label") === l,
      );
      return el?.value || "";
    }, label);
  const waitQueryEnabled = (timeout = 300_000) =>
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

  await page.goto("http://localhost:8503/AOI_TT监控报表");
  await page
    .getByText("正在加载 AOI TT 数据...")
    .waitFor({ state: "hidden", timeout: 600_000 });

  const title = page.getByRole("heading", {
    name: "AOI_TT监控报表",
    exact: true,
  });
  await title.waitFor({ timeout: 120_000 });

  // 空数据降级分支不视为失败加载，但本用例要求有数据
  const noData = page.getByText("当前产品暂无可展示的 AOI TT 数据。");
  if (await noData.count()) {
    throw new Error("AOI_TT 页面加载为空数据分支，无法继续 E2E。");
  }

  const filterHeading = page.getByRole("heading", { name: "筛选", exact: true });
  await filterHeading.waitFor({ timeout: 120_000 });

  // 厂别固定选 ARRAY（TT 参数 TDSUM 数据量最大）
  if ((await comboInputValue("厂别")) !== "ARRAY") {
    await page.getByRole("combobox", { name: "厂别" }).click();
    await page.getByRole("option", { name: "ARRAY", exact: true }).click();
  }

  // 站点多选：显式选 11620（ARRAY 首个 AOI 站点）
  const stepSelector = page.getByRole("combobox", { name: "站点" });
  await stepSelector.click();
  const firstStep = page.getByRole("option", { name: "11620", exact: true });
  await firstStep.click();
  await page.keyboard.press("Escape");
  const firstStepName = "11620";

  // Code名称（TT 参数）随站点自动全选后查询按钮启用
  await waitQueryEnabled(300_000);
  await page.getByRole("button", { name: "查询" }).click();

  // 报表区：站点分组标题 + 每 TT 三图
  const groupHeading = page.getByRole("heading", {
    name: new RegExp(`ARRAY \\| 站点 ${firstStepName}`),
  });
  await groupHeading.waitFor({ timeout: 300_000 });

  await page
    .getByText("月周天趋势（平均每片 TT 个数）")
    .first()
    .waitFor({ timeout: 300_000 });
  await page
    .getByText("By Lot（Lot 内平均每片 TT 个数）")
    .first()
    .waitFor({ timeout: 300_000 });
  await page
    .getByText("By Sheet（每片的 TT 个数）")
    .first()
    .waitFor({ timeout: 300_000 });

  const chartCount = await page.locator(".js-plotly-plot").count();
  if (chartCount < 3) {
    throw new Error(`图表数量不足：期望 >=3，实际 ${chartCount}`);
  }

  await page.screenshot({
    path: "output/screenshots/aoi_tt_e2e.png",
    fullPage: false,
  });
  return `AOI_TT E2E 通过：站点 ${firstStepName}，渲染图表 ${chartCount} 张`;
}
