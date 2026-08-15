async page => {
  // Streamlit 1.60 兼容：选中值从 input value / 容器文本读取，不依赖 "Selected X" aria-label
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

  await page.goto("http://localhost:8503/AOI_RS%E7%9B%91%E6%8E%A7%E6%8A%A5%E8%A1%A8");
  await page
    .getByText("正在加载 AOI RS 数据...")
    .waitFor({ state: "hidden", timeout: 600_000 });

  const title = page.getByRole("heading", {
    name: "AOI_RS监控报表",
    exact: true,
  });
  await title.waitFor({ timeout: 120_000 });

  // 空数据降级分支不视为失败加载，但本用例要求有数据
  const noData = page.getByText("当前产品暂无可展示的 AOI RS 数据。");
  if (await noData.count()) {
    throw new Error("AOI_RS 页面加载为空数据分支，无法继续 E2E。");
  }

  const filterHeading = page.getByRole("heading", { name: "筛选", exact: true });
  await filterHeading.waitFor({ timeout: 120_000 });

  // 厂别固定选 ARRAY
  if ((await comboInputValue("厂别")) !== "ARRAY") {
    await page.getByRole("combobox", { name: "厂别" }).click();
    await page.getByRole("option", { name: "ARRAY", exact: true }).click();
  }

  // 站点多选：选第一个数字站点（1.60 下拉首项为 "Select all" 伪选项，需跳过）
  const stepSelector = page.getByRole("combobox", { name: "站点" });
  await stepSelector.click();
  const firstStepOption = page.getByRole("option", { name: /^\d+$/ }).first();
  const firstStepName = (await firstStepOption.textContent()).trim();
  await firstStepOption.click();
  await page.keyboard.press("Escape");

  // Code 名称随站点自动全选后查询按钮启用
  await waitQueryEnabled(300_000);
  await page.getByRole("button", { name: "查询" }).click();

  // 报表区：站点分组标题 + 每 Code 三图（修饰在 service 层完成，此处只验证渲染契约）
  const groupHeading = page.getByRole("heading", {
    name: new RegExp(`ARRAY \\| 站点 ${firstStepName}`),
  });
  await groupHeading.waitFor({ timeout: 300_000 });

  await page
    .getByText("月周天趋势（平均每片 RS 个数）")
    .first()
    .waitFor({ timeout: 300_000 });
  await page
    .getByText("By Lot（Lot 内平均每片 RS 个数）")
    .first()
    .waitFor({ timeout: 300_000 });
  await page
    .getByText("By Sheet（每片的 RS 个数）")
    .first()
    .waitFor({ timeout: 300_000 });

  const chartCount = await page.locator(".js-plotly-plot").count();
  if (chartCount < 3) {
    throw new Error(`图表数量不足：期望 >=3，实际 ${chartCount}`);
  }

  await page.screenshot({
    path: "output/test-results/aoi_rs_e2e.png",
    fullPage: false,
  });
  return `AOI_RS E2E 通过：站点 ${firstStepName}，渲染图表 ${chartCount} 张`;
}
