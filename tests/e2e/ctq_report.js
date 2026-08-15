async page => {
  await page.goto("http://localhost:8503/CTQ%E7%9B%91%E6%8E%A7%E6%8A%A5%E8%A1%A8");
  await page
    .getByText("正在加载 CTQ 分布数据...")
    .waitFor({ state: "hidden", timeout: 300_000 });

  const title = page.getByRole("heading", {
    name: "CTQ监控报表",
    exact: true,
  });
  await title.waitFor({ timeout: 120_000 });

  const noData = page.getByText("当前产品暂无可展示的 CTQ 数据。");
  if (await noData.count()) {
    throw new Error("CTQ 页面加载为空数据分支，无法继续 E2E。");
  }

  const filterHeading = page.getByRole("heading", { name: "筛选", exact: true });
  await filterHeading.waitFor({ timeout: 120_000 });

  // 站点多选：选第一个可选站点
  const stepSelector = page.getByRole("combobox", { name: "站点" });
  await stepSelector.click();
  const firstStepOption = page.getByRole("option", { name: /^\d+$/ }).first();
  const firstStepName = (await firstStepOption.textContent()).trim();
  await firstStepOption.click();
  await page.keyboard.press("Escape");

  const queryButton = page.getByRole("button", { name: "查询" });
  await queryButton.waitFor({ state: "visible" });
  await page.waitForFunction(
    () => {
      const btn = [...document.querySelectorAll("button")].find(
        b => b.textContent?.trim() === "查询",
      );
      return btn && !btn.disabled;
    },
    undefined,
    { timeout: 300_000 },
  );
  await queryButton.click();

  // 分布图渲染（Sheet/点位分布 + 月周天分布）
  await page
    .getByText(/月周天分布/)
    .first()
    .waitFor({ timeout: 300_000 });

  const chartCount = await page.locator(".js-plotly-plot").count();
  if (chartCount < 1) {
    throw new Error(`CTQ 图表数量不足：实际 ${chartCount}`);
  }

  await page.screenshot({
    path: "output/test-results/ctq_e2e.png",
    fullPage: false,
  });
  return `CTQ E2E 通过：站点 ${firstStepName}，渲染图表 ${chartCount} 张`;
}
