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

  const selectProduct = async product => {
    const selector = page.getByRole("combobox", { name: "📦 当前产品型号" });
    if ((await selector.inputValue()) === product) return;
    await selector.click();
    await page.getByRole("option", { name: product, exact: true }).click();
    const loading = page.getByText("正在加载 AOI TT 数据...");
    await loading.waitFor({ state: "visible", timeout: 5_000 }).catch(() => {});
    await loading.waitFor({ state: "hidden", timeout: 600_000 });
  };

  await page.goto("http://localhost:8503/AOI_TT监控报表");
  await page
    .getByText("正在加载 AOI TT 数据...")
    .waitFor({ state: "hidden", timeout: 600_000 });

  const title = page.getByRole("heading", {
    name: "AOI_TT监控报表",
    exact: true,
  });
  await title.waitFor({ timeout: 120_000 });

  // 使用已知同时具备 ARRAY/TDSUM 与缺陷明细的产品，避免沿用浏览器会话中的产品。
  await selectProduct("M678");

  // 空数据降级分支不视为失败加载，但本用例要求有数据
  const noData = page.getByText("当前产品暂无可展示的 AOI TT 数据。");
  if (await noData.count()) {
    throw new Error("AOI_TT 页面加载为空数据分支，无法继续 E2E。");
  }

  const filterHeading = page.getByRole("heading", { name: "筛选", exact: true });
  await filterHeading.waitFor({ timeout: 120_000 });

  // Particle Size 多选默认必须是 Total/S/M/L/H 全选。
  for (const size of ["Total", "S", "M", "L", "H"]) {
    await page
      .getByRole("button", { name: `${size}, close by backspace`, exact: true })
      .waitFor({ timeout: 120_000 });
  }

  const filterBoxes = await Promise.all(
    ["厂别", "站点", "Code名称", "Particle Size"].map(label =>
      page.getByRole("combobox", { name: label }).boundingBox(),
    ),
  );
  if (filterBoxes.some(box => !box)) {
    throw new Error("AOI_TT 筛选行存在不可见控件");
  }
  const filterCenters = filterBoxes.map(box => box.y + box.height / 2);
  if (Math.max(...filterCenters) - Math.min(...filterCenters) > 8) {
    throw new Error(`Particle Size 未与其他筛选框对齐：${filterCenters.join(", ")}`);
  }

  // 厂别固定选 ARRAY（TT 参数 TDSUM 数据量最大）
  if ((await comboInputValue("厂别")) !== "ARRAY") {
    await page.getByRole("combobox", { name: "厂别" }).click();
    await page.getByRole("option", { name: "ARRAY", exact: true }).click();
  }

  // 站点多选：显式选 11620（ARRAY 首个 AOI 站点）
  const stepSelector = page.getByRole("combobox", { name: "站点" });
  const firstStep = page.getByRole("option", {
    name: "11620 PSI_FI_AOI",
    exact: true,
  });
  let stepSelected = false;
  for (let attempt = 0; attempt < 60; attempt += 1) {
    await stepSelector.click();
    if (await firstStep.count()) {
      await firstStep.click();
      stepSelected = true;
      break;
    }
    await page.keyboard.press("Escape");
    await page.waitForTimeout(500);
  }
  if (!stepSelected) throw new Error("未找到 M678 的 11620 PSI_FI_AOI 站点");
  await page
    .getByRole("button", { name: /^11620.*close by backspace$/ })
    .waitFor({ timeout: 120_000 });
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

  for (const size of ["Total", "S", "M", "L", "H"]) {
    await page
      .getByText(`Particle Size：${size}`, { exact: true })
      .first()
      .waitFor({ timeout: 300_000 });
  }

  await page.waitForFunction(
    () => document.querySelectorAll(".js-plotly-plot").length >= 15,
    undefined,
    { timeout: 300_000 },
  );

  const chartCount = await page.locator(".js-plotly-plot").count();
  if (chartCount < 15 || chartCount % 15 !== 0) {
    throw new Error(`全选图表数量不符合每参数 5 粒径 × 3 图：实际 ${chartCount}`);
  }

  await page.screenshot({
    path: "output/test-results/aoi-tt-particle-size/all-particle-sizes.png",
    fullPage: false,
  });

  // 探索性组合筛选：只保留 Total；S/M/L/H 图和标签都必须消失。
  const particleSelector = page.getByRole("combobox", { name: /Particle Size$/ });
  for (const size of ["H", "L", "M", "S"]) {
    await particleSelector.focus();
    await page.keyboard.press("Backspace");
    await page
      .getByRole("button", { name: `${size}, close by backspace`, exact: true })
      .waitFor({ state: "hidden", timeout: 120_000 });
  }
  await page
    .getByText("Particle Size：S", { exact: true })
    .waitFor({ state: "hidden", timeout: 300_000 });
  const totalOnlyCount = await page.locator(".js-plotly-plot").count();
  if (totalOnlyCount * 5 !== chartCount) {
    throw new Error(
      `单粒径图表数量未按 5:1 收敛：全选 ${chartCount}，Total-only ${totalOnlyCount}`,
    );
  }
  await page.keyboard.press("Escape");
  await page.screenshot({
    path: "output/test-results/aoi-tt-particle-size/total-only.png",
    fullPage: false,
  });
  return `AOI_TT Particle Size E2E 通过：站点 ${firstStepName}，全选 ${chartCount} 图，Total-only ${totalOnlyCount} 图`;
}
