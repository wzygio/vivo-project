async page => {
  // 前置：resources/aoi_rs_sheet_oos_decoration.xlsx 中 M626 全部 OOS 行已置 flag=Delete
  // 验证：刷新缓存后页面正常重渲染，被 Delete 的 lot 点不再出现在 By Lot 图中
  await page.goto(
    "http://localhost:8503/AOI_RS%E7%9B%91%E6%8E%A7%E6%8A%A5%E8%A1%A8?admin=true",
  );
  await page
    .getByText("正在加载 AOI RS 数据...")
    .waitFor({ state: "hidden", timeout: 300_000 });
  await page
    .getByRole("heading", { name: "AOI_RS监控报表", exact: true })
    .waitFor({ timeout: 120_000 });

  // 刷新缓存（重读工作簿 flag）
  await page.getByRole("button", { name: "🔄 刷新缓存" }).click();
  await page
    .getByText(/缓存已刷新/)
    .waitFor({ timeout: 60_000 });
  await page
    .getByText("正在加载 AOI RS 数据...")
    .waitFor({ state: "hidden", timeout: 300_000 });

  // 重新查询
  const factorySelector = page.getByRole("combobox", { name: "厂别" });
  if (!(await factorySelector.getAttribute("aria-label"))?.includes("ARRAY")) {
    await factorySelector.click();
    await page.getByRole("option", { name: "ARRAY", exact: true }).click();
  }
  const stepSelector = page.getByRole("combobox", { name: "站点" });
  await stepSelector.click();
  await page.getByRole("option", { name: "18629", exact: true }).click();
  await page.keyboard.press("Escape");
  const queryButton = page.getByRole("button", { name: "查询" });
  await page.waitForFunction(
    () => {
      const btn = [...document.querySelectorAll("button")].find(
        b => b.textContent?.trim() === "查询",
      );
      return btn && !btn.disabled;
    },
    { timeout: 60_000 },
  );
  await queryButton.click();

  const groupHeading = page.getByRole("heading", { name: /ARRAY \| 站点 18629/ });
  await groupHeading.waitFor({ timeout: 180_000 });
  await page
    .getByText("By Lot（Lot 内平均每片 RS 个数）")
    .first()
    .waitFor({ timeout: 180_000 });

  // 页面不得出现错误横幅
  const errorBanner = await page.locator('[data-testid="stException"]').count();
  if (errorBanner > 0) {
    throw new Error("页面出现异常横幅（stException）。");
  }

  // 被 Delete 的 lot 不得出现在 A8DMR 任何轨迹的数据点中。
  // 注意：不能用 getByText 断言——lot_id 会作为 x 轴刻度被同站点其他 Code 共享。
  const deletedLots = ["L3MY67002AC", "L3MY6700AAB"];
  const hits = await page.evaluate(lots => {
    const found = [];
    document.querySelectorAll(".js-plotly-plot").forEach(gd => {
      const title = gd.layout?.title?.text || "";
      (gd.data || []).forEach(t => {
        if ((t.name || "").includes("A8DMR") && !(t.name || "").includes("规格")) {
          lots.forEach(x => {
            if ((t.x || []).includes(x)) found.push({ title, trace: t.name, x });
          });
        }
      });
    });
    return found;
  }, deletedLots);
  if (hits.length !== 0) {
    throw new Error(`flag=Delete 的图点仍在轨迹数据中：${JSON.stringify(hits)}`);
  }

  await page.screenshot({
    path: "output/test-results/aoi_rs_delete_e2e.png",
    fullPage: false,
  });
  return "AOI_RS Delete E2E 通过：被删除图点不再展示，页面无异常";
}
