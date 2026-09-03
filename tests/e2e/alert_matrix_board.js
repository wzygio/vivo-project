async page => {
  // 自动预警看板 预警矩阵 E2E（隔离 fixture，tests/e2e/fixtures/alert_matrix_app.py，端口 8512）
  // 覆盖：矩阵区渲染存在性、四态单元格、点击 🔴 懒加载详情、点击非 🔴 说明文案、
  //       st.cache_data 命中与「刷新缓存后矩阵重建」。
  // 定位锚点：每个单元格容器的 st-key-matrix_cell_<row>_<prod> class（help tooltip
  // 会把 button 渲染两次，故不以 button 文本计数，而以 keyed 容器为准）。
  const base = "http://localhost:8512/";
  await page.goto(base);
  await page
    .getByText("🚦 预警矩阵", { exact: false })
    .first()
    .waitFor({ timeout: 120_000 });

  // ---- 1. 渲染存在性：标题 / 图例 / 行标签 / 产品列头 ----
  await page
    .getByText(/🟢 达标（无预警）｜🔴 有预警（点击查看详情）/)
    .waitFor({ timeout: 30_000 });
  // 行区在标题/图例之后渲染，等待最后一行出现再断言
  await page.getByText("Q-Time 单片异常", { exact: true }).waitFor({ timeout: 60_000 });
  const rowKeys = [
    "aoi_rs_sheet_oos",
    "aoi_tt_sheet_oos",
    "spc_sheet_oos",
    "spc_cpk_trend",
    "ctq_sheet_oos",
    "yield_lot_oos",
    "yield_trend_fluctuation",
    "qtime_sheet_oos",
  ];
  const products = ["M678", "Z571"];
  const rowLabels = [
    "AOI_RS 单片异常",
    "AOI_TT 单片异常",
    "SPC 单片异常",
    "SPC 趋势波动（CPK）",
    "CTQ 单片异常",
    "Yield 单片异常（Lot 超规）",
    "Yield 趋势波动",
    "Q-Time 单片异常",
  ];
  const bodyText = await page.evaluate(() => document.body.innerText);
  for (const label of [...rowLabels, "M678", "Z571", "监控参数"]) {
    if (!bodyText.includes(label)) {
      throw new Error(`矩阵缺少预期文本: ${label}`);
    }
  }

  // ---- 2. 四态单元格分布（fixture：🔴×4 ⚪×1 ⬜×1 🟢×10）----
  const cellStates = await page.evaluate(
    ({ rowKeys, products }) => {
      const states = {};
      for (const rowKey of rowKeys) {
        for (const prod of products) {
          const container = document.querySelector(
            `.st-key-matrix_cell_${rowKey}_${prod}`,
          );
          if (!container) {
            states[`${rowKey}|${prod}`] = "MISSING";
            continue;
          }
          const button = container.querySelector("button");
          states[`${rowKey}|${prod}`] = button
            ? button.innerText.trim()
            : "NO_BUTTON";
        }
      }
      return states;
    },
    { rowKeys, products },
  );
  const stateValues = Object.values(cellStates);
  const tally = icon => stateValues.filter(value => value === icon).length;
  const counts = {
    alert: tally("🔴"),
    noData: tally("⚪"),
    error: tally("⬜"),
    ok: tally("🟢"),
  };
  const expected = { alert: 4, noData: 1, error: 1, ok: 10 };
  for (const [key, want] of Object.entries(expected)) {
    if (counts[key] !== want) {
      throw new Error(
        `四态数量不符 ${key}: 实际 ${counts[key]}，期望 ${want}；明细 ${JSON.stringify(cellStates)}`,
      );
    }
  }

  // 单元格点击：rerun 会替换 DOM，每次点击前重新定位 keyed 容器内的按钮。
  const clickCell = async (rowKey, prod) => {
    const selector = `.st-key-matrix_cell_${rowKey}_${prod} button`;
    await page.waitForSelector(selector, { timeout: 60_000 });
    await page.locator(selector).first().click();
  };

  const waitBodyText = async (pattern, timeout = 120_000) => {
    await page.waitForFunction(
      source => new RegExp(source).test(document.body.innerText),
      pattern.source,
      { timeout },
    );
  };

  // ---- 3. 点击 🔴（Q-Time × M678）→ 详情懒加载（明细表 + 图像容器）----
  await clickCell("qtime_sheet_oos", "M678");
  await waitBodyText(/🔍 预警详情｜M678 × Q-Time 单片异常/);
  await waitBodyText(/检测到 1 条已确认真实超规/);
  await page
    .locator('div[data-testid="stPlotlyChart"]')
    .first()
    .waitFor({ timeout: 180_000 });
  await page.screenshot({
    path: "output/test-results/alert_matrix_detail_qtime_e2e.png",
    fullPage: false,
  });

  // ---- 4. 点击 🟢 → 达标说明，无详情 ----
  await clickCell("aoi_rs_sheet_oos", "M678");
  await waitBodyText(/该产品该项上一周期无预警（达标）。/);

  // ---- 5. 点击 ⚪（CTQ × M678）→ 无数据说明 ----
  await clickCell("ctq_sheet_oos", "M678");
  await waitBodyText(/修饰工作簿不存在/);

  // ---- 6. 点击 ⬜（Yield Lot × Z571）→ 失败原因 ----
  await clickCell("yield_lot_oos", "Z571");
  await waitBodyText(/加载失败：修饰工作簿读取失败/);
  await page.screenshot({
    path: "output/test-results/alert_matrix_board_e2e.png",
    fullPage: false,
  });

  // ---- 7. 缓存命中与刷新重建（?mode=cache，真实 st.cache_data 包装）----
  await page.goto(`${base}?mode=cache`);
  await page.getByText(/matrix-build-token:/).waitFor({ timeout: 120_000 });
  const readToken = async () =>
    (await page.getByText(/matrix-build-token:/).innerText()).trim();

  const tokenInitial = await readToken();
  // 普通 rerun（点击单元格）→ 命中缓存，令牌不变
  await clickCell("aoi_rs_sheet_oos", "M678");
  await page.waitForTimeout(3_000);
  const tokenAfterRerun = await readToken();
  if (tokenAfterRerun !== tokenInitial) {
    throw new Error(
      `普通 rerun 未命中矩阵缓存: ${tokenInitial} -> ${tokenAfterRerun}`,
    );
  }
  // 刷新缓存 → 令牌必须变化（generated_at 秒级分辨率，至多重试 3 次）
  let tokenRebuilt = tokenInitial;
  for (let attempt = 0; attempt < 3 && tokenRebuilt === tokenInitial; attempt += 1) {
    await page.locator(".st-key-matrix_fixture_refresh button").first().click();
    await page
      .waitForFunction(
        previous =>
          !document.body.innerText.includes(`matrix-build-token: ${previous}`),
        tokenInitial.replace("matrix-build-token: ", ""),
        { timeout: 30_000 },
      )
      .catch(() => {});
    tokenRebuilt = await readToken();
  }
  if (tokenRebuilt === tokenInitial) {
    throw new Error("刷新缓存后矩阵未重建（构建令牌未变化）。");
  }
  await page.screenshot({
    path: "output/test-results/alert_matrix_cache_rebuild_e2e.png",
    fullPage: false,
  });

  return { counts, tokenInitial, tokenRebuilt };
}
