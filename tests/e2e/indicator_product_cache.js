async page => {
  const base = "http://localhost:8518/";
  const output = "D:/wzy/Python/vivo-project/output/test-results/indicator-product-cache";
  const text = value => page.getByText(value, {exact:true});
  const wait = value => text(value).waitFor({timeout:60000});
  const button = key => page.locator(`.st-key-${key} button:visible`).first();
  const count = async (indicator, product, value) => wait(`count:${indicator}:${product}=${value}`);
  const state = async (indicator, product, expected) => {
    const locator = button(`matrix_cell_${indicator}_${product}`);
    await locator.waitFor();
    const label = (await locator.innerText()).trim();
    if (label !== expected) throw new Error(`${indicator}/${product}: ${label} != ${expected}`);
  };
  const choose = async (key, label) => {
    const option = page.getByRole("option", {name:label, exact:true});
    for (let attempt = 0; attempt < 5; attempt++) {
      if (!await option.isVisible()) {
        await page.locator(`.st-key-${key}`).getByRole("button", {name:"Open",exact:true}).click();
      }
      try { await option.waitFor({state:"visible",timeout:2000}); break; }
      catch (error) { if (attempt === 4) throw error; }
    }
    await option.click();
  };
  await page.setViewportSize({width:1920,height:1080});
  await page.goto(base);
  await count("yield_lot_oos", "M626", 1);
  await count("yield_trend_fluctuation", "M678", 1);
  if (await page.locator(".st-key-matrix_refresh_cell").count()) throw new Error("ordinary user sees refresh control");
  await state("spc_cpk_trend", "M626", "🔴");
  await button("matrix_cell_spc_cpk_trend_M626").click();
  await page.getByText("CPK 预警明细（上一周 2026-W36，CPK < 1.33）", {exact:true}).waitFor();
  await count("spc_cpk_trend", "M626", 1);
  await button("fixture_cpk_edit").click();
  await wait("CPK edited: True");
  await state("spc_cpk_trend", "M626", "🟢");
  await count("yield_lot_oos", "M626", 1);
  await count("yield_trend_fluctuation", "M678", 1);
  await button("fixture_compliance_on").click();
  await wait("Compliance enabled: True");
  await state("spc_cpk_trend", "M678", "🟢");
  await button("matrix_cell_spc_cpk_trend_M678").click();
  await wait("该产品该项上一周期无预警（达标）。");
  await count("yield_lot_oos", "M626", 1);
  await page.screenshot({path:`${output}/regular.png`,fullPage:true});
  await page.goto(`${base}?admin=true`);
  await count("yield_lot_oos", "M626", 1);
  await page.getByText("指标缓存管理（管理员）", {exact:true}).click();
  await button("matrix_refresh_cell").waitFor();
  await choose("matrix_refresh_indicator", "Yield 单片异常（Lot 超规）");
  await choose("matrix_refresh_product", "M626");
  await button("matrix_refresh_cell").click();
  await count("yield_lot_oos", "M626", 2);
  await count("yield_lot_oos", "M678", 1);
  await count("yield_trend_fluctuation", "M626", 1);
  await count("yield_trend_fluctuation", "M678", 1);
  await count("spc_cpk_trend", "M626", 1);
  await page.screenshot({path:`${output}/admin.png`,fullPage:true});
  await page.setViewportSize({width:768,height:900});
  const sizes = await page.evaluate(() => ({actual:document.documentElement.scrollWidth,visible:document.documentElement.clientWidth}));
  if (sizes.actual > sizes.visible + 2) throw new Error(`viewport overflow ${JSON.stringify(sizes)}`);
  await page.screenshot({path:`${output}/admin-768.png`,fullPage:true});
  if (await page.locator('[data-testid="stException"]').count()) throw new Error("Streamlit exception");
  return "PASS: Excel-only CPK; compliance details; admin-only scoped refresh; cross-product and cross-indicator counters; 1920/768 viewport";
}
