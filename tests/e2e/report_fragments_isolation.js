async page => {
  // Open report_fragments_app.py with ?report=lifetime|point|ijp before running.
  // The parent driver owns service/browser startup and cleanup.
  const report = new URL(page.url()).searchParams.get('report') || 'lifetime';
  const visible = value => page.getByText(value, {exact: true}).waitFor({timeout: 20000});
  const button = key => page.locator(`.st-key-${key} button`).first();
  const select = async (key, value) => {
    const combo = page.locator(`.st-key-${key}`).getByRole('combobox');
    if (key === 'point_test_product') {
      // A caption delta can arrive before the sibling fragment finishes.
      await page.waitForFunction(() => !document.querySelector('[data-testid="stStatusWidget"]'));
      await combo.fill(value);
      await combo.press('ArrowDown');
      await combo.press('Enter');
    } else {
      await combo.click();
      await page.getByRole('option', {name: value, exact: true}).click();
    }
    await page.keyboard.press('Escape');
  };
  const charts = async count => {
    await page.waitForFunction(expected =>
      document.querySelectorAll('[data-testid="stPlotlyChart"]').length === expected, count);
  };
  const source = async count => {
    await visible(`Fixture source loads: ${count}`);
    await visible(`Fixture page runs: ${count}`);
  };
  const sibling = async (renders, submitted) => {
    await visible(`Sibling renders: ${renders}`);
    await visible(`Sibling submitted result: ${submitted}`);
  };
  const assertNoException = async () => {
    if (await page.locator('[data-testid="stException"]').count()) throw Error('Streamlit exception');
  };
  await source(1);
  await sibling(1, 0);
  await button('fixture_sibling_query').click();
  await sibling(2, 1);
  const checks = [];
  if (report === 'lifetime') {
    await select('iqc_lifetime_products', 'M678');
    await source(1); await sibling(2, 1);
    await button('iqc_lifetime_query').click();
    await charts(3);
    await source(1); await sibling(2, 1);
    await button('fixture_sibling_query').click();
    await sibling(3, 2); await charts(3); await source(1);
    await select('iqc_lifetime_products', 'M626');
    await visible('筛选条件已修改，当前仍展示上次查询结果；点击“查询”更新。');
    await charts(3); await sibling(3, 2); await source(1);
    await button('iqc_lifetime_query').click();
    await charts(6); await source(1); await sibling(3, 2);
    await button('fixture_full_reload').click();
    await source(2); await charts(6); await sibling(4, 2);
    checks.push('linked drafts and query reuse source; submitted charts survive drafts and sibling; full reload reads again');
  } else if (report === 'point') {
    await select('point_test_product', 'Z576');
    await page.getByText('Z576 · 1 条明细 · 12 条膜位汇总', {exact: true}).waitFor();
    await charts(1); await source(1); await sibling(2, 1);
    await button('fixture_sibling_query').click();
    await sibling(3, 2); await charts(1); await source(1);
    await select('point_test_product', 'M626');
    await page.getByText('M626 · 1 条明细 · 12 条膜位汇总', {exact: true}).waitFor();
    await charts(1); await sibling(3, 2); await source(1);
    await button('fixture_full_reload').click();
    await source(2); await charts(1); await sibling(4, 2);
    checks.push('product changes reuse parsed workbook; sibling retains report; full reload reads source again');
  } else if (report === 'ijp') {
    await visible('Fixture region: AA区');
    await select('ijp_product_codes', 'M626');
    await source(1); await sibling(2, 1); await charts(0);
    await button('ijp_search').click();
    await visible('AA queries: 1'); await visible('Fixture result service: AA区');
    await charts(1); await source(1); await sibling(2, 1);
    await button('fixture_sibling_query').click();
    await sibling(3, 2); await charts(1); await source(1);
    await select('ijp_lines', '3CEE01');
    await visible('请选择筛选条件并点击“查询”。');
    await charts(0); await source(1); await sibling(3, 2);
    await button('ijp_search').click();
    await visible('AA queries: 2'); await charts(1); await source(1);
    // Region switching intentionally rebuilds the page's service. Subsequent
    // hole controls must stay local, and the hole query must use that service.
    await select('ijp_region', '孔区');
    await source(2); await visible('Fixture region: 孔区');
    await sibling(4, 2); await charts(0);
    await select('ijp_hole_products', 'M626');
    await source(2); await sibling(4, 2);
    await button('ijp_hole_search').click();
    await visible('Hole queries: 1'); await visible('Fixture result service: 孔区');
    await charts(1); await source(2); await sibling(4, 2);
    await button('fixture_sibling_query').click();
    await sibling(5, 3); await charts(1); await source(2);
    await select('ijp_hole_lines', '3CEE01');
    await visible('请选择筛选条件并点击“查询”。');
    await charts(0); await source(2); await sibling(5, 3);
    await button('ijp_hole_search').click();
    await visible('Hole queries: 2'); await charts(1); await source(2);
    await button('fixture_full_reload').click();
    await source(3); await visible('Hole queries: 2'); await charts(1); await sibling(6, 3);
    checks.push('AA/hole filters and query isolated; stale signature hides charts; region intentionally rebuilds correct service; sibling/results persist');
  } else throw Error(`Unknown report: ${report}`);
  await assertNoException();
  return {passed: true, report, checks};
}
