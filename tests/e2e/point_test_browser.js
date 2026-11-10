// Run only through run_point_test_browser.py, which owns and cleans all processes.
const { chromium } = require(process.env.PLAYWRIGHT_MODULE_PATH);
const path = require('node:path');
const fs = require('node:fs');

(async () => {
  const output = process.env.VIVO_POINT_BROWSER_OUTPUT;
  const browser = await chromium.launch({ headless: true, channel: 'chrome' });
  const errors = [];
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1080 }, acceptDownloads: true });
    // Headless Chrome cannot operate the OS file picker. Exercise Streamlit's
    // native browser-download fallback with the same CSV payload instead.
    await page.addInitScript(() => {
      window.showSaveFilePicker = async () => { throw new DOMException('Headless download fallback', 'NotSupportedError'); };
    });
    page.on('pageerror', error => errors.push(error.message));
    const tables = page.locator('[data-testid="stDataFrame"]');
    const choose = async (key, value) => {
      const combo = page.locator(`.st-key-${key}`).getByRole('combobox');
      await combo.scrollIntoViewIfNeeded();
      await combo.press('ArrowDown');
      await page.getByRole('option', { name: value, exact: true }).click();
    };
    await page.goto(process.env.VIVO_POINT_BROWSER_URL);
    await page.getByRole('heading', { name: '密集点测试报表' }).waitFor();
    await page.getByText('Z576 · 1 条明细 · 12 条膜位汇总', { exact: true }).waitFor();
    if (await page.getByRole('tab', { name: /调整前|调整后|修正后/ }).count()) throw new Error('Old stage tabs remain');
    const summary = tables.filter({ visible: true }).first();
    await summary.hover();
    const [summaryDownload] = await Promise.all([
      page.waitForEvent('download'),
      summary.getByRole('button', { name: /download|下载/i }).click(),
    ]);
    const summaryPath = path.join(output, 'summary-Z576.csv');
    await summaryDownload.saveAs(summaryPath);
    const summaryCsv = fs.readFileSync(summaryPath, 'utf8');
    if (/阶段|调整前|调整后|修正后/.test(summaryCsv)) throw new Error('Stage leaked into summary CSV');
    if (summaryCsv.trim().split(/\r?\n/).length !== 13) throw new Error('Summary must contain 12 corrected rows');
    await page.getByRole('tab', { name: '明细箱线图', exact: true }).click();
    const zGroup = page.locator('[data-testid="stExpander"]').filter({ hasText: 'Z576 / 1B455 / CD1' });
    await zGroup.locator('summary').click();
    await zGroup.locator('details:not([open])').waitFor({ state: 'attached' });
    await zGroup.locator('summary').click();
    await page.locator('.js-plotly-plot').filter({ visible: true }).waitFor();
    await choose('point_test_product', 'M626（测试注释）');
    await page.getByText('M626 · 1 条明细 · 12 条膜位汇总', { exact: true }).waitFor();
    await page.locator('[data-testid="stExpander"]').filter({ hasText: 'M626 / 1B455 / CD1' }).waitFor({ state: 'attached' });
    await page.getByRole('tab', { name: '明细数据', exact: true }).click();
    await tables.filter({ visible: true }).first().waitFor();
    // Native CSV contains the actual current product's full table.
    const table = tables.filter({ visible: true }).first();
    await table.hover();
    fs.writeFileSync(path.join(output, 'table-dom.html'), await table.evaluate(el => el.outerHTML));
    await page.screenshot({ path: path.join(output, 'detail-before-download.png'), fullPage: true });
    const [download] = await Promise.all([
      page.waitForEvent('download'),
      table.getByRole('button', { name: /download|下载/i }).click(),
    ]);
    const downloadPath = path.join(output, 'detail-M626.csv');
    await download.saveAs(downloadPath);
    if (!fs.readFileSync(downloadPath, 'utf8').includes('7.5')) throw new Error('CSV product values incorrect');
    await page.getByRole('tab', { name: '明细箱线图', exact: true }).click();
    for (const width of [1440, 768, 375]) {
      await page.setViewportSize({ width, height: 1000 });
      await page.getByRole('heading', { name: '密集点测试报表' }).scrollIntoViewIfNeeded();
      const size = await page.evaluate(() => ({ scroll: document.documentElement.scrollWidth, client: document.documentElement.clientWidth }));
      if (size.scroll > size.client + 2) throw new Error(`Page overflow at ${width}: ${JSON.stringify(size)}`);
      await page.screenshot({ path: path.join(output, `normal-${width}.png`), fullPage: true });
    }
    await choose('fixture_point_mode', 'empty');
    await page.getByText('该产品暂无密集点测试数据。', { exact: true }).waitFor();
    if (await tables.count() || await page.locator('.js-plotly-plot').count()) throw new Error('Stale content in empty state');
    await choose('fixture_point_mode', 'failure');
    await page.getByText('暂时无法读取密集点测试数据，请稍后刷新重试。', { exact: true }).waitFor();
    if (await tables.count() || await page.locator('.js-plotly-plot').count()) throw new Error('Stale content in failure state');
    const text = await page.locator('body').innerText();
    if (/PRIVATE_POINT_TEST_FAILURE|admin=true|Traceback/.test(text)) throw new Error('Technical error leaked');
    await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true });
    await choose('fixture_point_mode', 'normal');
    await page.getByText('Z576 · 1 条明细 · 12 条膜位汇总', { exact: true }).waitFor();
    // Same actual page and production cache/composition, now using the supplied workbook.
    await choose('fixture_point_mode', 'live');
    await page.getByText('Z576 · 936 条明细 · 72 条膜位汇总', { exact: true }).waitFor({ timeout: 180000 });
    await page.getByRole('tab', { name: '明细箱线图', exact: true }).click();
    const plot = page.locator('.js-plotly-plot').filter({ visible: true });
    await plot.waitFor();
    await page.waitForFunction(() => Array.from(document.querySelectorAll('.js-plotly-plot')).some(el => el.calcdata?.length === 6));
    const boxes = await plot.evaluate(el => ({
      samples: el.data.map(trace => trace.y.length),
      quartiles: el.calcdata.map(trace => ({ q1: trace[0].q1, median: trace[0].med, q3: trace[0].q3 })),
    }));
    if (boxes.samples.some(count => count !== 156)) throw new Error('Incorrect detail sample population');
    if (boxes.quartiles.some(box => !(box.q1 < box.median && box.median < box.q3))) throw new Error('Quartiles were not computed');
    await page.setViewportSize({ width: 1440, height: 1080 });
    await page.waitForTimeout(500);
    await page.screenshot({ path: path.join(output, 'live-workbook.png'), fullPage: true });
    if (errors.length) throw new Error(errors.join('\n'));
    fs.writeFileSync(path.join(output, 'result.json'), JSON.stringify({
      passed: true, widths: [1440, 768, 375], scenarios: ['normal', 'empty', 'failure', 'recovery', 'live'],
      productSwitch: true, correctedSummaryOnly: true, groupedExpanders: true, nativeCsv: true, boxes, pageErrors: errors,
    }, null, 2));
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
