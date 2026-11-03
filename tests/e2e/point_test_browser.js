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
    await page.getByText('Z576 · 1 条明细 · 36 条膜位汇总', { exact: true }).waitFor();
    for (const stage of ['调整前', '调整后', '修正后']) {
      await page.getByRole('tab', { name: stage, exact: true }).click();
      await page.getByRole('tab', { name: stage, exact: true }).getAttribute('aria-selected').then(value => {
        if (value !== 'true') throw new Error(`Stage not selected: ${stage}`);
      });
    }
    await choose('point_test_product', 'M626（测试注释）');
    await page.getByText('M626 · 1 条明细 · 36 条膜位汇总', { exact: true }).waitFor();
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
    await page.getByRole('tab', { name: '膜位汇总', exact: true }).click();
    for (const width of [1440, 768, 375]) {
      await page.setViewportSize({ width, height: 1000 });
      await page.getByRole('heading', { name: '密集点测试报表' }).scrollIntoViewIfNeeded();
      const size = await page.evaluate(() => ({ scroll: document.documentElement.scrollWidth, client: document.documentElement.clientWidth }));
      if (size.scroll > size.client + 2) throw new Error(`Page overflow at ${width}: ${JSON.stringify(size)}`);
      await page.screenshot({ path: path.join(output, `normal-${width}.png`), fullPage: true });
    }
    await choose('fixture_point_mode', 'empty');
    await page.getByText('该产品暂无密集点测试数据。', { exact: true }).waitFor();
    if (await tables.count()) throw new Error('Stale tables in empty state');
    await choose('fixture_point_mode', 'failure');
    await page.getByText('暂时无法读取密集点测试数据，请稍后刷新重试。', { exact: true }).waitFor();
    if (await tables.count()) throw new Error('Stale tables in failure state');
    const text = await page.locator('body').innerText();
    if (/PRIVATE_POINT_TEST_FAILURE|admin=true|Traceback/.test(text)) throw new Error('Technical error leaked');
    await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true });
    await choose('fixture_point_mode', 'normal');
    await page.getByText('Z576 · 1 条明细 · 36 条膜位汇总', { exact: true }).waitFor();
    // Same actual page and production cache/composition, now using the supplied workbook.
    await choose('fixture_point_mode', 'live');
    await page.getByText('Z576 · 936 条明细 · 216 条膜位汇总', { exact: true }).waitFor({ timeout: 180000 });
    await page.setViewportSize({ width: 1440, height: 1080 });
    await page.waitForTimeout(500);
    await page.screenshot({ path: path.join(output, 'live-workbook.png'), fullPage: true });
    if (errors.length) throw new Error(errors.join('\n'));
    fs.writeFileSync(path.join(output, 'result.json'), JSON.stringify({
      passed: true, widths: [1440, 768, 375], scenarios: ['normal', 'empty', 'failure', 'recovery', 'live'],
      productSwitch: true, stageSwitch: true, nativeCsv: true, pageErrors: errors,
    }, null, 2));
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
