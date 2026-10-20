// Run through run_aoi_rs_monitor_browser.py so service ownership is always cleaned.
const { chromium } = require(process.env.PLAYWRIGHT_MODULE_PATH);
const path = require('node:path');
const fs = require('node:fs');

(async () => {
  const output = process.env.VIVO_RS_BROWSER_OUTPUT;
  const browser = await chromium.launch({ headless: true, channel: 'chrome' });
  const errors = [];
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1080 }, acceptDownloads: true });
    page.on('pageerror', error => errors.push(error.message));
    const query = page.locator('.st-key-aoi_rs_monitor_query_submit button');
    const tables = page.locator('[data-testid="stDataFrame"]');
    const choose = async (key, value) => {
      const combo = page.locator(`.st-key-${key}`).getByRole('combobox');
      await combo.scrollIntoViewIfNeeded();
      await combo.focus();
      await combo.press('ArrowDown');
      await page.getByRole('option', { name: value, exact: true }).click();
    };
    const waitFor = async (condition, label) => {
      for (let index = 0; index < 100; index++) {
        if (await condition()) return;
        await page.waitForTimeout(100);
      }
      throw new Error(`Timed out: ${label}`);
    };
    await page.goto(process.env.VIVO_RS_BROWSER_URL);
    await query.waitFor();
    if (await tables.count()) throw new Error('Results appeared before query');
    await query.click();
    await waitFor(async () => await tables.count() === 2, 'two product tables');
    await page.getByText('M626 RS density', { exact: true }).waitFor();
    await page.getByText('M678 RS density', { exact: true }).waitFor();
    await page.screenshot({ path: path.join(output, 'normal-1440.png'), fullPage: true });
    const downloadEvent = page.waitForEvent('download');
    await page.getByRole('button', { name: '下载 AOI_RS 数据表', exact: true }).click();
    const download = await downloadEvent;
    await download.saveAs(path.join(output, 'download.xlsx'));
    if (fs.statSync(path.join(output, 'download.xlsx')).size < 1000) throw new Error('Empty Excel download');
    await choose('aoi_rs_monitor_factory', 'OLED');
    await waitFor(async () => await tables.count() === 0, 'filter hides old results');
    await query.click();
    await waitFor(async () => await tables.count() === 2, 'OLED tables');
    for (const width of [768, 375]) {
      await page.setViewportSize({ width, height: 1000 });
      await page.locator('.st-key-aoi_rs_monitor_weeks').scrollIntoViewIfNeeded();
      await page.screenshot({ path: path.join(output, `controls-${width}.png`) });
      if (width === 375) {
        const weeks = page.locator('.st-key-aoi_rs_monitor_weeks input');
        await weeks.fill('4');
        await weeks.press('Tab');
        await waitFor(async () => await tables.count() === 0, 'narrow-screen period change hides results');
        await query.click();
        await waitFor(async () => await tables.count() === 2, 'narrow-screen period query');
      }
      await tables.first().scrollIntoViewIfNeeded();
      const size = await page.evaluate(() => ({ scroll: document.documentElement.scrollWidth, client: document.documentElement.clientWidth }));
      if (size.scroll > size.client + 2) throw new Error(`Page overflow at ${width}: ${JSON.stringify(size)}`);
      await page.screenshot({ path: path.join(output, `normal-${width}.png`), fullPage: true });
      await page.getByRole('button', { name: '下载 AOI_RS 数据表', exact: true }).scrollIntoViewIfNeeded();
      await page.screenshot({ path: path.join(output, `download-${width}.png`) });
    }
    await page.setViewportSize({ width: 1440, height: 1080 });
    await page.waitForTimeout(500);
    await choose('fixture_rs_mode', 'failure');
    await choose('aoi_rs_monitor_factory', 'TP');
    await query.click();
    await page.getByText('AOI_RS 数据读取或计算失败，请稍后重试。', { exact: true }).waitFor();
    let text = await page.locator('body').innerText();
    if (text.includes('FIXTURE_PRIVATE_FAILURE') || text.includes('Traceback')) throw new Error('Private failure leaked');
    await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true });
    await choose('fixture_rs_mode', 'empty');
    await query.click();
    await page.getByText('M626 暂无所选范围的 AOI_RS 数据。', { exact: true }).waitFor();
    if (await tables.count()) throw new Error('Empty result contains stale tables');
    await page.screenshot({ path: path.join(output, 'empty.png'), fullPage: true });
    if (errors.length) throw new Error(errors.join('\n'));
    fs.writeFileSync(path.join(output, 'browser-result.json'), JSON.stringify({ passed: true, checks: [
      'actual page', 'query gate', 'two products', 'Excel download', 'filter invalidation',
      '1440/768/375 viewport fit', 'narrow-screen controls/table/download',
      'narrow-screen period change and query', 'ordinary failure', 'empty result', 'no page errors',
    ] }, null, 2));
    console.log('AOI_RS browser E2E passed');
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
