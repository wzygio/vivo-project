// Execute through run_z622_spc_browser.py, which owns and cleans all processes.
const { chromium } = require(process.env.PLAYWRIGHT_MODULE_PATH);
const fs = require('node:fs');
const path = require('node:path');

(async () => {
  const output = process.env.VIVO_Z622_BROWSER_OUTPUT;
  const browser = await chromium.launch({ headless: true, channel: 'chrome' });
  const pageErrors = [];
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1000 }, acceptDownloads: true });
    page.on('pageerror', error => pageErrors.push(error.message));
    await page.goto(process.env.VIVO_Z622_BROWSER_URL);
    await page.getByRole('heading', { name: 'Z622 SPC 数据验证' }).waitFor();
    await page.getByText(/Z622 · \d+ 条点位 · \d+ 个指标/).waitFor({ timeout: 180000 });
    const evidence = JSON.parse(fs.readFileSync(path.join(output, 'pipeline-normal.json'), 'utf8'));
    if (!evidence.enabled || !evidence.point_rows || !evidence.sql_calls) throw new Error('Pipeline did not use enabled Z622 and captured SQL transport');
    const selection = JSON.parse(fs.readFileSync(path.join(output, 'selection.json'), 'utf8'));
    const choose = async (key, value) => {
      const combo = page.locator(`.st-key-${key}`).getByRole('combobox');
      await combo.scrollIntoViewIfNeeded();
      await combo.click();
      await page.getByRole('option', { name: value, exact: true }).click();
      await page.keyboard.press('Escape');
    };
    await choose('spc_step_filter', selection.step);
    // The real cascade auto-selects all parameters for a newly selected station.
    // Selected values are absent from the dropdown: submit the default selection.
    await page.waitForFunction(() => {
      const query = Array.from(document.querySelectorAll('button')).find(button => button.textContent.trim() === '查询');
      return query && !query.disabled;
    });
    const selectedParameters = await page.locator('.st-key-spc_param_filter [data-baseweb="tag"]').allTextContents();
    await page.getByRole('button', { name: '查询', exact: true }).click();
    await page.waitForFunction(() => document.querySelectorAll('.js-plotly-plot').length >= 3, undefined, { timeout: 180000 });
    const figures = await page.locator('.js-plotly-plot').evaluateAll(nodes => nodes.map(node => ({
      traces: node.data?.length || 0, title: node.layout?.title?.text || '', scrollZoom: node._context?.scrollZoom,
    })));
    if (!figures[0].traces) throw new Error('Measurement overview has no real traces');
    if (figures.some(figure => figure.scrollZoom !== false)) throw new Error('Plotly scroll contract changed');
    const [download] = await Promise.all([
      page.waitForEvent('download'), page.getByRole('button', { name: '下载点位 CSV' }).click(),
    ]);
    await download.saveAs(path.join(output, 'Z622-points.csv'));
    const csv = fs.readFileSync(path.join(output, 'Z622-points.csv'), 'utf8');
    if (!csv.includes('Z622') || !csv.includes('L3ZB6BE03')) throw new Error('Export does not contain captured product/lot facts');
    if (csv.trim().split(/\r?\n/).length !== evidence.point_rows + 1) throw new Error('Export population differs from final points');
    await page.screenshot({ path: path.join(output, 'normal.png'), fullPage: true });
    await choose('z622_fixture_mode', 'empty');
    await page.getByText('当前产品暂无 SPC 数据。', { exact: true }).waitFor({ timeout: 180000 });
    if (await page.locator('.js-plotly-plot').count()) throw new Error('Stale charts in empty state');
    await choose('z622_fixture_mode', 'failure');
    await page.getByText('SPC 数据暂时无法读取，请稍后重试。', { exact: true }).waitFor({ timeout: 180000 });
    const body = await page.locator('body').innerText();
    if (/CAPTURED_PDS_TRANSPORT_FAILURE|Traceback|admin=true/.test(body)) throw new Error('Private failure details leaked');
    if (await page.locator('.js-plotly-plot').count()) throw new Error('Stale charts in failure state');
    await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true });
    if (pageErrors.length) throw new Error(pageErrors.join('\n'));
    fs.writeFileSync(path.join(output, 'result.json'), JSON.stringify({ passed: true,
      scope: 'captured PDS SQL transport -> production adapter/preparation/service/charts -> browser/CSV',
      scenarios: ['normal', 'empty', 'failure'], selection, selectedParameters, evidence, figures, pageErrors,
    }, null, 2));
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
