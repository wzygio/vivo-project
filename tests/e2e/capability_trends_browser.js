const { chromium } = require(process.env.PLAYWRIGHT_MODULE_PATH);
const path = require('node:path');
const fs = require('node:fs');

(async () => {
  const output = process.env.VIVO_CAPABILITY_BROWSER_OUTPUT;
  const base = process.env.VIVO_CAPABILITY_BROWSER_URL;
  const browser = await chromium.launch({ headless: true, channel: 'chrome' });
  const errors = [];
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1080 } });
    page.on('pageerror', error => errors.push(error.message));
    const text = value => page.getByText(value, { exact: true }).waitFor();
    const query = metric => page.locator(`.st-key-${metric}_monitor_query_submit button`);
    const plots = page.locator('.js-plotly-plot');
    const open = async metric => {
      const panel = page.getByTestId('stExpander').filter({ hasText: `${metric.toUpperCase()} 筛选条件` });
      await panel.waitFor();
      if (!await panel.locator('details').evaluate(node => node.open)) await panel.locator('summary').click();
    };
    const waitPlots = count => page.waitForFunction(n => document.querySelectorAll('.js-plotly-plot').length === n, count);
    const clean = async () => {
      const body = await page.locator('body').innerText();
      if (/PRIVATE_SOURCE_PATH|PRIVATE_DETAIL|最新项目明细|admin=true|Traceback/.test(body)) throw Error('Private details leaked');
      if (await page.getByTestId('stException').count()) throw Error('Streamlit exception');
    };
    await page.goto(base);
    await text('📊 CPK达标率趋势图');
    if (!await query('cpm').count()) throw Error('CPM unavailable in ordinary mode');
    if (await plots.count()) throw Error('Chart created before query');
    if (await page.locator('[data-testid="stExpander"] details[open]').count()) throw Error('Filters not collapsed');
    await open('cpk');
    await query('cpk').click();
    await waitPlots(3);
    const cpkPanel = page.getByTestId('stExpander').filter({ hasText: 'CPK 筛选条件' });
    if (await cpkPanel.locator('.js-plotly-plot').count() !== 3) throw Error('Product charts outside original expander');
    const totals = await plots.evaluateAll(nodes => nodes.map(node => node.data.filter(trace => trace.type === 'bar')[0].y[0]));
    if (JSON.stringify(totals) !== JSON.stringify([100, 40, 60])) throw Error('Product chart totals not separated');
    await text('CPK calls: 1');
    await text('Page runs: 1');
    if (await page.locator('.st-key-cpk_monitor_factories').count()) throw Error('Obsolete factory filter');
    const plot = await plots.first().evaluate(node => ({
      bars: node.data.filter(trace => trace.type === 'bar').map(trace => ({ x: trace.x, y: trace.y, color: trace.marker.color })),
      lines: node.data.filter(trace => trace.type === 'scatter').map(trace => ({ x: trace.x, y: trace.y, yaxis: trace.yaxis })),
      categoryarray: node.layout.xaxis.categoryarray,
    }));
    if (plot.bars.length !== 4 || plot.lines.length !== 4) throw Error('Wrong combo chart series');
    if (new Set(plot.bars.map(trace => trace.color)).size !== 4) throw Error('Period colors not distinct');
    if (!plot.lines.every(trace => trace.yaxis === 'y2')) throw Error('Rate axis not independent');
    if (plot.lines[3].y.slice(-2).some(rate => rate !== 1)) throw Error('Zero denominator is not 100%');
    if (JSON.stringify(plot.categoryarray.slice(-7)) !== JSON.stringify(['M9','M10','M11','W44','W45','W46','W47'])) throw Error('Three-month/four-week axis changed');
    await clean();
    await plots.first().scrollIntoViewIfNeeded();
    await plots.first().screenshot({ path: path.join(output, 'ordinary-chart.png') });
    const chartBox = await plots.first().boundingBox();
    const beforeScroll = await page.evaluate(() => ({
      main: document.querySelector('[data-testid="stMain"]').scrollTop,
      document: document.scrollingElement.scrollTop,
    }));
    const beforeRange = await plots.first().evaluate(node => JSON.stringify(node._fullLayout.xaxis.range));
    await page.mouse.move(chartBox.x + chartBox.width / 2, chartBox.y + chartBox.height / 2);
    await page.mouse.wheel(0, -250);
    await page.waitForTimeout(300);
    const afterScroll = await page.evaluate(() => ({
      main: document.querySelector('[data-testid="stMain"]').scrollTop,
      document: document.scrollingElement.scrollTop,
    }));
    if (JSON.stringify(beforeScroll) === JSON.stringify(afterScroll)) throw Error('Wheel over chart did not scroll the page');
    if (beforeRange !== await plots.first().evaluate(node => JSON.stringify(node._fullLayout.xaxis.range))) throw Error('Wheel zoom changed chart coordinates');
    await page.screenshot({ path: path.join(output, 'ordinary.png'), fullPage: true });
    await page.goto(`${base}/?admin=true`);
    await open('cpk');
    await open('cpm');
    await query('cpk').click();
    await waitPlots(3);
    await query('cpm').click();
    await waitPlots(6);
    await text('CPK calls: 1');
    await text('CPM calls: 1');
    await text('Page runs: 1');
    await query('cpk').click();
    await text('CPK calls: 2');
    await text('CPM calls: 1');
    await text('Page runs: 1');
    for (const width of [1440, 768, 375]) {
      await page.setViewportSize({ width, height: 1000 });
      await plots.first().scrollIntoViewIfNeeded();
      const overflow = await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth + 2);
      if (overflow) throw Error(`Page overflow at ${width}`);
      await page.screenshot({ path: path.join(output, `admin-${width}.png`), fullPage: true });
    }
    await page.setViewportSize({ width: 1440, height: 1080 });
    await page.goto(base);
    await open('cpk');
    const scenario = page.getByRole('combobox', { name: 'Fixture scenario', exact: true });
    await scenario.click();
    await page.getByRole('option', { name: 'failure', exact: true }).click();
    await query('cpk').click();
    await text('CPK 数据读取失败，请稍后重试。');
    await waitPlots(0);
    await clean();
    await scenario.click();
    await page.getByRole('option', { name: 'empty', exact: true }).click();
    await query('cpk').click();
    await waitPlots(3);
    await clean();
    await page.getByRole('button', { name: 'Fixture full page reload', exact: true }).click();
    await text('Page runs: 4');
    if (errors.length) throw Error(errors.join('\n'));
    fs.writeFileSync(path.join(output, 'browser-result.json'), JSON.stringify({ passed: true, plot, checks: [
      'actual history page', 'query gate', 'CPM available in ordinary mode', 'collapsed filters', 'bar counts and rate line',
      'four period colors', 'lines split at period boundaries', 'zero count 100%', 'three months/four weeks',
      'fragment isolation and retained sibling result', 'failure safe UI', 'empty data', '1440/768/375 layouts',
      'wheel over chart scrolls page without zooming',
      'summary and individual product charts in same expander with separate values',
    ] }, null, 2));
    console.log('Capability trend browser E2E passed');
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
