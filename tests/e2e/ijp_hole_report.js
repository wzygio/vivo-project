async page => {
  await page.setViewportSize({width: 1365, height: 768});
  await page.goto('http://localhost:8512');
  await page.getByRole('combobox', {name: '监控区域', exact: true}).click();
  await page.getByRole('option', {name: '孔区', exact: true}).click();
  await page.getByText('请选择筛选条件并点击“查询”。').waitFor();
  if (await page.locator('h3, [data-testid="stCaptionContainer"]').count())
    throw Error('Unexpected region subtitle or caption');
  await page.getByRole('heading', {name: 'IJP溢流监控报表', exact: true}).waitFor();
  const filters = await page.locator('[data-testid="stSelectbox"], [data-testid="stMultiSelect"]').evaluateAll(elements =>
    elements.map(element => element.getBoundingClientRect().top));
  if (filters.length !== 5 || Math.max(...filters) - Math.min(...filters) > 3)
    throw Error(`Filters must share one row: ${filters}`);
  await page.getByRole('button', {name: '查询', exact: true}).click();
  await page.locator('.js-plotly-plot').first().waitFor();
  if (await page.locator('.js-plotly-plot').count() !== 2) throw Error('Printer count');
  if (await page.getByRole('radiogroup', {name: '统计视图'}).count())
    throw Error('Removed modes are still available');
  const assertChart = async () => {
    const chart = await page.locator('.js-plotly-plot').first().evaluate(el => ({
      mode: el.layout.barmode, names: el.data.map(t => t.name),
      x: el.data[0].x, values: el.data.map(t => Array.from(t.y)),
      hover: el.data.map(t => t.hovertemplate),
    }));
    if (chart.mode !== 'stack' || chart.names.join() !== 'C3RA1,C3RA2,C3RA3' || chart.x.length !== 5 ||
        chart.x.slice(0, 2).join() !== '2026-08（月）,2026-09（月）')
      throw Error(JSON.stringify(chart));
    if (chart.values[2].some(value => value !== 0)) throw Error('C3RA3 must remain zero');
    chart.values[0].forEach((value, index) => {
      if (value > 0 && (value <= 0.9 || Math.abs(value + chart.values[1][index] - 1) > 1e-9))
        throw Error(`Unexpected stack: ${chart.values}`);
    });
  };
  await assertChart();
  await page.locator('.js-plotly-plot').first().screenshot({path: 'output/test-results/hole-period-chart.png'});
  await page.screenshot({path: 'output/test-results/hole-period.png', fullPage: true});
  if (await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth))
    throw Error('Horizontal overflow');
  const codes = page.locator('[role="combobox"][aria-label*="CODE"]');
  await codes.click();
  await page.getByRole('option', {name: 'C3RA2', exact: true}).click();
  await page.keyboard.press('Escape');
  await page.getByText('请选择筛选条件并点击“查询”。').waitFor();
  await page.getByRole('button', {name: '查询', exact: true}).click();
  await page.waitForFunction(() => {
    const el = document.querySelector('.js-plotly-plot');
    return el?.data.length === 1 && el.data[0].name === 'C3RA2';
  });
  const ratio = await page.locator('.js-plotly-plot').first().evaluate(el => el.data[0].y[1]);
  if (ratio < 0.085 - 1e-9 || ratio > 0.095 + 1e-9)
    throw Error(`Unexpected selected ratio: ${ratio}`);
  const line = page.locator('[role="combobox"][aria-label*="线体"]');
  await line.click();
  await page.getByRole('option', {name: '3CEE02', exact: true}).click();
  await page.keyboard.press('Escape');
  await page.getByRole('button', {name: '查询', exact: true}).click();
  await page.getByText('当前筛选条件下暂无 IJP 溢流数据。').waitFor();
  await page.screenshot({path: 'output/test-results/hole-empty.png', fullPage: true});
  await line.click(); await page.keyboard.press('Backspace'); await page.keyboard.press('Escape');
  await page.locator('[role="combobox"][aria-label*="产品型号"]').click();
  await page.getByRole('option', {name: /^M678(?:（.*）)?$/}).click();
  await page.keyboard.press('Escape');
  await page.getByRole('button', {name: '查询', exact: true}).click();
  await page.getByText('IJP 数据暂时无法读取，请稍后重试。').waitFor();
  if (await page.locator('.js-plotly-plot').count()) throw Error('Stale chart after error');
  const content = await page.locator('body').innerText();
  if (/admin=true|raw_ratio|in_window|SELECT |数据库权限|Traceback/.test(content))
    throw Error('Internal disclosure');
  await page.screenshot({path: 'output/test-results/hole-error.png', fullPage: true});
  await page.getByRole('combobox', {name: '监控区域', exact: true}).click();
  await page.getByRole('option', {name: 'AA区', exact: true}).click();
  await page.getByRole('heading', {name: 'IJP溢流监控报表', exact: true}).waitFor();
  return 'PASS: actual page, SQL adapter, five periods, ratios, filters, empty/error, region switch, viewport';
}
