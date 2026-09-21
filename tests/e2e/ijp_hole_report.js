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
  if (await page.locator('.js-plotly-plot').count() !== 1) throw Error('Glass count');
  const assertChart = async mode => {
    const chart = await page.locator('.js-plotly-plot').first().evaluate(el => ({
      mode: el.layout.barmode, names: el.data.map(t => t.name),
      values: el.calcdata.map(t => t.map(p => p.s)),
      hover: el.data.map(t => t.hovertemplate),
    }));
    if (chart.mode !== mode || chart.names.join() !== 'C3RA1,C3RA2,C3RA3')
      throw Error(JSON.stringify(chart));
  };
  await assertChart('stack');
  await page.locator('.js-plotly-plot').first().screenshot({path: 'hole-glass-chart.png'});
  await page.screenshot({path: 'hole-glass.png', fullPage: true});
  await page.getByText('Total', {exact: true}).click();
  await page.waitForFunction(() => document.querySelector('.js-plotly-plot')?.layout.barmode === 'group');
  await assertChart('group');
  await page.locator('.js-plotly-plot').first().screenshot({path: 'hole-total-chart.png'});
  await page.screenshot({path: 'hole-total.png', fullPage: true});
  await page.getByText('按天', {exact: true}).click();
  await page.waitForFunction(() => document.querySelectorAll('.js-plotly-plot').length === 2);
  await assertChart('stack');
  await page.locator('.js-plotly-plot').first().screenshot({path: 'hole-day-chart.png'});
  await page.screenshot({path: 'hole-day.png', fullPage: true});
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
  const ratio = await page.locator('.js-plotly-plot').first().evaluate(el => el.calcdata[0][0].s);
  if (ratio !== 0.5) throw Error(`Denominator changed: ${ratio}`);
  const line = page.locator('[role="combobox"][aria-label*="线体"]');
  await line.click();
  await page.getByRole('option', {name: '3CEE02', exact: true}).click();
  await page.keyboard.press('Escape');
  await page.getByRole('button', {name: '查询', exact: true}).click();
  await page.getByText('当前筛选条件下暂无 IJP 溢流数据。').waitFor();
  await page.screenshot({path: 'hole-empty.png', fullPage: true});
  await line.click(); await page.keyboard.press('Backspace'); await page.keyboard.press('Escape');
  await page.locator('[role="combobox"][aria-label*="产品型号"]').click();
  await page.getByRole('option', {name: 'M678', exact: true}).click();
  await page.keyboard.press('Escape');
  await page.getByRole('button', {name: '查询', exact: true}).click();
  await page.getByText('IJP 数据暂时无法读取，请稍后重试。').waitFor();
  if (await page.locator('.js-plotly-plot').count()) throw Error('Stale chart after error');
  const content = await page.locator('body').innerText();
  if (/admin=true|raw_ratio|in_window|SELECT |数据库权限|Traceback/.test(content))
    throw Error('Internal disclosure');
  await page.screenshot({path: 'hole-error.png', fullPage: true});
  await page.getByRole('combobox', {name: '监控区域', exact: true}).click();
  await page.getByRole('option', {name: 'AA区', exact: true}).click();
  await page.getByRole('heading', {name: 'IJP溢流监控报表', exact: true}).waitFor();
  return 'PASS: actual page, SQL adapter, three views, denominator, filters, empty/error, region switch, viewport';
}
