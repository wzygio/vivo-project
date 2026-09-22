async page => {
  // Exercise Streamlit's native download fallback without an OS save dialog.
  await page.addInitScript(() => { delete window.showSaveFilePicker; });
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.setViewportSize({ width: 1365, height: 1000 });
  await page.goto('http://localhost:8521');
  const table = page.getByTestId('stDataFrame');
  await table.waitFor({ timeout: 90000 });
  await page.waitForFunction(() => document.querySelectorAll('.js-plotly-plot').length === 8);
  if (await page.getByRole('combobox').count() !== 3) throw new Error('Expected product, status and batch filters');
  if (await page.getByRole('button', { name: /刷新数据|刷新缓存/ }).count()) throw new Error('Normal view exposes maintenance controls');
  const expanders = page.getByTestId('stExpander');
  if (await expanders.count() !== 3) throw new Error('Expected one product/batch expander and two metric children');
  const expander = expanders.nth(1);
  const boxes = await expander.locator('.js-plotly-plot').evaluateAll(elements => elements.map(el => {
    const r = el.getBoundingClientRect(); return { x: r.x, y: r.y };
  }));
  if (boxes.some(box => Math.abs(box.y - boxes[0].y) > 2) || boxes.some((box, i) => i > 0 && box.x <= boxes[i - 1].x)) throw new Error('Four charts are not in one row');
  const legendOverlap = await expander.locator('.js-plotly-plot').evaluateAll(elements => elements.some(el => {
    const legend = el.querySelector('.legend').getBoundingClientRect();
    const plot = el.querySelector('.nsewdrag').getBoundingClientRect();
    return legend.bottom > plot.top + 1;
  }));
  if (legendOverlap) throw new Error('Legend overlaps measurement curves');
  await expander.locator('summary').click();
  await expander.locator('details:not([open])').waitFor({ state: 'attached' });
  await expander.locator('summary').click();
  if (await page.getByRole('spinbutton').count()) throw new Error('Manual pagination remains');
  const charts = await page.locator('.js-plotly-plot').evaluateAll(elements => elements.map(el => ({
    traces: el.data.map(t => ({ name: t.name, x: t.x, y: t.y, hover: t.hovertemplate })),
    xaxis: el.layout.xaxis.title.text, yaxis: el.layout.yaxis.title.text,
    dtick: el.layout.xaxis.dtick, tickformat: el.layout.xaxis.tickformat,
  })));
  for (const chart of charts) {
    if (chart.traces.length !== 5 || chart.xaxis !== '测试时间' || !['效率衰减', '亮度衰减'].includes(chart.yaxis)) throw new Error('Wrong axes or traces');
    if (chart.dtick !== 100 || chart.tickformat !== '.0f') throw new Error('Wrong time tick precision');
    for (const [index, trace] of chart.traces.entries()) {
      if (trace.name !== String(index + 1)) throw new Error('Non-numeric sample legend');
      if (trace.x.join(',') !== '0,100,200,300,400') throw new Error('Time order changed');
      if (!trace.hover.includes('样品编号')) throw new Error('Hover lacks anonymous identity');
    }
  }
  await page.getByRole('combobox', { name: '产品型号', exact: true }).click();
  await page.getByRole('option', { name: 'M678', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.getByRole('combobox', { name: '产品状态', exact: true }).click();
  await page.getByRole('option', { name: '屏体', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.getByRole('combobox', { name: '批次号', exact: true }).click();
  await page.getByRole('option', { name: '2026/3/10', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.getByRole('button', { name: '查询', exact: true }).click();
  await table.waitFor();
  await page.locator('.js-plotly-plot').first().locator('.scatterlayer .trace').first().locator('.point').first().hover({ force: true });
  await page.locator('.hoverlayer').first().getByText(/样品编号/).waitFor();
  await page.screenshot({ path: 'live-hover.png', fullPage: true });
  const exposed = await page.evaluate(() => document.body.innerText +
    JSON.stringify([...document.querySelectorAll('.js-plotly-plot')].map(el => el.data)));
  if (/L3MR|panel_id|private-|admin=true|管理员|m3dwd|Traceback/.test(exposed)) throw new Error('Internal content leaked');
  for (const width of [1365, 768, 390]) {
    await page.setViewportSize({ width, height: 1000 });
    await page.waitForTimeout(350);
    if (await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth)) throw new Error(`Page overflow at ${width}`);
    await table.scrollIntoViewIfNeeded();
    await page.screenshot({ path: `live-${width}.png`, fullPage: true });
  }
  await page.setViewportSize({ width: 1365, height: 1000 });
  await table.hover();
  const pending = page.waitForEvent('download');
  await table.getByRole('button', { name: 'Download as CSV', exact: true }).click();
  const download = await pending;
  await download.saveAs('native-lifetime-live.csv');
  await page.goto('http://localhost:8521/?admin=true');
  await table.waitFor({ timeout: 90000 });
  if (await page.getByRole('button', { name: /刷新数据/ }).count()) throw new Error('Snapshot control should be absent');
  await page.getByRole('button', { name: '🔄 刷新缓存', exact: true }).click();
  await page.getByText('🔄 缓存已刷新 · 代码与配置已重载', { exact: true }).waitFor();
  await table.waitFor({ timeout: 90000 });
  if (await page.locator('[data-testid="stException"]').count() || errors.length) throw new Error(errors.join('\n') || 'Streamlit exception');
  return { status: 'passed', source: 'live M3', rows: 100, columns: 15, charts: 8, layout: 'four charts per metric expander row', filters: 3, tickStep: 100, headerCacheRefresh: true, viewports: [1365, 768, 390] };
}
