async page => {
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto('http://localhost:8522/');
  await page.getByRole('heading', { name: '蒸镀单腔停留时间监控' }).waitFor({ timeout: 30000 });
  await page.getByRole('button', { name: '查询单腔', exact: true }).click();
  await page.locator('[data-testid="stPlotlyChart"]').first().waitFor({ timeout: 60000 });
  await page.getByText('3CEE002 - PT → OC1', { exact: true }).waitFor({ timeout: 30000 });
  if (await page.locator('[data-testid="stException"]').count()) throw new Error('Live page exception');
  const metrics = await page.locator('[data-testid="stMetricValue"]').allTextContents();
  if (Number(metrics[0].replaceAll(',', '')) <= 0) throw new Error('Missing source glasses ' + metrics);
  await page.screenshot({ path: 'D:/wzy/Python/vivo-project/output/test-results/qtime-chamber/live-page.png', fullPage: true });
  await page.locator('[data-testid="stMultiSelect"]').filter({
    has: page.getByRole('combobox', {name: /腔室/}),
  }).getByRole('button', {name: 'Clear all', exact: true}).click();
  const oc = page.locator('[data-testid="stExpander"]').filter({hasText: '3CEE002 - OC2 → OC3'});
  await oc.waitFor();
  await oc.scrollIntoViewIfNeeded();
  await page.waitForFunction(() => [...document.querySelectorAll('.js-plotly-plot')]
    .filter(plot => plot.layout.title.text.includes('OC2')).some(plot => plot.querySelector('.barlayer .point path')));
  await oc.screenshot({path: 'D:/wzy/Python/vivo-project/output/test-results/qtime-chamber/live-oc2-oc3.png'});
  const figures = await page.evaluate(() => [...document.querySelectorAll('.js-plotly-plot')].map(plot => ({
    types: plot.data.map(trace => trace.type), maximum: Math.max(...plot.data.flatMap(trace => Array.from(trace.y))),
    bars: plot.querySelectorAll('.barlayer .point path').length,
  })));
  if (figures.some(figure => figure.maximum > 1000 || figure.types.some(type => type !== 'bar') || !figure.bars)) {
    throw new Error('Invalid live bars ' + JSON.stringify(figures));
  }
  if (await page.getByText('单腔停留时间明细', {exact: true}).count()) throw new Error('Detail table remains');
  await page.getByRole('combobox', { name: '产品型号', exact: true }).click();
  await page.getByRole('option', { name: 'M626', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.waitForFunction(() => [...document.querySelectorAll('.js-plotly-plot')]
    .every(plot => plot.data.every(trace => trace.name === 'M626')));
  await page.getByRole('combobox', { name: '线体', exact: true }).click();
  await page.getByRole('option', { name: '3CEE001', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.getByText('当前筛选条件下暂无单腔停留时间数据。', { exact: true }).waitFor();
  return { ok: true, initialMetrics: metrics, figures, line1M626: 0 };
}
