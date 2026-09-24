async page => {
  const base = 'http://localhost:8521/';
  const output = 'D:/wzy/Python/vivo-project/output/test-results/qtime-chamber';
  const query = () => page.getByRole('button', { name: '查询单腔', exact: true });
  const waitText = text => page.getByText(text, { exact: false }).first().waitFor({ timeout: 30000 });
  const select = async (label, value) => {
    await page.getByRole('combobox', { name: new RegExp(label) }).click();
    await page.getByRole('option', { name: value, exact: true }).click();
    await page.keyboard.press('Escape');
  };
  const counts = async () => {
    const text = await page.locator('body').innerText();
    return Object.fromEntries(['page', 'station', 'chamber'].map(module => [module,
      Math.max(...[...text.matchAll(new RegExp(module + '-executions: (\\d+)', 'g'))].map(match => Number(match[1])))
    ]));
  };
  const healthy = async () => {
    if (await page.locator('[data-testid="stException"]').count()) throw new Error('Streamlit exception');
    const body = await page.locator('body').innerText();
    for (const forbidden of ['INTERNAL_SECRET_SQL', 'DISABLED_PRODUCT', 'DISABLED_GLASS', 'OUTLIER_GLASS', '下载单腔明细', '单腔停留时间明细', '空选表示全部', '数据覆盖', '产品归属尚未确认', '部分记录缺少玻璃编号']) {
      if (body.includes(forbidden)) throw new Error('Unexpected content: ' + forbidden);
    }
  };
  const warnings = [];
  page.on('console', message => { if (/WebGL|context lost/i.test(message.text())) warnings.push(message.text()); });
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto(base);
  await waitText('请选择筛选条件并点击“查询单腔”。');
  const beforeLower = await counts();
  await query().click();
  await waitText('3CEE002 - PT → OC1');
  await page.waitForFunction(() => document.querySelectorAll('.js-plotly-plot').length === 2);
  const afterLower = await counts();
  if (beforeLower.page !== afterLower.page || beforeLower.station !== afterLower.station || beforeLower.chamber === afterLower.chamber) {
    throw new Error('Lower interaction reran parent/upper: ' + JSON.stringify({beforeLower, afterLower}));
  }
  // Rerun the real upper module, then submit it. The chamber fragment must stay intact.
  await page.getByRole('combobox', { name: '厂别', exact: true }).click();
  await page.getByRole('option', { name: 'OLED', exact: true }).click();
  await waitText('station-executions: 2');
  await page.getByRole('combobox', { name: '站点', exact: true }).click();
  await page.getByRole('option', { name: /OLED_OUT/ }).click();
  await page.keyboard.press('Escape');
  await page.getByRole('button', { name: '查询', exact: true }).click();
  await page.locator('[data-testid="stExpander"]').filter({hasText: 'OLED_OUT'}).first().waitFor();
  const afterUpper = await counts();
  if (afterUpper.page !== afterLower.page || afterUpper.chamber !== afterLower.chamber || afterUpper.station <= afterLower.station) {
    throw new Error('Upper interaction reran parent/lower: ' + JSON.stringify({afterLower, afterUpper}));
  }
  await healthy();
  await page.screenshot({ path: output + '/fragment-modules.png', fullPage: true });
  await select('产品型号', 'M678');
  await page.getByText('3CEE001 - PT → OC1', {exact: true}).waitFor({state: 'hidden'});
  await select('线体', '3CEE001');
  await waitText('当前筛选条件下暂无单腔停留时间数据。');
  await page.getByRole('button', { name: '刷新单腔缓存', exact: true }).click();
  await waitText('请选择筛选条件并点击“查询单腔”。');

  await page.goto(base);
  await waitText('请选择筛选条件并点击“查询单腔”。');
  await query().click();
  await waitText('3CEE002 - PT → OC1');
  // Empty chamber selection means all 11 chambers on both lines.
  await page.locator('[data-testid="stMultiSelect"]').filter({
    has: page.getByRole('combobox', { name: /腔室/ }),
  }).getByRole('button', { name: 'Clear all', exact: true }).click();
  await page.waitForFunction(() => document.querySelectorAll('.js-plotly-plot').length === 22
    && [...document.querySelectorAll('.js-plotly-plot')].every(plot => plot.querySelector('.scatterlayer .point')));
  const chartEvidence = await page.evaluate(() => [...document.querySelectorAll('.js-plotly-plot')].map(plot => ({
    types: plot.data.map(trace => trace.type), points: plot.data.flatMap(trace => Array.from(trace.y)),
    markers: plot.querySelectorAll('.scatterlayer .point').length,
    upper: plot.layout.yaxis.range[1],
    webgl: plot.querySelectorAll('.gl-container canvas').length,
  })));
  if (chartEvidence.some(chart => chart.types.some(type => type !== 'scatter') || !chart.markers || chart.webgl || chart.upper !== 6000 || chart.points.some(value => value > 1000))) {
    throw new Error('Invalid rendered chart: ' + JSON.stringify(chartEvidence));
  }
  const oc = page.locator('[data-testid="stExpander"]').filter({hasText: '3CEE002 - OC2 → OC3'});
  await oc.scrollIntoViewIfNeeded();
  await oc.screenshot({ path: output + '/oc2-oc3-markers.png' });
  if (await page.locator('[data-testid="stDataFrame"], [data-testid="stMetric"]').count()) throw new Error('Unexpected table/metrics');
  await healthy();
  if (warnings.length) throw new Error('WebGL context errors: ' + warnings.join('\n'));
  await page.setViewportSize({ width: 390, height: 844 });
  await oc.screenshot({ path: output + '/mobile-markers.png' });
  if (await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 2)) throw new Error('Viewport overflow');

  for (const [scenario, message] of [['failure', '蒸镀单腔停留时间数据读取失败，请稍后重试。'], ['empty', '当前筛选条件下暂无单腔停留时间数据。']]) {
    await page.goto(base + '?scenario=' + scenario);
    await waitText('请选择筛选条件并点击“查询单腔”。');
    await query().click();
    await waitText(message);
    await healthy();
    if (await page.locator('[data-testid="stPlotlyChart"]').count()) throw new Error('Stale result after ' + scenario);
  }
  await page.goto(base);
  await waitText('请选择筛选条件并点击“查询单腔”。');
  await query().click();
  await waitText('3CEE002 - PT → OC1');
  await healthy();
  return { ok: true, charts: chartEvidence.length, beforeLower, afterLower, afterUpper,
    checks: ['bidirectional fragment isolation', 'line/chamber expanders', '22 SVG marker charts', '6000 axis limit', 'OC2-OC3', '1000 boundary', 'no tables/metrics/captions', 'filters', 'refresh', 'mobile', 'safe failure and recovery'] };
}
