async page => {
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.setViewportSize({ width: 1365, height: 1000 });
  await page.goto('http://localhost:8521');
  const table = page.locator('.iqc-sheet');
  await table.waitFor({ timeout: 90000 });
  await page.waitForFunction(() => document.querySelectorAll('.js-plotly-plot').length === 4);
  if (await page.getByRole('combobox').count() !== 2) throw new Error('Expected only product and batch filters');
  const headers = await table.locator('th').allTextContents();
  if (headers.length !== 15 || headers[4] !== '样品编号' || headers[14] !== '效率衰减') throw new Error('Incorrect columns');
  if (await table.locator('tbody tr').count() !== 100) throw new Error('Expected 100 live measurements');
  const charts = await page.locator('.js-plotly-plot').evaluateAll(elements => elements.map(el => ({
    traces: el.data.map(t => ({ name: t.name, x: t.x, y: t.y, hover: t.hovertemplate })),
    xaxis: el.layout.xaxis.title.text, yaxis: el.layout.yaxis.title.text,
  })));
  for (const chart of charts) {
    if (chart.traces.length !== 5 || chart.xaxis !== '测试时间' || chart.yaxis !== '效率衰减') throw new Error('Wrong axes or traces');
    for (const [index, trace] of chart.traces.entries()) {
      if (trace.name !== String(index + 1)) throw new Error('Non-numeric sample legend');
      if (trace.x.join(',') !== '0,100,200,300,400') throw new Error('Time order changed');
      if (!trace.hover.includes('样品编号')) throw new Error('Hover lacks anonymous identity');
    }
  }
  await page.getByRole('combobox', { name: '产品型号', exact: true }).click();
  await page.getByRole('option', { name: 'M678', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.getByRole('combobox', { name: '批次号', exact: true }).click();
  await page.getByRole('option', { name: '2026/3/10', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.waitForFunction(() => document.querySelectorAll('.iqc-sheet tbody tr').length === 100);
  await page.locator('.js-plotly-plot').first().locator('.scatterlayer .trace').first().locator('.point').first().hover({ force: true });
  await page.locator('.hoverlayer').first().getByText(/样品编号/).waitFor();
  await page.screenshot({ path: 'live-hover.png', fullPage: true });
  const exposed = await page.evaluate(() => document.body.innerText + document.querySelector('.iqc-sheet').outerHTML +
    JSON.stringify([...document.querySelectorAll('.js-plotly-plot')].map(el => el.data)));
  if (/L3MR|panel_id|private-|admin=true|管理员|m3dwd|Traceback/.test(exposed)) throw new Error('Internal content leaked');
  for (const width of [1365, 768, 390]) {
    await page.setViewportSize({ width, height: 1000 });
    await page.waitForTimeout(350);
    if (await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth)) throw new Error(`Page overflow at ${width}`);
    await table.evaluate(el => { el.scrollLeft = el.scrollWidth; });
    if (!await table.evaluate(el => el.scrollLeft + el.clientWidth >= el.scrollWidth - 2)) throw new Error('Cannot reach final measurement column');
    await page.screenshot({ path: `live-${width}.png`, fullPage: true });
  }
  await page.setViewportSize({ width: 1365, height: 1000 });
  await table.evaluate(el => { el.scrollLeft = 0; });
  await page.getByRole('button', { name: '刷新数据', exact: true }).click();
  await table.waitFor({ timeout: 90000 });
  if (await page.locator('[data-testid="stException"]').count() || errors.length) throw new Error(errors.join('\n') || 'Streamlit exception');
  return { status: 'passed', source: 'live M3', rows: 100, columns: 15, charts: 4, samplesPerChart: 5, viewports: [1365, 768, 390] };
}
