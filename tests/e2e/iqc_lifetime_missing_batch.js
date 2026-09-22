async page => {
  await page.addInitScript(() => { delete window.showSaveFilePicker; });
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.setViewportSize({ width: 1365, height: 1000 });
  await page.goto('http://localhost:8521');
  const table = page.getByTestId('stDataFrame');
  await table.waitFor({ timeout: 60000 });
  await page.getByRole('combobox', { name: '批次号', exact: true }).click();
  await page.getByRole('option', { name: '未填写批次', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.waitForFunction(() => {
    const labels = [...document.querySelectorAll('[data-testid="stExpander"] summary')];
    return labels.length > 0 && labels.every(el => el.textContent.includes('未填写批次'));
  });
  const charts = page.locator('.js-plotly-plot');
  await page.waitForFunction(() => document.querySelectorAll('.js-plotly-plot').length >= 2);
  const metrics = await charts.evaluateAll(elements => elements.map(el => el.layout.yaxis.title.text));
  if (!metrics.includes('效率衰减') || !metrics.includes('亮度衰减')) throw new Error('Missing decay chart');
  const alerts = await page.getByTestId('stAlert').allTextContents();
  if (alerts.some(text => /批次|无法读取/.test(text))) throw new Error('Unexpected batch warning or load failure');
  if (await page.getByTestId('stException').count() || errors.length) throw new Error('Page runtime error');
  await table.hover();
  const pending = page.waitForEvent('download');
  await table.getByRole('button', { name: 'Download as CSV', exact: true }).click();
  const download = await pending;
  await download.saveAs('missing-batch-live.csv');
  await page.screenshot({ path: 'missing-batch-live.png', fullPage: true });
  return { status: 'passed', source: 'live M3', batch: '未填写批次', metrics, batchWarning: false };
}
