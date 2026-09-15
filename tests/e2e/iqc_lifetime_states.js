async page => {
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.setViewportSize({ width: 1365, height: 1000 });
  const table = page.locator('.iqc-sheet');
  for (const scenario of ['failure', 'empty']) {
    await page.goto(`http://localhost:8522/?scenario=${scenario}`);
    await page.getByText(scenario === 'failure'
      ? '寿命测试数据暂时无法读取，请稍后刷新重试。'
      : '没有符合筛选条件的寿命测试记录，请调整筛选条件或稍后刷新。', { exact: true }).waitFor();
    if (await table.count() || await page.locator('.js-plotly-plot').count()) throw new Error('Stale result shown');
    if (/private|Traceback|admin=true|管理员/.test(await page.locator('body').innerText())) throw new Error('Failure leaks internals');
    await page.screenshot({ path: `controlled-${scenario}.png`, fullPage: true });
  }
  await page.goto('http://localhost:8522/?scenario=missing');
  await table.waitFor();
  await page.getByText('部分测量值缺失，明细保留空白，趋势图在缺失测点处断开。', { exact: true }).waitFor();
  await page.waitForFunction(() => document.querySelectorAll('.js-plotly-plot').length === 5);
  const traces = await page.locator('.js-plotly-plot').first().evaluate(el => el.data);
  if (traces.length !== 9 || traces[0].y[1] !== null || traces[0].connectgaps !== false) throw new Error('Missing gaps or >5 samples lost');
  await page.getByRole('spinbutton', { name: '页码', exact: true }).fill('2');
  await page.getByRole('spinbutton', { name: '页码', exact: true }).press('Enter');
  await page.waitForFunction(() => document.querySelectorAll('.iqc-sheet tbody tr').length === 9);
  await page.screenshot({ path: 'controlled-last-page.png', fullPage: true });
  await page.getByRole('combobox', { name: '批次号', exact: true }).click();
  await page.getByRole('option', { name: '2026/3/10', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.waitForFunction(() => document.querySelector('input[aria-label="页码"]')?.value === '1');
  await page.getByRole('combobox', { name: '产品型号', exact: true }).click();
  await page.getByRole('option', { name: 'M999', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.waitForFunction(() => document.querySelectorAll('.iqc-sheet tbody tr').length === 1);
  const cells = await table.locator('tbody tr').first().locator('td').allTextContents();
  if (cells[0] !== 'M999' || cells[3] !== 'other-batch' || cells[4] !== '1') throw new Error('Dependent filters or group numbering wrong');
  if (await page.locator('.js-plotly-plot').count() !== 1) throw new Error('Old charts remain after filtering');
  await page.getByRole('combobox', { name: '批次号', exact: true }).click();
  if (await page.getByRole('option', { name: '2026/3/10', exact: true }).count()) throw new Error('Obsolete batch available');
  await page.keyboard.press('Escape');
  await page.screenshot({ path: 'controlled-dependent-filter.png', fullPage: true });
  const exposed = await page.evaluate(() => document.body.innerText + JSON.stringify([...document.querySelectorAll('.js-plotly-plot')].map(el => el.data)));
  if (/private-|panel_id|admin=true|管理员/.test(exposed)) throw new Error('Identity leak');
  if (await page.locator('[data-testid="stException"]').count() || errors.length) throw new Error(errors.join('\n') || 'Streamlit exception');
  return { status: 'passed', scenarios: ['failure', 'empty', 'recovery', 'missing', 'nine-samples', 'last-page', 'filter-reset', 'anonymous-hover'] };
}
