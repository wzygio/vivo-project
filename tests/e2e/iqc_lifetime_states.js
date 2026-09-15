async page => {
  // Exercise Streamlit's native download fallback without an OS save dialog.
  await page.addInitScript(() => { delete window.showSaveFilePicker; });
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.setViewportSize({ width: 1365, height: 1000 });
  const table = page.getByTestId('stDataFrame');
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
  if ((await page.locator('body').innerText()).includes('M999')) throw new Error('Disabled product appears in report');
  await page.getByRole('combobox', { name: '产品型号', exact: true }).click();
  if (await page.getByRole('option', { name: 'M999', exact: true }).count()) throw new Error('Disabled product option');
  await page.keyboard.press('Escape');
  const mainGroup = page.getByTestId('stExpander').filter({ has: page.locator('summary', { hasText: 'M678' }) });
  const traces = await mainGroup.locator('.js-plotly-plot').first().evaluate(el => el.data);
  if (traces.length !== 9 || traces[0].y[1] !== null || traces[0].connectgaps !== false) throw new Error('Missing gaps or >5 samples lost');
  if (await page.getByRole('spinbutton').count()) throw new Error('Manual pagination remains');
  await table.hover();
  const pending = page.waitForEvent('download');
  await table.getByRole('button', { name: 'Download as CSV', exact: true }).click();
  const download = await pending;
  await download.saveAs('native-lifetime-controlled.csv');
  await page.getByRole('combobox', { name: '批次号', exact: true }).click();
  await page.getByRole('option', { name: '2026/3/10', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.getByRole('combobox', { name: '产品型号', exact: true }).click();
  await page.getByRole('option', { name: 'M626', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.waitForFunction(() => document.querySelectorAll('.js-plotly-plot').length === 1);
  await page.getByRole('combobox', { name: '产品状态', exact: true }).click();
  await page.getByRole('option', { name: '模组', exact: true }).click();
  await page.keyboard.press('Escape');
  if (await page.locator('.js-plotly-plot').count() !== 1) throw new Error('Old charts remain after filtering');
  await page.getByRole('combobox', { name: '批次号', exact: true }).click();
  if (await page.getByRole('option', { name: '2026/3/10', exact: true }).count()) throw new Error('Obsolete batch available');
  await page.keyboard.press('Escape');
  await page.screenshot({ path: 'controlled-dependent-filter.png', fullPage: true });
  const exposed = await page.evaluate(() => document.body.innerText + JSON.stringify([...document.querySelectorAll('.js-plotly-plot')].map(el => el.data)));
  if (/private-|panel_id|admin=true|管理员/.test(exposed)) throw new Error('Identity leak');
  if (await page.locator('[data-testid="stException"]').count() || errors.length) throw new Error(errors.join('\n') || 'Streamlit exception');
  return { status: 'passed', scenarios: ['failure', 'empty', 'recovery', 'missing', 'nine-samples', 'complete-native-export', 'filter-reset', 'anonymous-hover', 'enabled-products', 'product-status'] };
}
