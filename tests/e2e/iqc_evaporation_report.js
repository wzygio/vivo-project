async page => {
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.setViewportSize({ width: 1365, height: 900 });
  await page.goto('http://localhost:8517');
  await page.getByRole('heading', { name: 'IQC-蒸镀材料', exact: true }).waitFor();
  await page.getByText('请选择检验日期范围并点击“查询”。', { exact: true }).waitFor();
  await page.getByRole('button', { name: '查询', exact: true }).click();
  const table = page.locator('.iqc-sheet');
  await table.waitFor({ timeout: 90000 });
  const headers = await table.locator('th').allTextContents();
  if (headers.length !== 27 || headers[25] !== 'IQC结果' || headers[26] !== 'COA结果') {
    throw new Error(`Incorrect report columns: ${headers.join(',')}`);
  }
  if (await table.locator('tbody tr').count() !== 100) throw new Error('First page should contain 100 rows');
  const firstNumber = await table.locator('tbody tr').first().locator('td').first().innerText();
  await page.getByRole('spinbutton', { name: '页码', exact: true }).fill('2');
  await page.getByRole('spinbutton', { name: '页码', exact: true }).press('Enter');
  await page.waitForFunction(() => document.querySelector('.iqc-sheet tbody td')?.textContent === '101');
  await page.locator('[role="combobox"][aria-label*="物料分类"]').click();
  await page.locator('[role="combobox"][aria-label*="物料分类"]').fill('有机');
  await page.getByRole('option', { name: '有机', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.waitForFunction(() => {
    const rows = [...document.querySelectorAll('.iqc-sheet tbody tr')];
    return rows.length > 0 && rows.every(row => row.children[8].textContent === '有机');
  });
  await page.waitForFunction(() => document.querySelector('input[aria-label="页码"]')?.value === '1');
  await page.locator('[role="combobox"][aria-label*="特性项目"]').click();
  await page.locator('[role="combobox"][aria-label*="特性项目"]').fill('HPLC-A');
  await page.getByRole('option', { name: 'HPLC-A', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.waitForFunction(() => {
    const rows = [...document.querySelectorAll('.iqc-sheet tbody tr')];
    return rows.length > 0 && rows.every(row => row.children[10].textContent === 'HPLC-A');
  });
  const filteredRows = await table.locator('tbody tr').count();
  const cells = await table.locator('tbody tr').evaluateAll(rows => rows.map(row => [...row.children].map(c => c.textContent)));
  if (!cells.some(row => row[15].trim() !== '')) throw new Error('Live IQC values were not loaded');
  // Stored decision parity is checked by the live SQL integration test;
  // null preservation is checked with controlled fixtures, not mutable live data.
  for (const width of [1365, 768]) {
    await page.setViewportSize({ width, height: 900 });
    if (await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth)) {
      throw new Error(`Page overflows at ${width}px`);
    }
    await table.evaluate(el => { el.scrollLeft = el.scrollWidth; });
    const rightEdge = await table.evaluate(el => el.scrollLeft > 0 && el.scrollLeft + el.clientWidth >= el.scrollWidth - 2);
    if (!rightEdge) throw new Error('Cannot scroll to result columns');
    await page.screenshot({ path: `real-results-${width}.png`, fullPage: true });
  }
  await page.setViewportSize({ width: 1365, height: 900 });
  await table.evaluate(el => { el.scrollLeft = 0; });
  await page.screenshot({ path: 'real-measurements.png', fullPage: true });
  const visible = await page.locator('body').innerText();
  for (const forbidden of ['管理员', 'admin=true', '示例数据', '原始数据', 'flag', 'mdw.', 'Traceback']) {
    if (visible.includes(forbidden)) throw new Error(`Internal content leaked: ${forbidden}`);
  }
  const dateInput = page.getByRole('textbox', { name: 'Select a date range.', exact: true });
  await dateInput.fill('2020/01/01 – 2020/01/02');
  await dateInput.press('Tab');
  await page.getByText('请选择检验日期范围并点击“查询”。', { exact: true }).waitFor();
  if (await table.count()) throw new Error('Old data remains after date change');
  await page.getByRole('button', { name: '查询', exact: true }).click();
  await page.getByText('没有符合筛选条件的记录，请调整筛选条件。', { exact: true }).waitFor({ timeout: 90000 });
  await page.screenshot({ path: 'real-empty.png', fullPage: true });
  if (await page.locator('[data-testid="stException"]').count() || errors.length) throw new Error(errors.join('\n') || 'Streamlit exception');
  return { status: 'passed', source: 'live PostgreSQL', columns: headers.length, filteredRows, firstNumber, viewports: [1365, 768] };
}
