async page => {
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.goto('http://localhost:8517');
  await page.getByRole('heading', { name: 'IQC-蒸镀材料', exact: true }).waitFor();
  const product = page.getByRole('combobox', { name: '产品型号', exact: true });
  const dates = page.getByRole('textbox', { name: 'Select a date range.', exact: true });
  await product.waitFor();
  if (await page.locator('[data-testid="stMultiSelect"]').count()) throw new Error('Obsolete filters remain');
  for (const width of [1365, 768]) {
    await page.setViewportSize({ width, height: 900 });
    const a = await dates.boundingBox(), b = await product.boundingBox();
    if (!a || !b || Math.abs(a.y - b.y) > 5) throw new Error('Date and product are not on one row');
    if (await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth)) throw new Error('Page overflows');
    await page.screenshot({ path: `product-filters-${width}.png`, fullPage: true });
  }
  await product.click();
  await page.getByRole('option', { name: '通用', exact: true }).click();
  await page.getByRole('button', { name: '查询', exact: true }).click();
  await page.waitForFunction(() => document.querySelector('.iqc-sheet') || document.body.innerText.includes('没有符合筛选条件的记录'));
  const table = page.locator('.iqc-sheet');
  await table.waitFor({ timeout: 60000 });
  if (await table.count()) {
    const rows = await table.locator('tbody tr').evaluateAll(rows => rows.map(r => [...r.children].map(c => c.textContent)));
    if (rows.some(r => r[1] !== '通用' || r[8] !== '有机' || r[9] !== 'V3')) throw new Error('Report scope violated');
    if ((await table.locator('th').allTextContents())[1] !== '产品型号') throw new Error('Column not renamed');
  }
  const headers = await table.locator('th').allTextContents();
  if (headers.length !== 27) throw new Error('Missing report columns');
  let measuredRows = 0, totalRows = 0;
  const pager = page.getByRole('spinbutton', { name: '页码', exact: true });
  const pages = Number(await pager.getAttribute('max'));
  for (let number = 1; number <= pages; number++) {
    if (number > 1) {
      await pager.fill(String(number));
      await pager.press('Enter');
      await page.waitForFunction(n => document.querySelector('.iqc-sheet tbody tr td')?.textContent.trim() === String((n - 1) * 100 + 1), number);
    }
    const rows = await table.locator('tbody tr').evaluateAll(rows => rows.map(r => [...r.children].map(c => c.textContent.trim())));
    totalRows += rows.length;
    for (const row of rows) {
      if (!row[10]) throw new Error('Missing characteristic');
      if (row[1] !== '通用' || row[8] !== '有机' || row[9] !== 'V3') throw new Error('Scope violated');
      if (row[15]) measuredRows++;
    }
  }
  if (!measuredRows) throw new Error('No database measurements displayed');
  await page.screenshot({ path: 'common-live-query.png', fullPage: true });
  if (await page.locator('[data-testid="stException"]').count() || errors.length) throw new Error('Browser exception');
  return { status: 'passed', source: 'live PostgreSQL', filters: ['date', 'enabled product + common'], totalRows, measuredRows, viewports: [1365, 768], hasRows: await table.count() > 0 };
}
