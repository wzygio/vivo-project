async page => {
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  // Capture the native save-file stream without an operating-system dialog.
  await page.addInitScript(() => {
    window.iqcExport = null;
    window.showSaveFilePicker = async () => ({
      createWritable: async () => {
        let contents = '';
        return {
          write: async bytes => { contents += new TextDecoder().decode(bytes); },
          close: async () => { window.iqcExport = contents; },
        };
      },
    });
  });
  await page.goto('http://localhost:8517');
  const product = page.getByTestId('stMultiSelect').getByRole('combobox');
  await product.click();
  await page.getByRole('option', { name: '通用', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.getByRole('button', { name: '查询', exact: true }).click();
  const table = page.getByTestId('stDataFrame');
  await table.waitFor({ timeout: 60000 });
  if (await page.getByRole('spinbutton', { name: '页码', exact: true }).count()) throw new Error('Manual pagination remains');
  for (const width of [1365, 768]) {
    await page.setViewportSize({ width, height: 900 });
    const dates = page.getByRole('textbox', { name: 'Select a date range.', exact: true });
    const a = await dates.boundingBox(), b = await product.boundingBox();
    if (!a || !b || Math.abs(a.y - b.y) > 5) throw new Error('Filters not on one row');
    if (await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth)) throw new Error('Page overflows');
    await page.screenshot({ path: `native-table-${width}.png`, fullPage: true });
  }
  await table.hover();
  await table.getByRole('button', { name: 'Download as CSV', exact: true }).click();
  await page.waitForFunction(() => window.iqcExport !== null);
  const csv = await page.evaluate(() => window.iqcExport);
  const lines = csv.trim().split(/\r?\n/);
  const headers = lines[0].replace(/^\uFEFF/, '').split(',');
  if (headers.length !== 27 || headers[0] !== '序号' || headers[1] !== '产品型号') throw new Error('Wrong export schema');
  const results = lines.slice(1).map(line => line.match(/,(OK|NG),(OK|NG)$/));
  if (results.some(result => !result)) throw new Error('Missing computed IQC/COA results in export');
  await page.getByText('IQC／COA结果按测点判定：任一测点为NG则为NG，否则为OK；缺失测点按OK处理。', { exact: true }).waitFor();
  if (lines.length < 102) throw new Error('Export truncated to former page size');
  if (/ticketno|flag|catalog_match|measurement_match|admin=true/i.test(csv)) throw new Error('Export leaks internal fields');
  if (await page.getByTestId('stException').count() || errors.length) throw new Error('Browser exception');
  return { status: 'passed', source: 'live PostgreSQL', renderer: 'st.dataframe', exportRows: lines.length - 1, exportColumns: headers.length, viewports: [1365, 768] };
}
