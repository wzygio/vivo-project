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
  await page.getByRole('combobox', {name:'产品型号'}).waitFor();
  if (await page.getByTestId('stMultiSelect').count() !== 1 || await page.getByTestId('stSelectbox').count()) throw new Error('Wrong product selector');
  await page.getByRole('button', { name: '查询', exact: true }).click();
  const table = page.getByTestId('stDataFrame');
  await table.waitFor({ timeout: 60000 });
  if (await page.getByRole('spinbutton', { name: '页码', exact: true }).count()) throw new Error('Manual pagination remains');
  for (const width of [1365, 768]) {
    await page.setViewportSize({ width, height: 900 });
    const dates = page.getByRole('textbox', { name: 'Select a date range.', exact: true });
    const a = await dates.boundingBox(), b = await page.getByRole('button', { name: '查询', exact: true }).boundingBox();
    if (!a || !b || Math.abs(a.y - b.y) > 5) throw new Error('Filters not on one row');
    if (await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth)) throw new Error('Page overflows');
    await page.getByRole('heading', {name:'IQC-蒸镀材料',exact:true}).scrollIntoViewIfNeeded();
    await page.screenshot({ path: `native-table-${width}.png`, fullPage: true });
  }
  await table.hover();
  await table.getByRole('button', { name: 'Download as CSV', exact: true }).click();
  await page.waitForFunction(() => window.iqcExport !== null);
  const csv = await page.evaluate(() => window.iqcExport);
  const lines = csv.trim().split(/\r?\n/);
  const headers = lines[0].replace(/^\uFEFF/, '').split(',');
  if (headers.length !== 27 || headers[0] !== '序号' || headers[1] !== '产品型号' || headers[2] !== '量产/非量产' || headers.includes('蒸镀批次') || headers.includes('batch_no')) throw new Error('Wrong export schema');
  if (csv.includes('通用')) throw new Error('WMS product attribute remains');
  const firstMeasurement = headers.indexOf('IQC-1');
  if (headers[firstMeasurement - 2] !== 'IQC结果' || headers[firstMeasurement - 1] !== 'COA结果') throw new Error('Result columns should precede IQC-1');
  const results = lines.slice(1).map(line => line.match(/,(OK|NG),(OK|NG),/));
  if (results.some(result => !result)) throw new Error('Missing computed IQC/COA results in export');
  await page.getByText('IQC／COA结果按测点判定：任一测点为NG则为NG，否则为OK；缺失测点按OK处理。', { exact: true }).waitFor();
  if (lines.length < 102) throw new Error('Export truncated to former page size');
  if (/ticketno|flag|catalog_match|measurement_match|admin=true/i.test(csv)) throw new Error('Export leaks internal fields');
  if (await page.getByTestId('stException').count() || errors.length) throw new Error('Browser exception');
  await page.getByRole('heading', {name:'IQC-蒸镀材料',exact:true}).scrollIntoViewIfNeeded();
  await page.getByRole('combobox', {name:'产品型号'}).click();
  await page.getByRole('option', {name:'M626（CPD2515）',exact:true}).click();
  await page.keyboard.press('Escape');
  await page.getByText('请选择检验日期范围和产品型号并点击“查询”。', {exact:true}).waitFor();
  await table.waitFor({state: 'hidden'});
  if (await table.count()) throw new Error('Product change shows stale results');
  await page.getByRole('button', {name:'查询',exact:true}).click();
  await table.waitFor({timeout:60000});
  await page.evaluate(() => { window.iqcExport = null; });
  await table.hover();
  await table.getByRole('button', {name:'Download as CSV',exact:true}).click();
  await page.waitForFunction(() => window.iqcExport !== null);
  const selectedCsv = await page.evaluate(() => window.iqcExport);
  if (selectedCsv.trim().split(/\r?\n/).slice(1).some(line => line.split(',')[1] !== 'M626')) throw new Error('Product column does not match selector');
  const selectedRows = selectedCsv.trim().split(/\r?\n/).length-1;
  if (selectedRows <= 0 || selectedRows > lines.length-1) throw new Error('Selected export empty or expanded');
  return { status: 'passed', source: 'live PostgreSQL', renderer: 'st.dataframe', exportRows: lines.length - 1, selectedRows, exportColumns: headers.length, viewports: [1365, 768] };
}
