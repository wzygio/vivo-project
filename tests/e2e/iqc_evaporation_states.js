async page => {
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.addInitScript(() => {
    window.iqcExport = null;
    window.showSaveFilePicker = async () => ({createWritable: async () => {
      let contents = '';
      return {write: async bytes => { contents += new TextDecoder().decode(bytes); },
        close: async () => { window.iqcExport = contents; }};
    }});
  });
  await page.setViewportSize({ width: 1365, height: 900 });
  await page.goto('http://localhost:8518/?scenario=failure');
  await page.getByRole('button', { name: '查询', exact: true }).click();
  await page.getByText('蒸镀材料检验数据暂时无法读取，请稍后重新查询。', { exact: true }).waitFor();
  if (await page.getByTestId('stDataFrame').count()) throw new Error('Failure shows stale table');
  const body = await page.locator('body').innerText();
  if (/private SQL|connection details|Traceback|管理员|admin=true/.test(body)) throw new Error('Failure leaks internals');
  await page.goto('http://localhost:8518/');
  await page.getByRole('button', { name: '查询', exact: true }).click();
  const table = page.getByTestId('stDataFrame');
  await table.waitFor();
  await page.getByText('共 205 条记录 · 左右滚动查看全部测点和检验结果', { exact: true }).waitFor();
  if (await page.getByRole('spinbutton', { name: '页码', exact: true }).count()) throw new Error('Manual pagination remains');
  await page.screenshot({ path: 'native-controlled-full-result.png', fullPage: true });
  const exportCsv = async () => {
    await page.evaluate(() => { window.iqcExport = null; });
    await table.hover();
    await table.getByRole('button', {name: 'Download as CSV', exact: true}).click();
    await page.waitForFunction(() => window.iqcExport !== null);
    const csv = await page.evaluate(() => window.iqcExport);
    if (/ticketno|flag|catalog_match|admin=true|private SQL|connection details/i.test(csv)) throw new Error('Export leaks internals');
    const headers = csv.trim().split(/\r?\n/)[0].replace(/^\uFEFF/, '').split(',');
    if (headers.length !== 27 || headers[2] !== '量产/非量产' || headers.includes('蒸镀批次') || headers.includes('batch_no')) throw new Error('Wrong public schema');
    if (csv.includes('通用')) throw new Error('WMS product attribute remains');
    return csv;
  };
  const allCsv = await exportCsv();
  if (allCsv.trim().split(/\r?\n/).length !== 206) throw new Error('Union duplicates or truncates facts');
  await page.getByRole('combobox', {name: '产品型号'}).click();
  await page.getByRole('option', {name: 'M626（CPD2515）', exact: true}).click();
  await page.keyboard.press('Escape');
  await page.getByText('请选择检验日期范围和产品型号并点击“查询”。', {exact: true}).waitFor();
  await table.waitFor({state: 'hidden'});
  if (await table.count()) throw new Error('Product change shows stale table');
  await page.getByRole('button', {name:'查询', exact:true}).click();
  await page.getByText('共 65 条记录 · 左右滚动查看全部测点和检验结果', {exact:true}).waitFor();
  const selectedCsv = await exportCsv();
  if (selectedCsv.trim().split(/\r?\n/).length !== 66 || selectedCsv.includes('100024720')) throw new Error('Export not product-scoped');
  if (!selectedCsv.includes('SHARED-MATERIAL')) throw new Error('Lost shared material');
  if (selectedCsv.trim().split(/\r?\n/).slice(1).some(line => {
    const fields = line.split(',');
    return fields[1] !== 'M626';
  })) throw new Error('Product column does not match selected scope');
  for (const width of [1365, 768, 375]) {
    await page.setViewportSize({width,height:900});
    await page.getByRole('heading', {name:'IQC-蒸镀材料',exact:true}).scrollIntoViewIfNeeded();
    if (await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth)) throw new Error('Page overflows');
    await page.screenshot({path:`product-filter-${width}.png`,fullPage:true});
  }
  await page.goto('http://localhost:8518/?scenario=empty');
  await page.getByRole('button', { name: '查询', exact: true }).click();
  await page.getByText('没有符合筛选条件的记录，请调整筛选条件。', { exact: true }).waitFor();
  if (await table.count()) throw new Error('Common rows shown for unmatched product');
  if (await page.locator('[data-testid="stMultiSelect"]').count() !== 1) throw new Error('Product filter missing');
  if (await page.locator('[data-testid="stException"]').count()) throw new Error('Streamlit exception');
  if (errors.length) throw new Error('Browser error');
  return { status: 'passed', source: 'controlled data port', allRows:205,selectedRows:65,scenarios: ['failure', 'recovery', 'full-result', 'product-scope', 'export', 'empty'],viewports:[1365,768,375] };
}
