async page => {
  const output = 'D:/wzy/Python/vivo-project/output/test-results/qtime-chamber';
  const openAdmin = async () => {
    const container = page.locator('[data-testid="stExpander"]').filter({hasText: '开发者后台：Q-Time 超规数据修饰'});
    if (!await container.locator('details').getAttribute('open').then(value => value !== null)) {
      await page.getByText('开发者后台：Q-Time 超规数据修饰', {exact: true}).click();
    }
  };
  const counts = async () => {
    const body = await page.locator('body').innerText();
    return Object.fromEntries(['page', 'station', 'chamber'].map(module => [module,
      Math.max(...[...body.matchAll(new RegExp(module + '-executions: (\\d+)', 'g'))].map(match => Number(match[1])))
    ]));
  };
  const downloadLedger = async name => {
    await openAdmin();
    const pending = page.waitForEvent('download');
    await page.getByRole('button', {name: '下载修饰表', exact: true}).click();
    const download = await pending;
    await download.saveAs(output + '/' + name);
    return (await (await page.request.get(download.url())).body()).toString('base64');
  };
  await page.goto('http://localhost:8521/?admin=true');
  await page.getByRole('button', {name: '查询单腔', exact: true}).click();
  await page.getByText('50.00%', {exact: true}).waitFor();
  await page.getByRole('button', {name: '查询', exact: true}).click();
  const beforeWorkbook = await downloadLedger('admin-before.xlsx');
  // Use an in-memory workbook: enterprise filesystem encryption alters bytes read by Node.
  const upload = await (await page.request.get('http://127.0.0.1:8523/admin-decision-upload.json')).json();
  await page.locator('input[type="file"]').evaluate((input, encoded) => {
    const bytes = Uint8Array.from(atob(encoded), character => character.charCodeAt(0));
    const transfer = new DataTransfer();
    transfer.items.add(new File([bytes], 'admin-decision-upload.xlsx',
      {type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'}));
    input.files = transfer.files;
    input.dispatchEvent(new Event('change', {bubbles: true}));
  }, upload.base64);
  await page.getByRole('button', {name: '确认覆盖并刷新', exact: true, includeHidden: true}).waitFor({state: 'attached'});
  await openAdmin();
  await page.getByRole('button', {name: '确认覆盖并刷新', exact: true}).waitFor();
  const before = await counts();
  await page.getByRole('button', {name: '确认覆盖并刷新', exact: true}).click();
  await page.getByText('station-decision-revision: 1', {exact: true}).first().waitFor();
  await page.getByText('fixture-result-flags: [True, True]', {exact: true}).first().waitFor();
  // This result is emitted after the rebuilt admin download controls.
  await page.getByText('当前查询窗口内未发现超规的 Q-Time 记录。', {exact: true}).waitFor({state: 'attached'});
  await page.getByRole('button', {name: '下载修饰表', exact: true, includeHidden: true}).waitFor({state: 'attached'});
  const afterWorkbook = await downloadLedger('admin-after.xlsx');
  const after = await counts();
  if (await page.locator('[data-testid="stException"]').count()) throw new Error('Save raised Streamlit exception');
  if (before.chamber !== after.chamber || before.page !== after.page || after.station <= before.station) {
    throw new Error('Admin save reran another module: ' + JSON.stringify({before, after}));
  }
  if (beforeWorkbook === afterWorkbook) throw new Error('Download retained old decisions');
  return {ok: true, before, after, workbookChanged: true};
}
