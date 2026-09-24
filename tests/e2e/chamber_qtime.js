async page => {
  const base = 'http://localhost:8521/';
  const output = 'D:/wzy/Python/vivo-project/output/test-results/qtime-chamber';
  const query = () => page.getByRole('button', { name: '查询单腔', exact: true });
  const waitText = text => page.getByText(text, { exact: false }).first().waitFor({ timeout: 30000 });
  const select = async (label, value) => {
    await page.getByRole('combobox', { name: new RegExp(label) }).click();
    await page.getByRole('option', { name: value, exact: true }).click();
    await page.keyboard.press('Escape');
  };
  const healthy = async () => {
    if (await page.locator('[data-testid="stException"]').count()) throw new Error('Streamlit exception');
    const body = await page.locator('body').innerText();
    for (const forbidden of ['INTERNAL_SECRET_SQL', 'DISABLED_PRODUCT', 'DISABLED_GLASS', 'admin=true', '修饰', '配置路径']) {
      if (body.includes(forbidden)) throw new Error('Leaked ' + forbidden);
    }
  };
  const downloadAndVerify = async (file, expectedRows, expectedProducts) => {
    const pendingDownload = page.waitForEvent('download');
    await page.getByRole('button', { name: '下载单腔明细', exact: true }).click();
    const download = await pendingDownload;
    await download.saveAs(output + '/' + file);
    const response = await page.request.get(download.url());
    const csv = (await response.text()).replace(/^\uFEFF/, '').trim();
    const rows = csv.split(/\r?\n/).map(row => row.split(','));
    if (rows.length !== expectedRows + 1) throw new Error('CSV row count');
    if (rows[0].join(',') !== '产品型号,线体,GlassID,进片时间,腔室,停留时间（秒）,目标值（秒）,状态') throw new Error('CSV field projection');
    if (rows.slice(1).some(row => !expectedProducts.includes(row[0]) || row.length !== 8)) throw new Error('CSV product scope');
    if (rows.slice(1).filter(row => row[5] === '').length !== 2) throw new Error('CSV missing values');
    if (csv.includes('DISABLED') || csv.includes('flag')) throw new Error('CSV disclosure');
  };
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto(base);
  await waitText('请选择筛选条件并点击“查询单腔”。');
  const headings = await page.locator('h3').allTextContents();
  if (headings.indexOf('北极星QTime监控') >= headings.indexOf('蒸镀单腔停留时间监控')) throw new Error('Incorrect module order');
  await query().click();
  await waitText('50.00%');
  await page.locator('[data-testid="stPlotlyChart"]').first().waitFor();
  await healthy();
  await page.screenshot({ path: output + '/normal.png', fullPage: true });
  await select('产品型号', 'M678');
  await waitText('0.00%');
  await select('线体', '3CEE001');
  await waitText('当前筛选条件下暂无单腔停留时间数据。');
  await page.screenshot({ path: output + '/filter-empty.png', fullPage: true });
  await page.getByRole('button', { name: '刷新单腔缓存', exact: true }).click();
  await waitText('请选择筛选条件并点击“查询单腔”。');

  await page.goto(base);
  await waitText('请选择筛选条件并点击“查询单腔”。');
  await query().click();
  await waitText('50.00%');
  await select('腔室', 'OC1->OC2');
  await page.waitForFunction(() => document.querySelectorAll('[data-testid="stPlotlyChart"]').length === 2);
  await page.getByText('单腔停留时间明细', { exact: true }).click();
  await downloadAndVerify('details.csv', 6, ['M626', 'M678']);
  await select('产品型号', 'M678');
  await select('线体', '3CEE002');
  await waitText('0.00%');
  await downloadAndVerify('filtered-details.csv', 4, ['M678']);
  await healthy();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({ path: output + '/mobile.png', fullPage: true });
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 2);
  if (overflow) throw new Error('Viewport overflow');

  await page.goto(base + '?scenario=failure');
  await waitText('请选择筛选条件并点击“查询单腔”。');
  await query().click();
  await waitText('蒸镀单腔停留时间数据读取失败，请稍后重试。');
  await healthy();
  if (await page.locator('[data-testid="stPlotlyChart"]').count()) throw new Error('Stale result after failure');
  await page.screenshot({ path: output + '/failure.png', fullPage: true });
  await page.goto(base + '?scenario=empty');
  await waitText('请选择筛选条件并点击“查询单腔”。');
  await query().click();
  await waitText('当前筛选条件下暂无单腔停留时间数据。');
  await healthy();
  await page.goto(base);
  await waitText('请选择筛选条件并点击“查询单腔”。');
  await query().click();
  await waitText('50.00%');
  await healthy();
  return { ok: true, checks: ['initial gate', 'module order', 'product/line/chamber filters', 'empty', 'refresh', 'CSV', 'mobile fit', 'safe failure', 'recovery'] };
}
