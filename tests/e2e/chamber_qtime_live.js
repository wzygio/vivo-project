async page => {
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto('http://localhost:8522/');
  await page.getByRole('heading', { name: '蒸镀单腔停留时间监控' }).waitFor({ timeout: 30000 });
  await page.getByRole('button', { name: '查询单腔', exact: true }).click();
  await page.locator('[data-testid="stPlotlyChart"]').first().waitFor({ timeout: 60000 });
  await page.getByText('单腔停留时间明细', { exact: true }).waitFor({ timeout: 30000 });
  if (await page.locator('[data-testid="stException"]').count()) throw new Error('Live page exception');
  const metrics = await page.locator('[data-testid="stMetricValue"]').allTextContents();
  if (metrics[0] !== '3,705') throw new Error('Unexpected validated source glass count ' + metrics);
  await page.screenshot({ path: 'D:/wzy/Python/vivo-project/output/test-results/qtime-chamber/live-page.png', fullPage: true });
  await page.getByRole('combobox', { name: '产品型号', exact: true }).click();
  await page.getByRole('option', { name: 'M626', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.waitForFunction(() => document.querySelector('[data-testid="stMetricValue"]')?.textContent === '1,405');
  await page.getByRole('combobox', { name: '线体', exact: true }).click();
  await page.getByRole('option', { name: '3CEE001', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.getByText('当前筛选条件下暂无单腔停留时间数据。', { exact: true }).waitFor();
  return { ok: true, glasses: 3705, m626: 1405, line1M626: 0 };
}
