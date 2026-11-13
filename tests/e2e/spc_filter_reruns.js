async page => {
  const checks = [];
  const requireText = async (text) => {
    await page.getByText(text, { exact: true }).waitFor();
    checks.push(text);
  };
  await requireText('Fixture load calls: 1');
  await page.getByRole('combobox', { name: '站点', exact: true }).click();
  await page.getByRole('option', { name: '1B990', exact: true }).click();
  await page.keyboard.press('Escape');
  await requireText('Fixture load calls: 1');
  // Selecting the station automatically selects both available parameters.
  await page.getByRole('combobox', { name: /参数名称/ }).press('Backspace');
  await requireText('当前筛选条件尚未查询。');
  await requireText('Fixture load calls: 1');
  await page.getByRole('button', { name: '查询', exact: true }).click();
  await requireText('Fixture query result: 1');
  await requireText('Fixture load calls: 1');
  await page.getByRole('combobox', { name: '厂别', exact: true }).click();
  await page.getByRole('option', { name: 'OLED', exact: true }).click();
  await requireText('当前筛选条件尚未查询。');
  if (!(await page.getByRole('button', { name: '查询', exact: true }).isDisabled())) {
    throw new Error('Factory change did not clear the submitted query');
  }
  if (await page.getByText('Fixture query result: 1', { exact: true }).count()) {
    throw new Error('Unsubmitted factory selection still shows old charts');
  }
  await requireText('Fixture load calls: 1');
  await page.getByRole('combobox', { name: '站点', exact: true }).click();
  await page.getByRole('option', { name: '2120A', exact: true }).click();
  await page.keyboard.press('Escape');
  await page.getByRole('button', { name: '查询', exact: true }).click();
  await requireText('Fixture query result: 2');
  await requireText('Fixture load calls: 1');
  await requireText('Fixture published alerts remain visible');
  // Positive control: a full page action must still enter the real page loader.
  await page.getByRole('button', { name: 'Fixture full page reload', exact: true }).click();
  await requireText('Fixture load calls: 2');
  return { passed: true, checks, scope: 'isolated real SPC page; no database or workbook I/O' };
}
