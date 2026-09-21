async page => {
  await page.goto("http://localhost:8517");
  const visible = async text => page.getByText(text, { exact: true }).waitFor();
  const choose = async (label, value) => {
    await page.getByRole("combobox", { name: new RegExp(`${label}$`) }).click();
    await page.getByRole("option", { name: value, exact: true }).click();
    await page.keyboard.press("Escape");
  };
  const unchanged = async count => {
    await visible(`Data loads: ${count}; Alert scans: ${count}`);
    if (await page.locator('[data-testid="stException"]').count()) {
      throw new Error("Streamlit exception during fragment interaction");
    }
  };

  await unchanged(1);
  await choose("不良 Group", "Group-A");
  await choose("不良 Code", "Code-A");
  await visible("当前筛选条件尚未查询。");
  await unchanged(1);
  await page.getByRole("button", { name: "查询", exact: true }).click();
  await visible("Detail codes: Code-A");
  await visible("Detail source: M678/0");
  await unchanged(1);
  await choose("Detail interaction", "Lot-2");
  await unchanged(1);

  await choose("不良 Group", "Group-B");
  await visible("当前筛选条件尚未查询。");
  await page.getByText("Detail source: M678/0", { exact: true }).waitFor({ state: "hidden" });
  await choose("不良 Code", "Code-B");
  await page.getByRole("button", { name: "查询", exact: true }).click();
  await visible("Detail codes: Code-A,Code-B");
  await unchanged(1);

  await page.getByRole("button", { name: "Refresh cache", exact: true }).click();
  await visible("Detail source: M678/1");
  await unchanged(2);
  await choose("Product", "M626");
  await visible("请至少选择一个 Defect Group 和 Defect Code。");
  await page.getByText("Detail source: M678/1", { exact: true }).waitFor({ state: "hidden" });
  await unchanged(3);
  await choose("不良 Group", "Group-C");
  await choose("不良 Code", "Code-C");
  await page.getByRole("button", { name: "查询", exact: true }).click();
  await visible("Detail source: M626/1");
  await visible("Detail codes: Code-C");
  await unchanged(3);
  await page.screenshot({ path: "yield-code-fragment.png", fullPage: true });
  return { passed: true, filterInteractionsFullRuns: 1, afterRefresh: 2, afterProductChange: 3 };
}
