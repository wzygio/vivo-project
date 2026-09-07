async page => {
  const output = "output/test-results/monitor-oos";
  const base = "http://localhost:8514/";
  const errors = [];
  page.on("console", message => {
    if (message.type() === "error") errors.push(message.text());
  });

  await page.goto(base);
  await page.getByRole("button", { name: "查询" }).waitFor({ timeout: 60_000 });
  if (await page.getByText("超规趋势", { exact: true }).count()) {
    throw new Error("未点击查询前不应渲染超规趋势");
  }
  if (await page.getByText(/数据最后更新时间/).count()) {
    throw new Error("普通 URL 不应渲染最后更新时间");
  }

  await page.getByRole("button", { name: "查询" }).click();
  await page.getByText("超规趋势", { exact: true }).waitFor({ timeout: 60_000 });
  await page.getByText("Top 10 站点", { exact: true }).waitFor();
  await page.getByText("超规明细", { exact: true }).waitFor();
  await page.getByText("超规记录", { exact: true }).waitFor();
  if (await page.getByText(/数据最后更新时间/).count()) {
    throw new Error("普通 URL 查询后仍不应渲染最后更新时间");
  }
  await page.screenshot({ path: `${output}/regular.png`, fullPage: true });

  await page.goto(`${base}?admin=true`);
  await page.getByRole("button", { name: "查询" }).click();
  await page.getByText("数据最后更新时间（管理员）", { exact: true }).waitFor({ timeout: 60_000 });
  await page.screenshot({ path: `${output}/admin.png`, fullPage: true });

  await page.setViewportSize({ width: 768, height: 900 });
  await page.waitForTimeout(300);
  const overflow = await page.evaluate(() => ({
    scrollWidth: document.documentElement.scrollWidth,
    clientWidth: document.documentElement.clientWidth,
  }));
  if (overflow.scrollWidth > overflow.clientWidth + 2) {
    throw new Error(`768px 视口横向溢出: ${JSON.stringify(overflow)}`);
  }
  await page.screenshot({ path: `${output}/admin-768.png`, fullPage: true });

  if (errors.length) throw new Error(`浏览器控制台错误: ${errors.join(" | ")}`);
  return "OOS monitor E2E passed";
}
