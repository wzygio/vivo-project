async page => {
  await page.goto(
    "http://localhost:8503/%E8%87%AA%E5%8A%A8%E9%A2%84%E8%AD%A6%E7%9C%8B%E6%9D%BF?admin=true",
  );

  const configExpander = page
    .locator("summary")
    .filter({ hasText: "数据修饰配置" });
  await configExpander.waitFor({ timeout: 120_000 });
  await configExpander.click();

  await page
    .getByText(
      "配置来源：resources/compliance_config.xlsx。每一行都是启用规则，支持 ALL。",
      { exact: true },
    )
    .waitFor({ timeout: 120_000 });

  const panel = configExpander.locator(
    'xpath=ancestor::div[@data-testid="stExpander"]',
  );
  const panelText = await panel.innerText();
  for (const removedText of ["规则键", "周别", "默认配置", "备注"]) {
    if (panelText.includes(removedText)) {
      throw new Error(`修饰配置面板仍显示已取消字段: ${removedText}`);
    }
  }

  const downloadPromise = page.waitForEvent("download");
  await panel.getByRole("button", { name: "📥 下载配置文件" }).click();
  const download = await downloadPromise;
  if (download.suggestedFilename() !== "compliance_config.xlsx") {
    throw new Error(`配置下载文件名错误: ${download.suggestedFilename()}`);
  }
  await download.saveAs(
    "output/test-results/monitor-compliance-e2e/downloaded-compliance-config.xlsx",
  );

  const errorCount = await page
    .getByText(/配置文件解析失败|读取 compliance 配置失败|Traceback/)
    .count();
  if (errorCount !== 0) {
    throw new Error("自动预警页面存在修饰配置加载错误。");
  }

  return {
    url: page.url(),
    dimensions: ["厂别", "产品型号", "监控类型", "月份"],
    download: download.suggestedFilename(),
  };
}
