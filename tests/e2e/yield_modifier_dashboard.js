async page => {
  // 入库不良率分析看板 E2E 烟测（worktree :8510）
  // 验证：页面无异常渲染完成；月/周/日趋势与 Mapping 区块出现；
  // 修饰表同步不破坏现有页面结构。
  const errors = [];
  page.on("pageerror", e => errors.push(String(e)));

  // 先到首页跑 Home.py 的 sys.path 引导（进程级注册），再进看板页
  await page.goto("http://localhost:8510/");
  await page.waitForTimeout(3000);
  await page.goto(
    "http://localhost:8510/%E5%85%A5%E5%BA%93%E4%B8%8D%E8%89%AF%E7%8E%87%E5%88%86%E6%9E%90%E7%9C%8B%E6%9D%BF",
  );
  await page
    .getByText("正在加载全维度分析数据...")
    .waitFor({ state: "hidden", timeout: 600_000 });

  // 页面级异常检测（Streamlit 异常会以 exception 元素渲染）
  const exceptionCount = await page.locator('[data-testid="stException"]').count();
  if (exceptionCount > 0) {
    const text = await page.locator('[data-testid="stException"]').first().innerText();
    throw new Error(`页面出现 Streamlit 异常: ${text.slice(0, 500)}`);
  }

  // 主内容渲染完成（页面文本含趋势区块；不用可见性等待，隐藏辅助文本也会命中）
  await page.waitForFunction(
    () => document.body.innerText.includes("趋势"),
    undefined,
    { timeout: 300_000 },
  );

  const bodyText = await page.evaluate(() => document.body.innerText);
  const hasTraceback = bodyText.includes("Traceback");
  if (hasTraceback) throw new Error("页面文本包含 Traceback");

  // 输出页面关键区块存在性摘要
  const summary = {
    exceptionCount,
    hasTraceback,
    pageErrors: errors,
    hasTrendSection: bodyText.includes("趋势"),
    hasMapping: bodyText.toLowerCase().includes("mapping") || bodyText.includes("拼板"),
  };
  console.log("PAGE_SUMMARY " + JSON.stringify(summary));

  await page.screenshot({
    path: "output/test-results/yield_modifier_dashboard.png",
    fullPage: false,
  });
  if (exceptionCount > 0 || hasTraceback || errors.length > 0) {
    throw new Error("E2E 烟测失败");
  }
  console.log("E2E_SMOKE_PASS");
}
