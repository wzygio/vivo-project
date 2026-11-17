// The Python runner owns every temporary server, browser, child and listener.
const { chromium } = require(process.env.PLAYWRIGHT_MODULE_PATH);
const fs = require('node:fs');
const path = require('node:path');

(async () => {
  const output = process.env.VIVO_QTIME_BROWSER_OUTPUT;
  const browser = await chromium.launch({ headless: true, channel: 'chrome' });
  const pageErrors = [];
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
    page.on('pageerror', error => pageErrors.push(error.message));
    await page.goto(process.env.VIVO_QTIME_BROWSER_URL);
    await page.getByText(/^重点站点Q-?Time监控$/).waitFor();
    const query = () => page.getByRole('button', { name: '查询', exact: true }).click();
    const choose = async (label, value) => {
      await page.getByRole('combobox', { name: label, exact: true }).click();
      await page.getByRole('option', { name: value, exact: true }).click();
      await page.getByText(`${label === '测试数据' ? 'fixture_mode' : 'fixture_decision'}: ${value}`, { exact:true }).waitFor();
      await page.locator('[data-testid="stStatusWidget"]').waitFor({state:'hidden'});
    };
    const plots = () => page.locator('.js-plotly-plot');
    const figures = () => plots().evaluateAll(nodes => nodes.map(node => ({
      title: node.layout.title.text.replace(/<[^>]+>/g,''),
      y: node.calcdata[0].map(point => point.s ?? point.y),
      specLines: (node.layout.shapes || []).map(shape => shape.y0),
      traces: node.data.map(trace => trace.type), scrollZoom: node._context.scrollZoom,
    })));
    const healthy = async () => {
      if (await page.locator('[data-testid="stException"]').count()) throw new Error('Streamlit exception');
      if (/FIXTURE_PRIVATE_TRANSPORT_ERROR|Traceback|admin=true/.test(await page.locator('body').innerText())) {
        throw new Error('Private technical detail exposed');
      }
    };
    if (await plots().count()) throw new Error('Query gate did not hold');
    await query();
    await page.waitForFunction(() => document.querySelectorAll('.js-plotly-plot').length === 2
      && [...document.querySelectorAll('.js-plotly-plot')].every(node => node.calcdata?.[0]?.length));
    let actual = await figures();
    if (actual[0].title !== 'M626' || actual[0].y[0] !== 2.5 || actual[1].y[0] !== 1) {
      throw new Error('Source-to-page interval/mapping mismatch: ' + JSON.stringify(actual));
    }
    if (actual.some(item => item.traces.join(',') !== 'bar' || item.scrollZoom !== false)) {
      throw new Error('Missing-spec or scroll behavior changed');
    }
    await page.screenshot({ path: path.join(output,'synthetic.png'), fullPage:true });
    await page.setViewportSize({ width:390, height:844 });
    if (await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 2)) throw new Error('Mobile overflow');
    await page.screenshot({ path: path.join(output,'mobile.png'), fullPage:true });
    await page.setViewportSize({ width:1440, height:1000 });
    await choose('测试规格与决策','默认修饰');
    await query();
    await page.waitForFunction(() => {
      const node=document.querySelector('.js-plotly-plot');
      const value=node?.calcdata?.[0]?.[0]?.s;
      return value >= 1.7 && value <= 1.9;
    });
    await choose('测试规格与决策','保留预警');
    await query();
    await page.getByText(/检测到 1 条已确认超规/).waitFor();
    await choose('测试规格与决策','删除记录');
    await query();
    await page.waitForFunction(() => document.querySelectorAll('.js-plotly-plot').length === 1);
    if ((await figures())[0].title !== 'M678') throw new Error('Delete did not use original decision path');
    await choose('测试规格与决策','正式规格');
    await choose('测试数据','真实只读样本');
    await query();
    await page.waitForFunction(() => document.querySelectorAll('.js-plotly-plot').length >= 6);
    const captured = await figures();
    if (captured.reduce((sum,item) => sum + item.y.length,0) < 100) throw new Error('Captured real data not rendered');
    if (captured.some(item => item.specLines.length !== 1 || item.specLines[0] !== 200
        || item.y.some(value => value >= 200))) throw new Error('Approved 200-hour specification/decoration not applied');
    await page.screenshot({path:path.join(output,'captured-real.png'),fullPage:true});
    for (const mode of ['空结果','读取失败']) {
      await choose('测试数据',mode);
      await query();
      await page.waitForFunction(() => document.querySelectorAll('.js-plotly-plot').length === 0);
      await page.locator('[data-testid="stStatusWidget"]').waitFor({state:'hidden'});
      if (await plots().count()) throw new Error('Stale plots after '+mode);
      await healthy();
    }
    await choose('测试数据','合成样本');
    await query();
    await page.waitForFunction(() => document.querySelectorAll('.js-plotly-plot').length === 2);
    await healthy();
    if (pageErrors.length) throw new Error(pageErrors.join('\n'));
    fs.writeFileSync(path.join(output,'result.json'),JSON.stringify({passed:true,
      scope:'bounded captured SQL transport -> real fixed source -> repository/snapshot/service -> existing page/charts',
      scenarios:['synthetic intervals and product mapping','missing spec','True/False/Delete','approved 200-hour specification','captured real data','empty','failure','recovery','mobile','scroll'],
      synthetic:actual,captured,pageErrors},null,2));
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode=1; });
