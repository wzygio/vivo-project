const assert = require('node:assert/strict');
const path = require('node:path');
const { chromium } = require(process.env.PLAYWRIGHT_MODULE_PATH);

(async () => {
  const browser = await chromium.launch({ headless: true, channel: 'chrome' });
  try {
    const page = await browser.newPage({ viewport: { width: 1365, height: 1000 } });
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    const url = new URL(process.env.VIVO_QTIME_BROWSER_URL);
    url.searchParams.set('scenario', 'spec');
    await page.goto(url.href);
    await page.getByText('发现 1 个低于 0.95 的规格点。', {exact:true}).waitFor();
    assert.equal(await page.locator('.js-plotly-plot').count(), 0);
    await page.getByRole('button', { name: '查询', exact: true }).click();
    await page.waitForFunction(() =>
      document.querySelectorAll('.js-plotly-plot').length === 12 &&
      [...document.querySelectorAll('.js-plotly-plot')].every(el => el.calcdata?.length));
    const manual = page.locator('.st-key-iqc_lifetime_manual_results');
    for (const [product, hours] of [['M626', [620]], ['M678', [660, 535, 665, 420]]]) {
      const batch = manual.getByTestId('stExpander').filter({has: page.locator('summary', {hasText: product})});
      const charts = await batch.locator('.js-plotly-plot').evaluateAll(els => els.map(el => ({
        metric: el.layout.yaxis.title.text, traces: el.data, labels: el.layout.annotations || [],
        texts: Array.from(el.querySelectorAll('.textpoint text'), t => t.textContent),
      })));
      assert.equal(charts.length, hours.length * 2 + 1);
      for (const [i, chart] of charts.entries()) {
        const markers = chart.traces.filter(t => t.mode === 'markers+text');
        if (chart.metric === 'JNCD') {
          assert.equal(markers.length, 0);
          continue;
        }
        const hour = hours[i % hours.length];
        const marker = markers[0];
        assert.deepEqual(marker.x, [hour]);
        const expected = chart.metric === '效率衰减' ? 0.965 - 0.000065 * (hour - 400) : 1.0084;
        assert.ok(Math.abs(marker.y[0] - expected) < 1e-10);
        assert.ok(chart.texts.includes(expected.toFixed(4)));
        assert.ok(chart.labels.some(item => item.text === '规格 ' + hour + 'H'));
        assert.ok(chart.labels.some(item => item.text === '0.95'));
        const forecast = chart.traces.find(t => t.line?.dash === 'dash');
        assert.deepEqual(forecast.x, [400, 1200]);
        assert.ok(Math.abs(forecast.y[1] - (chart.metric === '效率衰减' ? 0.913 : 1.0084)) < 1e-10);
      }
    }
    await page.screenshot({path:path.join(process.env.VIVO_Z622_BROWSER_OUTPUT, 'lifetime-specs.png')});
    assert.equal(await page.getByTestId('stException').count(), 0);
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({status:'passed', charts:12, forecast_hours:1200, report_alerts:1}));
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
