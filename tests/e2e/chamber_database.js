async page => {
  const base = 'http://127.0.0.1:8537/';
  const output = 'D:/wzy/Python/vivo-project/output/test-results/qtime-chamber-db';
  const query = () => page.getByRole('button', {name: '查询单腔', exact: true}).click();
  const healthy = async () => {
    if (await page.locator('[data-testid="stException"]').count()) throw Error('Streamlit exception');
    const body = await page.locator('body').innerText();
    for (const word of ['DISABLED_GLASS', 'DISABLED_PRODUCT', 'DB_OUTLIER', 'admin=true',
                        'SELECT ', 'statement_timeout', 'chamber_database.py', 'sqlite', 'Traceback']) {
      if (body.includes(word)) throw Error('Unexpected public content: '+word);
    }
  };
  const choose = async (label, option) => {
    await page.getByRole('combobox', {name: label, exact: true}).click();
    await page.getByRole('option', {name: new RegExp('^'+option+'(?:（.*）)?$')}).click();
    await page.keyboard.press('Escape');
  };
  const figures = () => page.evaluate(() => [...document.querySelectorAll('.js-plotly-plot')].map(p => ({
    names: p.data.map(t => t.name), y: p.data.flatMap(t => Array.from(t.y)),
    max: p.layout.yaxis.range[1], types: p.data.map(t => t.type),
    markers: p.querySelectorAll('.scatterlayer .point').length
  })));
  await page.setViewportSize({width:1440,height:1000});
  await page.goto(base);
  await page.getByText('请选择筛选条件并点击“查询单腔”。', {exact:true}).waitFor();
  await query();
  await page.waitForFunction(() => document.querySelectorAll('.js-plotly-plot').length === 2);
  const initial = await figures();
  if (initial.some(p => p.max !== 1200 || p.types.some(t => t !== 'scatter') || !p.markers)
      || initial.flatMap(p => p.y).some(y => y > 1000)
      || Math.round(initial.flatMap(p=>p.y).reduce((a,b)=>a+b,0)) !== 1120) throw Error('Wrong SQL points');
  await healthy();
  await page.screenshot({path:output+'/database-fixture.png',fullPage:true});
  await choose('产品型号','M678');
  await page.waitForFunction(() => document.querySelectorAll('.js-plotly-plot').length === 1
    && [...document.querySelectorAll('.js-plotly-plot')].every(p=>p.data.every(t=>t.name==='M678')));
  await choose('线体','3CEE001');
  await page.getByText('当前筛选条件下暂无单腔停留时间数据。',{exact:true}).waitFor();
  await healthy();
  await page.getByRole('button',{name:'刷新单腔缓存',exact:true}).click();
  await page.getByText('请选择筛选条件并点击“查询单腔”。',{exact:true}).waitFor();

  await page.goto(base);
  await page.getByText('请选择筛选条件并点击“查询单腔”。',{exact:true}).waitFor();
  await query();
  await page.locator('[data-testid="stPlotlyChart"]').first().waitFor();
  const dates = page.locator('[data-testid="stDateInput"] input');
  await dates.nth(1).fill('2026/09/02');
  await dates.nth(1).press('Enter');
  await dates.nth(0).fill('2026/09/02');
  await dates.nth(0).press('Enter');
  await page.getByText('请选择筛选条件并点击“查询单腔”。',{exact:true}).waitFor();
  if (await page.locator('[data-testid="stPlotlyChart"]').count()) throw Error('Stale charts after date change');
  await query();
  await page.waitForFunction(() => document.querySelectorAll('.js-plotly-plot').length === 1);
  const changed = await figures();
  if (Math.abs(changed[0].y[0]-40)>0.001) throw Error('Date cache reused wrong data');
  await dates.nth(0).fill('2026/08/01');
  await dates.nth(0).press('Enter');
  await page.getByText('请选择筛选条件并点击“查询单腔”。',{exact:true}).waitFor();
  await query();
  await page.getByText('请选择先后有序且不超过31天的日期范围。',{exact:true}).waitFor();
  if (await page.locator('[data-testid="stPlotlyChart"]').count()) throw Error('Invalid date left stale charts');
  await healthy();
  for (const [scenario,text] of [['empty','当前筛选条件下暂无单腔停留时间数据。'],
                                ['failure','蒸镀单腔停留时间数据读取失败，请稍后重试。']]) {
    await page.goto(base+'?scenario='+scenario);
    await page.getByText('请选择筛选条件并点击“查询单腔”。',{exact:true}).waitFor();
    await query();
    await page.getByText(text,{exact:true}).waitFor();
    if (await page.locator('[data-testid="stPlotlyChart"]').count()) throw Error('Stale '+scenario);
    await healthy();
  }
  await page.goto(base);
  await page.getByText('请选择筛选条件并点击“查询单腔”。',{exact:true}).waitFor();
  await query();
  await page.locator('[data-testid="stPlotlyChart"]').first().waitFor();
  await page.setViewportSize({width:390,height:844});
  await page.screenshot({path:output+'/database-mobile.png',fullPage:true});
  if (await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+2)) throw Error('Mobile overflow');
  await healthy();
  await page.goto(base+'?scenario=live');
  await page.getByText('请选择筛选条件并点击“查询单腔”。',{exact:true}).waitFor();
  await query();
  await page.locator('[data-testid="stPlotlyChart"]').first().waitFor({timeout:90000});
  await healthy();
  const live = await figures();
  if (live.some(p=>p.max!==1200 || p.y.some(y=>y>1000) || !p.markers)) throw Error('Invalid live data');
  await page.setViewportSize({width:1440,height:1000});
  await page.screenshot({path:output+'/database-live.png',fullPage:true});
  await page.locator('[data-testid="stMultiSelect"]').filter({
    has: page.getByRole('combobox',{name:/腔室/})
  }).getByRole('button',{name:'Clear all',exact:true}).click();
  await page.waitForFunction(()=>document.querySelectorAll('.js-plotly-plot').length===10
    && [...document.querySelectorAll('.js-plotly-plot')].every(p=>p.querySelector('.scatterlayer .point')));
  const allChambers = await figures();
  if (allChambers.some(p=>p.max!==1200 || p.y.some(y=>y>1000) || !p.markers)) throw Error('Invalid all-chamber points');
  await healthy();
  return {ok:true,initial,changed,liveCharts:allChambers.length,livePoints:allChambers.reduce((n,p)=>n+p.y.length,0),
    checks:['SQL pairing','products/lines/chambers','date/cache','oversized dates','refresh','empty/failure/recovery','mobile','live database']};
}
