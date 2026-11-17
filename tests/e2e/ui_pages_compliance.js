async page => {
  const params=new URL(page.url()).searchParams;
  const scope=params.get('scope'), grain=params.get('grain'), report=params.get('report'), mode=params.get('mode');
  const text=async value=>page.getByText(value,{exact:true}).waitFor({timeout:20000});
  const changes=[];
  if(scope){
    const publications=scope==='ctq'?0:1;
    const counter=`Fixture loads: 1; publications: ${publications}`;
    await text(counter);
    await page.getByRole('combobox',{name:'站点',exact:true}).click();
    await page.getByRole('option',{name:'S1',exact:true}).click();
    await page.keyboard.press('Escape');
    await text(counter);
    await page.getByRole('button',{name:'查询',exact:true}).click();
    await page.getByText('Fixture queried chart',{exact:false}).waitFor();
    if(scope==='aoi_tt'){
      await page.getByRole('combobox',{name:/Particle Size/}).press('Backspace');
      await page.keyboard.press('Escape');
      await text('当前筛选条件尚未查询。');
      if(await page.getByText('Fixture queried chart',{exact:false}).count()) throw Error('Particle change retained unsubmitted charts');
      await page.getByRole('button',{name:'查询',exact:true}).click();
      await page.getByText('Fixture queried chart',{exact:false}).waitFor();
    }
    await text(counter);
    await page.getByRole('button',{name:'Fixture full page reload',exact:true}).click();
    await text(`Fixture loads: 2; publications: ${publications*2}`);
    changes.push('cascade/query kept source and publication at 1; full run advanced to 2');
  }else if(grain){
    await text('Fixture summary: 1; loads: 1');
    const area=page.getByRole('textbox',{name:new RegExp(`请在此输入或粘贴您想查询的${grain} IDs`)});
    await area.fill(grain==='Sheet'?'S1\nS1':'L1\nL1');
    await area.press('Tab');
    await text('Fixture details: 2; loads: 1');
    await text('Fixture summary: 1; loads: 1');
    await text('Fixture code: 1; loads: 1');
    await page.getByRole('button',{name:'Fixture full page reload',exact:true}).click();
    await text('Fixture summary: 2; loads: 2');
    changes.push('detail child rerun preserved parent, code sibling and source; full reload advanced source');
  }else if(report==='parts'){
    await text('Parts loads: 1');
    const input=page.locator('.st-key-parts_filter_factories').getByRole('combobox');
    await input.click();
    await page.getByRole('option',{name:'ARRAY',exact:true}).click();await page.keyboard.press('Escape');
    await text('Parts loads: 1');await text('Parts publications: 1');
    await page.getByRole('button',{name:'Fixture full page reload',exact:true}).click();
    await text('Parts loads: 2');await text('Parts publications: 2');
    changes.push('parts filter did not reload or republish');
  }else if(report==='evap'){
    await text('Evap publications: 1');
    await page.locator('.st-key-iqc_evap_products').getByRole('combobox').click();
    await page.getByRole('option',{name:/Z553/}).click();await page.keyboard.press('Escape');
    await text('Evap publications: 1');
    await page.locator('.st-key-iqc_evap_query button').click();await text('Evap queries: 1');
    await text('Evap publications: 1');
    await page.getByRole('button',{name:'Fixture full page reload',exact:true}).click();
    await text('Evap publications: 2');changes.push('manual query isolated from automatic publication');
  }else if(mode==='warning'){
    await text('Page runs: 1');
    const adminUrl = new URL(page.url());
    adminUrl.searchParams.set('admin','true');
    await page.goto(adminUrl.toString());
    await text('Page runs: 1');
    for(const label of ['CPK 筛选条件','CPM 筛选条件']){
      await page.getByTestId('stExpander').filter({hasText:label}).locator('summary').click();
    }
    await page.locator('.st-key-cpk_monitor_query_submit button').click();await text('CPK queries: 1');
    await page.locator('.st-key-cpm_monitor_query_submit button').click();await text('CPM queries: 1');
    await text('CPK queries: 1');await text('Page runs: 1');
    await page.getByRole('button',{name:'Fixture full page reload',exact:true}).click();await text('Page runs: 2');
    changes.push('CPK and CPM preserve each other and do not rerun page');
  }else if(mode==='documents'){
    await text('Page runs: 1');
    await page.getByText('📤 上传新台账/周报/解析报告/专项资料',{exact:true}).click();
    await page.getByRole('combobox',{name:'资料类别',exact:true}).click();
    await page.getByRole('option',{name:'解析报告',exact:true}).click();await text('Page runs: 1');
    await page.getByRole('button',{name:'Fixture full page reload',exact:true}).click();await text('Page runs: 2');
    changes.push('upload category isolated from document page and preview');
  }else throw Error('Unknown fixture case');
  if(await page.locator('[data-testid="stException"]').count()) throw Error('Streamlit exception visible');
  return {passed:true,case:scope||grain||report||mode,changes};
}
