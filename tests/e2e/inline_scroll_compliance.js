async page => {
  const kind = new URL(page.url()).searchParams.get('kind');
  const evidence = { kind, expectedCharts: 18, wheel: [] };
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.getByText('Fixture charts ready', { exact: true }).waitFor();
  // Expand the actual alert parent, leaving its normal nested renderer intact.
  for (let count = 0; count < 30; count++) {
    const closed = page.locator('details:not([open]) > summary');
    if (!(await closed.count())) break;
    await closed.first().click();
  }
  await page.waitForFunction(() => {
    const charts = [...document.querySelectorAll('.js-plotly-plot')];
    return charts.length === 18 && charts.every(chart => chart._fullLayout && chart.clientWidth > 0);
  });
  const charts = page.locator('.js-plotly-plot');
  evidence.charts = await charts.count();
  const configs = await charts.evaluateAll(elements => elements.map(chart => chart._context.scrollZoom));
  if (configs.some(value => value !== false)) throw new Error(`Unexpected scrollZoom configuration: ${configs}`);
  evidence.scrollZoomDisabledForAll = true;
  const position = () => page.evaluate(() => ({
    document: document.scrollingElement.scrollTop,
    main: document.querySelector('[data-testid="stMain"]')?.scrollTop || 0,
  }));
  const ranges = chart => chart.evaluate(element => Object.fromEntries(
    Object.keys(element._fullLayout).filter(key => /^[xy]axis\d*$/.test(key))
      .map(key => [key, element._fullLayout[key].range]),
  ));
  const sameRanges = (left, right) => JSON.stringify(Object.keys(left)) === JSON.stringify(Object.keys(right))
    && Object.keys(left).every(key => left[key].length === right[key].length
      && left[key].every((value, index) => typeof value === 'number'
        ? Math.abs(value - right[key][index]) <= 1e-7 * Math.max(1, Math.abs(value))
        : value === right[key][index]));

  await page.evaluate(() => {
    window.__fixtureWheelEvents = [];
    window.addEventListener('wheel', event => window.__fixtureWheelEvents.push({
      x: event.clientX, y: event.clientY, deltaY: event.deltaY,
    }), { passive: true });
  });
  const wheelCheck = async (index, deltaY, viewport) => {
    const chart = charts.nth(index);
    await chart.scrollIntoViewIfNeeded();
    const box = await chart.boundingBox();
    const before = await position();
    const axes = await ranges(chart);
    await page.mouse.move(box.x + box.width * .5, box.y + box.height * .5);
    await page.mouse.wheel(0, deltaY);
    await page.waitForFunction(({ previous, direction }) => {
      const now = [document.scrollingElement.scrollTop,
        document.querySelector('[data-testid="stMain"]')?.scrollTop || 0];
      return direction > 0 ? now.some((value, i) => value > previous[i] + 5)
        : now.some((value, i) => value < previous[i] - 5);
    }, { previous: [before.document, before.main], direction: deltaY });
    const after = await position();
    if (!sameRanges(axes, await ranges(chart))) throw new Error(`Wheel changed chart axes at index ${index}`);
    evidence.wheel.push({ index, viewport, deltaY, before, after });
  };
  // Ordinary and alert renderers each supply period/Lot-or-chamber/Sheet-or-time slots.
  for (const index of [0, 1, 2, 9, 10, 11]) await wheelCheck(index, 450, 'desktop');
  await wheelCheck(17, -650, 'desktop');

  const first = charts.first();
  await first.scrollIntoViewIfNeeded();
  await first.hover();
  const initial = await ranges(first);
  const buttons = await first.locator('.modebar-btn').evaluateAll(elements => elements.map(element => element.getAttribute('data-title')));
  const zoomTitle = buttons.find(title => /^(Zoom in|放大)$/i.test(title));
  const resetTitle = buttons.find(title => /^(Reset axes|重置坐标轴|重置坐标)$/i.test(title));
  if (!zoomTitle || !resetTitle) throw new Error(`Missing toolbar tools: ${buttons}`);
  await first.locator(`.modebar-btn[data-title="${zoomTitle}"]`).click();
  const zoomed = await ranges(first);
  if (sameRanges(initial, zoomed)) throw new Error('Toolbar zoom did not change axes');
  await first.locator(`.modebar-btn[data-title="${resetTitle}"]`).click();
  if (!sameRanges(initial, await ranges(first))) throw new Error('Toolbar reset did not restore axes');
  evidence.toolbar = { zoom: zoomTitle, reset: resetTitle, verified: true };

  await page.setViewportSize({ width: 560, height: 900 });
  await page.waitForFunction(() => [...document.querySelectorAll('.js-plotly-plot')]
    .every(chart => chart._fullLayout.width <= window.innerWidth));
  await wheelCheck(0, 450, 'narrow');
  await wheelCheck(17, -650, 'narrow');
  const layout = await page.evaluate(() => ({
    width: innerWidth, documentWidth: document.documentElement.scrollWidth,
    documentHeight: document.scrollingElement.scrollHeight,
    mainHeight: document.querySelector('[data-testid="stMain"]')?.scrollHeight || 0,
    viewportHeight: innerHeight,
  }));
  if (layout.documentWidth > layout.width + 3) throw new Error(`Narrow horizontal overflow: ${JSON.stringify(layout)}`);
  if (Math.max(layout.documentHeight, layout.mainHeight) <= layout.viewportHeight + 500) throw new Error('Fixture is not a long report');
  evidence.layout = layout;
  evidence.pointerEvents = await page.evaluate(() => window.__fixtureWheelEvents);
  if (evidence.pointerEvents.some(event => !event.x && !event.y)) throw new Error('Wheel event landed at (0,0)');
  await page.screenshot({ path: `scroll-${kind}-narrow.png` });
  return { passed: true, ...evidence, scope: 'actual renderers with deterministic points; no production input or service' };
}
