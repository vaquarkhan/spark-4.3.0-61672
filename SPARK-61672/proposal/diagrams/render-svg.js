const path = require('path');
const puppeteer = require('puppeteer');
(async () => {
  const browser = await puppeteer.launch({
    headless: true,
    executablePath: 'C:\\spark-build\\.cache\\puppeteer\\chrome\\win64-150.0.7871.24\\chrome-win64\\chrome.exe',
    args: ['--no-sandbox']
  });
  const page = await browser.newPage();
  await page.setViewport({width: 1600, height: 1000, deviceScaleFactor: 1});
  const url = 'file://' + path.resolve(__dirname, 'provider-neutral-ai-functions.svg').replace(/\\/g, '/');
  await page.goto(url, {waitUntil: 'load'});
  await page.screenshot({path: path.resolve(__dirname, 'provider-neutral-ai-functions.png')});
  await browser.close();
})();
