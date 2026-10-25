import { chromium } from '@playwright/test';
import fs from 'node:fs';
const [base, output] = process.argv.slice(2);
if (!/^http:\/\/127\.0\.0\.1:\d+$/.test(base)) throw new Error('LOCAL_REHEARSAL_ONLY');
const browser = await chromium.launch({headless:true});
try {
  const page = await browser.newPage({viewport:{width:1440,height:900}});
  const errors = [];
  page.on('pageerror', () => errors.push('PAGE_ERROR'));
  const paths = ['/','/explore','/surges','/saved','/notifications','/categories','/reports'];
  const checks = [];
  for (const path of paths) {
    const response = await page.goto(base+path);
    await page.waitForLoadState('networkidle');
    await page.reload(); await page.waitForLoadState('networkidle');
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth > innerWidth);
    if (response.status()!==200 || overflow) throw new Error('DEPLOYED_ROUTE_FAILED');
    checks.push({path,status:response.status(),refresh:true,overflow});
  }
  await page.setViewportSize({width:375,height:812}); await page.goto(base+'/categories'); await page.waitForLoadState('networkidle');
  const mobileOverflow=await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth);
  if(mobileOverflow || errors.length)throw new Error('DEPLOYED_BROWSER_FAILED');
  const revision=await (await page.request.get(base+'/revision.json')).json();
  fs.writeFileSync(output,JSON.stringify({checks,mobileOverflow,pageErrors:errors.length,revision},null,2)+'\n',{flag:'wx'});
} finally { await browser.close(); }
