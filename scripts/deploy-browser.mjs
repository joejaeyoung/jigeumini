import { chromium, expect } from '@playwright/test';
import fs from 'node:fs';
const [base, output] = process.argv.slice(2);
if (!/^http:\/\/127\.0\.0\.1:\d+$/.test(base)) throw new Error('LOCAL_REHEARSAL_ONLY');
const channel = 'a'.repeat(32);
const browser = await chromium.launch({headless:true});
try {
  const page = await browser.newPage({viewport:{width:1440,height:900}});
  const errors = [], responses = [], failedRequests = [];
  let injectingError = false;
  page.on('pageerror', () => errors.push('PAGE_ERROR'));
  page.on('response', response => {if (new URL(response.url()).pathname.startsWith('/api/') && !injectingError) responses.push({path:new URL(response.url()).pathname,status:response.status()});});
  page.on('requestfailed', request => {if (new URL(request.url()).pathname.startsWith('/api/') && !injectingError && !request.failure()?.errorText.includes('ERR_ABORTED')) failedRequests.push('API_REQUEST_FAILED');});
  const paths = [['/','오늘의 발견'],['/explore','방송 탐색'],['/surges','지금, 반응이 모이는 순간'],['/saved','다시 만나고 싶은 방송'],['/notifications','관심 방송 알림'],['/categories','카테고리 순위'],[`/channels/${channel}`,'수집 채널 aaaaaaaa'],[`/channels/${channel}/reports`,'방송별 요약']];
  const checks = [];
  for (const [path,title] of paths) {
    const response = await page.goto(base+path);
    await page.waitForLoadState('networkidle');
    const refreshed = await page.reload(); await page.waitForLoadState('networkidle');
    await expect(page.getByRole('heading',{name:title,exact:true})).toBeVisible();
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth > innerWidth);
    if (response.status()!==200 || refreshed.status()!==200 || overflow || response.headers()['x-content-type-options']!=='nosniff') throw new Error('DEPLOYED_ROUTE_FAILED');
    checks.push({path,status:response.status(),refresh:true,overflow,titleVisible:true,nosniff:true});
  }
  // 합성 Kafka/DB 입력에서 만들어진 실제 목록과 요약을 nginx를 통해 읽는다.
  const reportsResponse = await page.request.get(base+`/api/v1/channels/${channel}/reports`);
  if(!reportsResponse.ok())throw new Error('DEPLOYED_REPORT_API_FAILED');
  const reports = await reportsResponse.json();
  if(reports.reports.length!==1)throw new Error('DEPLOYED_REPORT_DATA_MISSING');
  await expect(page.locator('.report-list li')).toHaveCount(1);
  const detailPath='/reports/'+reports.reports[0].reportId;
  const detailResponse=await page.request.get(base+'/api/v1'+detailPath);
  if(!detailResponse.ok())throw new Error('DEPLOYED_REPORT_API_FAILED');
  const detail=await detailResponse.json();
  if(detail.referenceCount!==20)throw new Error('DEPLOYED_REPORT_DATA_DIFFERENCE');
  await page.goto(base+detailPath);await page.waitForLoadState('networkidle');
  await expect(page.getByRole('heading',{name:'방송 요약',exact:true})).toBeVisible();
  await expect(page.locator('div').filter({has:page.locator('dt',{hasText:'관측 채팅 · 참고'})}).last().locator('dd')).toContainText('20');
  const asset=await page.locator('script[src^="/assets/"]').first().getAttribute('src');
  const assetResponse=await page.request.get(base+asset), revisionResponse=await page.request.get(base+'/revision.json');
  for(const response of [assetResponse,revisionResponse])if(!response.ok()||response.headers()['x-content-type-options']!=='nosniff')throw new Error('DEPLOYED_HEADER_FAILED');
  if(!responses.length || responses.some(r=>r.status<200||r.status>=300) || failedRequests.length)throw new Error('DEPLOYED_API_FAILED');
  await page.setViewportSize({width:375,height:812});await page.goto(base+detailPath);await page.waitForLoadState('networkidle');
  const mobileOverflow=await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth);
  if(mobileOverflow || errors.length)throw new Error('DEPLOYED_BROWSER_FAILED');
  injectingError=true;
  await page.route('**/api/v1/rankings?**',route=>route.fulfill({status:503,json:{error:'API_UNAVAILABLE'}}));
  await page.goto(base+'/explore');await expect(page.getByRole('alert')).toContainText('불러오지 못했어요');
  await page.unroute('**/api/v1/rankings?**');await page.getByRole('button',{name:'다시 시도',exact:true}).click();
  await expect(page.getByRole('alert')).toHaveCount(0);
  const revision=await revisionResponse.json();
  fs.writeFileSync(output,JSON.stringify({checks,reportDetail:{path:detailPath,referenceCount:detail.referenceCount,rendered:true},apiResponses:responses,failedApiRequests:failedRequests.length,mobileOverflow,pageErrors:errors.length,apiErrorUi:true,apiRetryRecovered:true,staticHeadersVerified:true,revision},null,2)+'\n',{flag:'wx'});
} finally { await browser.close(); }
