import { defineConfig } from '@playwright/test';
export default defineConfig({
  testDir: './tests', timeout: 30000, fullyParallel: true, workers: 2,
  forbidOnly: Boolean(process.env.CI), retries: process.env.CI ? 1 : 0,
  use: { baseURL: 'http://127.0.0.1:18084', trace: 'retain-on-failure' },
  webServer: { command: 'npm run dev -- --port 18084', url: 'http://127.0.0.1:18084', reuseExistingServer: !process.env.CI },
});
