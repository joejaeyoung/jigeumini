import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
const proxy = { '/api': { target: process.env.API_PROXY_TARGET || 'http://127.0.0.1:18080', changeOrigin: false } };
export default defineConfig({
  plugins: [react()],
  server: { port: 18082, strictPort: true, proxy },
  preview: { port: 18083, strictPort: true, proxy },
});
