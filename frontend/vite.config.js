import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';

// Minimal Vite config — just the Vue SFC plugin. No routing, no proxy: the
// dashboard talks straight to the deployed Worker (CORS is open on that side).
export default defineConfig({
  plugins: [vue()],
});
