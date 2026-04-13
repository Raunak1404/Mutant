import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  base: '/static/',
  build: {
    outDir: '../static',
    emptyOutDir: true,
  },
  server: {
    port: 3000,
    proxy: {
      '/jobs': {
        target: 'http://localhost:8000',
        ws: true,
      },
      '/chat': {
        target: 'http://localhost:8000',
      },
    },
  },
})
