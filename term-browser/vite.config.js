import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    react()
  ],
  base: "/static/term-browser/",
  build: {
    outDir: '../src/bioterms/data/static/term-browser',
    emptyOutDir: true,
  }
})
