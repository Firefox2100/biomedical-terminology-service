import { defineConfig } from 'vitest/config'

export default defineConfig({
  root: new URL('..', import.meta.url).pathname,
  resolve: {
    alias: {
      '@server-js': new URL('../src/bioterms/data/static/js', import.meta.url).pathname,
    },
  },
  test: {
    environment: 'jsdom',
    include: ['term-browser/tests/**/*.test.{js,jsx}'],
    setupFiles: ['term-browser/tests/setup.js'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov'],
      reportsDirectory: 'term-browser/coverage',
      include: [
        'src/bioterms/data/static/js/api_keys.js',
        'src/bioterms/data/static/js/vocabularies.js',
        'term-browser/src/**/*.{js,jsx}',
      ],
      exclude: ['term-browser/src/main.jsx'],
      thresholds: {
        statements: 80,
        lines: 80,
        functions: 80,
        branches: 70,
      },
    },
  },
})
