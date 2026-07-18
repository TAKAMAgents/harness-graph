import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig(({ mode }) => {
  const environment = loadEnv(mode, process.cwd(), '')
  const apiTarget = environment.VITE_API_PROXY_TARGET ?? 'http://127.0.0.1:3000'

  return {
    plugins: [react(), tailwindcss()],
    server: {
      port: 4173,
      strictPort: true,
      proxy: {
        '/v1': apiTarget,
        '/health': apiTarget,
      },
    },
    preview: {
      port: 4173,
      strictPort: true,
    },
  }
})
