import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { App } from './App'
import './styles.css'

const root = document.getElementById('root')

if (root === null) {
  throw new Error('HarnessGraph UI root element is unavailable')
}

createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
