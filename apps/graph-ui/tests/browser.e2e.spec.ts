import { expect, test } from '@playwright/test'

test('completed enrichment renders semantic knowledge, provenance, and resolvable citations', async ({ page }) => {
  await page.goto('/')

  await expect(
    page.getByRole('heading', { name: 'Turning verified execution into reusable graph knowledge' }),
  ).toBeVisible()
  await expect(page.getByText('Mistral semantic overlay', { exact: true })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Episodes' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Entities' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Claims' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Relations' })).toBeVisible()
  await expect(page.getByText('mistral-small-2603', { exact: true })).toBeVisible()
  await expect(page.getByText('Conversation And Execution', { exact: true })).toBeVisible()
  await expect(page.getByText('e'.repeat(64), { exact: true })).toBeVisible()
  await expect(page.getByText('f'.repeat(64), { exact: true })).toBeVisible()
  await expect(page.getByText('High confidence', { exact: true }).first()).toBeVisible()
  await expect(page.getByText('Explicit', { exact: true }).first()).toBeVisible()
  await expect(page.getByText('0 activities', { exact: true })).toBeVisible()

  await page.getByRole('link', { name: 'Go to source citation 1' }).first().click()
  await expect(page).toHaveURL(new RegExp(`#source-${'a'.repeat(64)}$`))
  await expect(page.getByRole('heading', { name: 'Architecture decision evidence' })).toBeVisible()
})

test('no completed run uses the deterministic display without enrichment claims', async ({ page }) => {
  await page.goto('/')
  await page.getByRole('button', { name: /Inspect → modify → verify/ }).click()

  await expect(page.getByRole('heading', { name: 'Inspect → modify → verify' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Deterministic view is active' })).toBeVisible()
  await expect(page.getByText('Complete without fresh verification', { exact: true })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Claims' })).toHaveCount(0)
  await expect(page.getByText('Mistral semantic overlay', { exact: true })).toHaveCount(0)
})

test('the experience contract excludes forbidden internal and sensitive fields', async ({ request }) => {
  const list = await request.get('/v1/experience/sessions')
  const detail = await request.get('/v1/experience/sessions/ses_enriched_e2e')
  expect(list.ok()).toBe(true)
  expect(detail.ok()).toBe(true)

  const serialized = `${await list.text()}${await detail.text()}`
  expect(serialized).not.toContain('"key"')
  expect(serialized).not.toContain('raw_transcript')
  expect(serialized).not.toContain('transcript_text')
  expect(serialized).not.toContain('field_path')
  expect(serialized).not.toContain('/Users/')
  expect(serialized).not.toContain('MISTRAL_API_KEY')
})

test('a response carrying an internal graph field fails closed without rendering it', async ({ page }) => {
  await page.goto('/?contract_violation=1')

  await expect(page.getByRole('heading', { name: 'Experience view unavailable' })).toBeVisible()
  await expect(page.getByText('The session list violated the source-safe API contract.')).toBeVisible()
  await expect(page.getByText('internal-graph-identity')).toHaveCount(0)
})

test('keyboard and mobile users can navigate the semantic session surface', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 })
  await page.goto('/')

  await page.keyboard.press('Tab')
  await expect(page.getByRole('link', { name: 'Skip to session details' })).toBeFocused()
  await page.keyboard.press('Enter')
  await expect(page.locator('#main-content')).toBeFocused()
  await expect(page.getByRole('navigation', { name: 'Session results' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Episodes' })).toBeVisible()
})
