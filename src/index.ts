import { Hono } from 'hono'

type Bindings = {
  STATE: KVNamespace
  META_ACCESS_TOKEN: string
  META_AD_ACCOUNT_ID: string
  META_API_VERSION: string
  POSTHOG_API_KEY: string
  POSTHOG_PROJECT_ID: string
  POSTHOG_HOST: string
  ALLOW_ALL_EVENTS?: string
}

const app = new Hono<{ Bindings: Bindings }>()

const ALLOWED_EVENTS = new Set([
  // Budget
  'update_ad_set_budget', 'update_campaign_budget', 'update_campaign_group_spend_cap',
  'ad_account_update_spend_limit', 'ad_account_remove_spend_limit', 'ad_account_reset_spend_limit',
  // Status
  'update_ad_run_status', 'update_ad_set_run_status', 'update_campaign_run_status',
  'ad_review_declined', 'ad_review_approved',
  // Targeting
  'update_ad_set_target_spec', 'update_ad_targets_spec', 'update_audience', 'create_audience', 'delete_audience',
  // Billing
  'ad_account_billing_decline', 'ad_account_billing_charge_failed',
  // Creative
  'update_ad_creative', 'create_ad'
])

const ALLOWED_OBJECTS = new Set(['CAMPAIGN', 'AD_SET', 'AD', 'AUDIENCE'])

// Map API object types to display-friendly names
const OBJECT_TYPE_DISPLAY_MAP: Record<string, string> = {
  'CAMPAIGN': 'Campaign',
  'AD_SET': 'Ad Set', 
  'AD': 'Ad',
  'AUDIENCE': 'Audience'
}

async function syncMetaToPosthog(env: Bindings, daysBack?: number) {
  console.log('syncMetaToPosthog: start')
  const allowAll = env.ALLOW_ALL_EVENTS === 'true'

  // Validate required environment variables early
  if (!env.META_ACCESS_TOKEN || !env.META_AD_ACCOUNT_ID) {
    console.error('Missing META_ACCESS_TOKEN or META_AD_ACCOUNT_ID')
    return
  }
  
  // get last sync time from kv to prevent double sync
  const lastSyncTime = (await env.STATE.get('last_sync_time')) || '0'

  // Calculate time range if daysBack is specified
  const params = new URLSearchParams({
    fields: 'event_time,event_type,translated_event_type,object_id,object_name,object_type,actor_name,extra_data',
    limit: '100', // Increased limit for 7-day fetch
    access_token: env.META_ACCESS_TOKEN,
  })

  // Add time range parameters if daysBack is specified
  if (daysBack) {
    const now = new Date()
    const since = new Date(now.getTime() - (daysBack * 24 * 60 * 60 * 1000))
    
    // Meta API expects Unix timestamps
    params.append('since', Math.floor(since.getTime() / 1000).toString())
    params.append('until', Math.floor(now.getTime() / 1000).toString())
    
    console.log(`Fetching activities from ${since.toISOString()} to ${now.toISOString()}`)
  }

  const url = `https://graph.facebook.com/${env.META_API_VERSION}/${env.META_AD_ACCOUNT_ID}/activities?${params.toString()}`

  const resp = await fetch(url)

  if (!resp.ok) {
    console.error('Meta API Error', await resp.text())
    return
  }

  const data: any = await resp.json()

  const activities = data.data.reverse() // old to new
  console.log('syncMetaToPosthog: fetched activities', activities.length)

  let maxTime = parseInt(lastSyncTime);

  for (const activity of activities) {
    console.log(`Processing activity: ${activity.event_type} on ${activity.object_type}`)
    
    // meta uses seconds
    const eventTime = new Date(activity.event_time).getTime() / 1000
    const metaTimestamp = Date.parse(activity.event_time) / 1000

    // check if the event is new (skip this check if we're doing a historical sync)
    if (!daysBack && metaTimestamp <= parseInt(lastSyncTime)) {
      console.log(`Skipping old event: ${activity.event_type} (${metaTimestamp} <= ${lastSyncTime})`)
      continue;
    }

    if (!ALLOWED_OBJECTS.has(activity.object_type)) {
      console.log(`Skipping disallowed object type: ${activity.object_type}`)
      continue;
    }
    
    // Check if event type is allowed (unless ALLOW_ALL_EVENTS is true)
    if (!allowAll && !ALLOWED_EVENTS.has(activity.event_type)) {
      console.log(`Skipping disallowed event type: ${activity.event_type}`)
      continue;
    }

    let extraData = {}
    try {
        if (activity.extra_data) {
             extraData = JSON.parse(activity.extra_data)
        }
    } catch (e) {
        console.error('Failed to parse extra_data', activity.extra_data)
    }

    const message = await generateAnnotationMessage(activity, extraData)

    if (message) {
      // annotate to ph
      await createPostHogAnnotation(env, message, activity.event_time)
      console.log(`Sent to PostHog: ${message}`)
    }

    // Update max time seen
    if (metaTimestamp > maxTime) maxTime = metaTimestamp

  }

  // Only update last sync time if this is a regular sync (not historical)
  if (!daysBack) {
    await env.STATE.put('last_sync_time', maxTime.toString())
  }
}

async function generateAnnotationMessage(activity: any, extra: any): Promise<string | null> {
  const name = activity.object_name || "Unkown"
  const type = activity.event_type
  const displayObjectType = OBJECT_TYPE_DISPLAY_MAP[activity.object_type] || activity.object_type


  if (type.includes('budget') || type.includes('spend_cap')) {
    console.log('budget updated', extra)
    const oldVal = extra.old_value.old_value || '?'
    const newVal = extra.new_value.new_value || '?'
    return `Budget updated on ${displayObjectType}: ${name} (₱${oldVal} -> ₱${newVal})` // [cite: 95]
  }

  

  // fallback if the other things above dont work

  return `${activity.event_type} on ${name}`
}

async function createPostHogAnnotation(env: Bindings, content: string, dateIso: string) {
  const url = `${env.POSTHOG_HOST}/api/projects/${env.POSTHOG_PROJECT_ID}/annotations/`

  const resp = await fetch(url, { // <-- Added 'resp' variable
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${env.POSTHOG_API_KEY}`
    },
    body: JSON.stringify({
      content: content,
      date_created: dateIso,
      scope: 'organization'
    })
  });
  
  // 💡 NEW: Check status and log error
  if (!resp.ok) {
    console.error('PostHog API Error Status:', resp.status);
    console.error('PostHog API Error Body:', await resp.text());
  }
}

// testing
app.get('/sync', async (c) => {
  console.log('HTTP /sync invoked')
  await syncMetaToPosthog(c.env)
  return c.text('Sync process triggered')
})

// Sync activity logs for the past 7 days
app.get('/sync/7days', async (c) => {
  console.log('HTTP /sync/7days invoked')
  await syncMetaToPosthog(c.env, 7)
  return c.text('7-day sync process triggered')
})

// Generic endpoint for custom day ranges
app.get('/sync/:days', async (c) => {
  const days = parseInt(c.req.param('days'))
  if (isNaN(days) || days <= 0) {
    return c.text('Invalid days parameter. Must be a positive number.', 400)
  }
  
  console.log(`HTTP /sync/${days} invoked`)
  await syncMetaToPosthog(c.env, days)
  return c.text(`${days}-day sync process triggered`)
})

export default app
