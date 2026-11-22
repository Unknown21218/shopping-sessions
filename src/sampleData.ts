import type { PipelineInput } from './pipeline.ts';

export const SAMPLE_DATASET: PipelineInput = {
  manifest: JSON.stringify(
    {
      window_start: '2026-05-01T00:00:00Z',
      window_end: '2026-05-04T00:00:00Z',
    },
    null,
    2
  ),
  productsCsv: [
    'product_id,category',
    'prod_laptop_1,electronics',
    'prod_headphones_2,electronics',
    'prod_sneakers_3,footwear',
    'prod_jacket_4,apparel',
    'prod_mug_5,home',
  ].join('\n'),
  campaignsCsv: [
    'campaign_id,channel',
    'camp_spring_search,paid_search',
    'camp_social_retarget,paid_social',
    'camp_vip_email,email',
  ].join('\n'),
  linksCsvs: [
    {
      name: 'links_01.csv',
      content: [
        'user_a,user_b',
        'u_alice_mobile,u_alice_web',
        'u_alice_web,u_alice_tablet',
        'u_bob_guest,u_bob_member',
      ].join('\n'),
    },
  ],
  clicksCsvs: [
    {
      name: 'clicks_01.csv',
      content: [
        'click_id,user_id,campaign_id,timestamp',
        'ck_101,u_alice_mobile,camp_spring_search,2026-04-29T14:00:00Z',
        'ck_102,u_alice_web,camp_social_retarget,2026-05-01T09:50:00Z',
        'ck_103,u_bob_guest,camp_vip_email,2026-05-02T08:00:00Z',
        'ck_104,u_carol,camp_spring_search,2026-04-15T10:00:00Z',
      ].join('\n'),
    },
  ],
  eventsCsvs: [
    {
      name: 'events_01.csv',
      content: [
        'event_id,user_id,timestamp,ingest_time,event_type,product_id,price_cents',
        // Alice session 1 (2026-05-01 10:00 - 10:15): credited to camp_social_retarget (latest <= start), assisted by both search & social
        'ev_001,u_alice_tablet,2026-05-01T10:00:00Z,2026-05-01T10:00:05Z,view,prod_laptop_1,0',
        'ev_002,u_alice_web,2026-05-01T10:08:00Z,2026-05-01T10:08:03Z,cart,prod_laptop_1,0',
        'ev_003,u_alice_web,2026-05-01T10:15:00Z,2026-05-01T10:15:02Z,purchase,prod_laptop_1,129900',
        // Duplicate ev_003 with later ingest_time and different price -> must be deduped keeping earliest ingest_time!
        'ev_003,u_alice_web,2026-05-01T10:15:00Z,2026-05-01T10:20:00Z,purchase,prod_laptop_1,999999',
        // Alice session 2 (>30m gap at 14:00): purchase in footwear
        'ev_004,u_alice_mobile,2026-05-01T14:00:00Z,2026-05-01T14:00:10Z,purchase,prod_sneakers_3,14500',
        // Bob session on 2026-05-02 (linked u_bob_guest -> u_bob_member, canonical u_bob_guest): credited to camp_vip_email
        'ev_005,u_bob_member,2026-05-02T11:30:00Z,2026-05-02T11:30:04Z,view,prod_jacket_4,0',
        'ev_006,u_bob_member,2026-05-02T11:42:00Z,2026-05-02T11:42:01Z,purchase,prod_jacket_4,18900',
        // Carol session on 2026-05-03 (click ck_104 was >7 days ago -> organic)
        'ev_007,u_carol,2026-05-03T16:00:00Z,2026-05-03T16:00:02Z,view,prod_mug_5,0',
        'ev_008,u_carol,2026-05-03T16:05:00Z,2026-05-03T16:05:02Z,purchase,prod_unknown_99,2400',
        // Late-arriving event (>24h ingest lag) -> dropped
        'ev_009,u_carol,2026-05-01T08:00:00Z,2026-05-03T09:00:00Z,view,prod_mug_5,0',
      ].join('\n'),
    },
  ],
};
