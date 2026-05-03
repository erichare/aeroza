/**
 * Curated catalog of major US weather events for the /demo Storm Replay
 * page. Each entry is hand-written commentary plus the metadata needed
 * to drive the radar replay (time window, bbox).
 *
 * MRMS — the radar product that powers replay — became operational in
 * 2014. Events before that are kept here for context (April 27 2011 is
 * one of the most consequential US tornado outbreaks in modern history)
 * but flagged `replayable: false`. The page renders the commentary
 * card without the radar for those, which is honest about what we can
 * and can't show.
 *
 * Adding an event:
 * 1. Pick a real, verifiable, dramatic event with a recognisable name.
 * 2. Bracket its peak with start/end (UTC); 4–6 hours plays well.
 * 3. Bbox should frame the action, not the whole CONUS — tighter is
 *    more dramatic.
 * 4. Commentary should explain *what made it noteworthy*, not just
 *    "there was weather". Casualty / impact figures from NWS or NCEI
 *    where possible.
 */

export interface FeaturedEvent {
  /** Stable id used for URL slugs and the picker key. */
  id: string;
  /** Headline name as it should appear in the picker. */
  name: string;
  /** Human-readable date / time framing for the badge. */
  date: string;
  /** Geographic location summary ("Southeast US"). */
  location: string;
  /**
   * Inclusive UTC start of the replay window. Bracket the peak — too
   * narrow and the storm is already mid-track when playback opens; too
   * wide and the user sits through dead air.
   */
  startUtc: string;
  /** Exclusive UTC end of the replay window. */
  endUtc: string;
  /**
   * Map viewport [west, south, east, north]. Tighter than CONUS so the
   * camera frames the storm.
   */
  bbox: [number, number, number, number];
  /**
   * False when the event predates MRMS (~2014) or is otherwise outside
   * the archive's coverage. The page renders commentary only — the
   * radar is suppressed with a clear "pre-MRMS" badge.
   */
  replayable: boolean;
  /** One-line summary for the picker tile. */
  summary: string;
  /** Multi-paragraph commentary shown alongside the map. */
  commentary: string[];
  /** Optional reference URL (Wikipedia, NWS, etc.) for "read more". */
  reference?: { label: string; url: string };
}

export const FEATURED_EVENTS: ReadonlyArray<FeaturedEvent> = [
  {
    id: "houston-derecho-2024",
    name: "Houston Derecho",
    date: "May 16, 2024",
    location: "Southeast Texas",
    startUtc: "2024-05-16T22:00:00Z",
    endUtc: "2024-05-17T02:30:00Z",
    bbox: [-97.5, 28.5, -94.0, 31.0],
    replayable: true,
    summary:
      "A long-track derecho with hurricane-force gusts tore across Houston, killing 8 and leaving over 900,000 without power.",
    commentary: [
      "On the evening of May 16, 2024 a derecho — a fast-moving line of severe thunderstorms with widespread damaging winds — crossed Southeast Texas. Houston, the fourth-largest city in the US, took the worst of it. NWS Houston measured a peak gust of 100 mph at the Hobby Airport surface station, with several other CWAs reporting 80+ mph. The storm killed 8 and left more than 900,000 customers without power, some for over a week.",
      "What you'll see in the replay: a tight, bowing line of intense (>55 dBZ) reflectivity sweeping west to east across the bbox. Watch the convex bow at the leading edge — that bow's where the strongest straight-line winds were occurring. The MRMS composite makes the rear-inflow notch (the dark wedge behind the line) clearly visible.",
      "Notable: this was a *non-tornadic* event, but its straight-line wind damage was comparable in cost to many landfalling hurricanes. Derechos are an under-appreciated category of severe weather precisely because they don't usually produce the iconic tornado funnel.",
    ],
    reference: {
      label: "NWS Houston event summary",
      url: "https://www.weather.gov/hgx/2024May16Derecho",
    },
  },
  {
    id: "rolling-fork-tornado-2023",
    name: "Rolling Fork Outbreak",
    date: "March 24–25, 2023",
    location: "Mississippi & Alabama",
    startUtc: "2023-03-25T00:00:00Z",
    endUtc: "2023-03-25T06:00:00Z",
    bbox: [-92.5, 31.0, -86.5, 35.5],
    replayable: true,
    summary:
      "A long-track EF-4 tornado obliterated Rolling Fork, MS — part of an outbreak that produced 24 confirmed tornadoes across the Deep South.",
    commentary: [
      "Late on March 24, 2023, a discrete supercell formed in the Mississippi Delta and produced a long-track EF-4 tornado that struck the small town of Rolling Fork. The tornado was on the ground for 59 miles with peak winds estimated at 195 mph; it killed 17 in Rolling Fork alone and obliterated much of the town's structure within a few minutes. The broader outbreak produced 24 confirmed tornadoes across MS, AL, GA, and TN.",
      "What you'll see in the replay: a discrete supercell signature — a single, isolated, intense reflectivity core moving northeast across the bbox. The classic 'hook echo' (a comma-shaped extension on the south side of the storm) is visible in higher-resolution products; in the MRMS composite at this zoom the storm reads as a small, intense, persistent cell.",
      "Notable: the storm's discrete (non-line) structure is what allowed it to produce such a long, violent track. Discrete supercells in moisture-rich environments are the most prolific producers of strong-to-violent tornadoes.",
    ],
    reference: {
      label: "NWS Jackson MS storm summary",
      url: "https://www.weather.gov/jan/2023-03-24-25-Tornadoes",
    },
  },
  {
    id: "mayfield-quad-state-2021",
    name: "Mayfield Quad-State Tornado",
    date: "December 10–11, 2021",
    location: "AR / MO / TN / KY",
    startUtc: "2021-12-11T00:00:00Z",
    endUtc: "2021-12-11T08:00:00Z",
    bbox: [-92.0, 34.5, -85.5, 38.5],
    replayable: true,
    summary:
      "An exceptionally long-track tornado carved a 165-mile path through four states on a December evening — well outside traditional tornado season.",
    commentary: [
      "The night of December 10–11, 2021 produced the longest-track tornado in US history east of the Rockies. A single supercell produced a tornado (or possibly a tight family of tornadoes — debate is ongoing) that travelled approximately 165 miles from northeast Arkansas, across the Missouri Bootheel and northwest Tennessee, and devastated Mayfield, KY. Total deaths from the outbreak: 89, with 57 in Kentucky alone.",
      "What you'll see in the replay: an unusually persistent, intense reflectivity core moving rapidly northeast across the bbox. The track length is what makes this storm visually distinctive — most supercells dissipate or cycle within an hour, but this one held its structure across four states.",
      "Notable: December tornadoes are rare. The atmospheric setup that produced this event — strong jet stream over an unseasonably warm, moist air mass — is the kind of regime that climate change is making marginally more common. The cold-season severe-weather threat in the Mid-South is no longer purely theoretical.",
    ],
    reference: {
      label: "NWS Paducah event page",
      url: "https://www.weather.gov/pah/December-10th-11th-2021-Tornado",
    },
  },
  {
    id: "super-outbreak-2011",
    name: "Super Outbreak",
    date: "April 25–28, 2011",
    location: "Southeast US",
    startUtc: "2011-04-27T18:00:00Z",
    endUtc: "2011-04-28T02:00:00Z",
    bbox: [-91.0, 30.5, -83.0, 36.0],
    replayable: false,
    summary:
      "The most prolific tornado outbreak in US history: 360+ tornadoes over four days, including 15 EF-4s and four EF-5s. 324 dead.",
    commentary: [
      "The April 2011 Super Outbreak is, by most measures, the most consequential severe-weather event in modern US history. Across April 25–28, 360 tornadoes were confirmed across 21 states, with the bulk of the activity on April 27. That single day produced 216 tornadoes — the most ever recorded in a 24-hour period — including 15 EF-4s and four EF-5s. Total deaths from the outbreak: 324, with 238 in Alabama alone.",
      "Why we can't replay this one: the MRMS composite product became operational in late 2014 and the archive doesn't reach back to 2011. The original WSR-88D Level II radar data exists in the NCEI archive (and is what most analyses use), but the per-cycle gridded composite product we power /map with simply didn't exist yet. We've left this event in the catalog because it's important context for anyone working in severe-weather data — and because honest \"we can't show this\" beats invented graphics.",
      "If you want to see what this looked like: the National Weather Service's Birmingham office has an excellent post-event analysis with reconstructed Level II loops, NWS warning timelines, and damage surveys. The Tuscaloosa–Birmingham EF-4 (~80 miles long, 1.5 miles wide at peak) is the most-studied tornado of the modern radar era.",
    ],
    reference: {
      label: "NWS Birmingham — April 27 2011 Tornadoes",
      url: "https://www.weather.gov/bmx/event_04272011",
    },
  },
];

export function findFeaturedEvent(id: string): FeaturedEvent | undefined {
  return FEATURED_EVENTS.find((e) => e.id === id);
}
