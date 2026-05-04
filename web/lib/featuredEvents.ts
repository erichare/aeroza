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
 *    Severe convection (tornado outbreaks, derechos), winter cyclones
 *    (blizzards, ice storms), and tropical systems (hurricanes,
 *    landfalling TS) all read well — MRMS reflectivity composites
 *    show winter precipitation bands and tropical rainshields too,
 *    not just summer convective cores.
 * 2. Bracket its peak with start/end (UTC); 4–6 hours plays well for
 *    convective events, 12–24 hours for slower-evolving winter
 *    cyclones.
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
  /**
   * NWS Weather Forecast Office 3-letter codes that cover the event's
   * bbox. Used by /demo to query the IEM historical-warnings archive
   * (`GET /v1/alerts/historical?wfos=...`). Pick the WFOs whose CWA
   * (county warning area) overlaps the bbox — IEM filters strictly by
   * issuing office, so missing one means missing every warning that
   * office issued. https://www.weather.gov/srh/nwsoffices for the map.
   *
   * Empty array = skip the historical-alerts overlay (e.g. for
   * pre-MRMS events where the radar replay isn't available either).
   */
  wfos: ReadonlyArray<string>;
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
    wfos: ["HGX", "LCH"],
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
    id: "midwest-blizzard-2024",
    name: "Mid-January Plains Blizzard",
    date: "January 12–13, 2024",
    location: "Plains & Upper Midwest",
    startUtc: "2024-01-12T18:00:00Z",
    endUtc: "2024-01-13T18:00:00Z",
    bbox: [-100.0, 38.5, -86.0, 46.0],
    replayable: true,
    wfos: ["DMX", "OAX", "MPX", "ARX"],
    summary:
      "A fast-moving cyclone dragged Arctic air across the Plains on NFL Wild Card weekend; wind chills hit -50°F and the storm-related death toll exceeded 80.",
    commentary: [
      "On January 12–13, 2024 a low-pressure system intensified rapidly across the central US, dragging Arctic air south behind it. Blizzard Warnings covered most of Iowa, Nebraska, and surrounding states. Wind chills plunged to -50°F across the northern Plains. The storm forced the NFL to postpone the Buffalo–Pittsburgh playoff game by a day — a rare logistical surrender. National death toll attributed to the storm and the Arctic air mass that followed: 80+ across multiple states.",
      "What you'll see in the replay: a classic comma-shaped precipitation field with the dry slot wrapping in from the southwest. The northwestern half is snow with embedded heavier bands; the southeastern flank shows lift on the warm side that produced ice-storm conditions across parts of Tennessee. MRMS reflectivity reads snow at lower dBZ values than rain — the greens here are intense snowfall rates, not light drizzle.",
      "Notable: wind chills, not snowfall totals, were the deadliest hazard. The storm itself was a textbook mid-latitude cyclone; what made it lethal was the Arctic air mass it pulled south once the precipitation ended. NWS messaging shifted mid-event from 'be prepared to travel after the storm' to 'do not expose any skin' — a useful case study in cascading hazards.",
    ],
    reference: {
      label: "NWS Des Moines event summary",
      url: "https://www.weather.gov/dmx/January_12_13_2024_Blizzard",
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
    wfos: ["JAN", "MEG", "BMX", "MOB"],
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
    id: "buffalo-blizzard-2022",
    name: "Buffalo Christmas Blizzard",
    date: "December 23–25, 2022",
    location: "Western New York",
    startUtc: "2022-12-23T12:00:00Z",
    endUtc: "2022-12-24T18:00:00Z",
    bbox: [-80.0, 42.0, -77.0, 44.0],
    replayable: true,
    wfos: ["BUF"],
    summary:
      "A 967-mb bomb cyclone whipped lake-effect snow into a multi-day blizzard that buried Buffalo under 50+ inches and killed 47 in Erie County alone.",
    commentary: [
      "Late on December 23, 2022, an exceptionally deep extratropical cyclone (central pressure ≈967 mb — a textbook 'bomb cyclone') combined with an Arctic air mass to deliver a multi-day lake-effect blizzard to Western New York. Buffalo recorded over 50 inches of snow with sustained winds of 65+ mph and visibility near zero for more than 36 hours. The storm killed 47 people in Erie County (many trapped in cars or homes that lost heat) and contributed to over 100 deaths nationwide.",
      "What you'll see in the replay: long, organised lake-effect snow bands extending east-southeast off Lake Erie across WNY. The bands' intensity (greens / yellows on the dBZ ramp here represent very heavy snow rates, not rain) and persistence are the signatures of a fully-tapped lake-effect setup. Watch the eastern terminus of the band — that's where Buffalo sat for ~36 hours while the band refused to migrate.",
      "Notable: lake-effect snow is hard to forecast precisely because the band's exact position depends on wind direction within a few degrees. NWS Buffalo nailed the macro forecast (a major lake-effect event) days in advance; the operational challenge was convincing the public that *this* lake-effect event was different — fatal — instead of the usual seasonal nuisance.",
    ],
    reference: {
      label: "NWS Buffalo storm summary",
      url: "https://www.weather.gov/buf/Blizzard122322",
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
    wfos: ["PAH", "LZK", "MEG", "LMK"],
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
    id: "winter-storm-uri-2021",
    name: "Winter Storm Uri",
    date: "February 13–17, 2021",
    location: "Texas & Southern Plains",
    startUtc: "2021-02-15T00:00:00Z",
    endUtc: "2021-02-15T18:00:00Z",
    bbox: [-103.0, 26.0, -93.0, 36.5],
    replayable: true,
    wfos: ["FWD", "HGX", "EWX", "LUB", "MAF"],
    summary:
      "Arctic air drove temperatures below 0°F across Texas, triggering the cascading ERCOT grid failure. 246 confirmed deaths.",
    commentary: [
      "From February 13–17, 2021, an extraordinarily deep cold-air outbreak pushed Arctic temperatures all the way to the Gulf of Mexico. Lows fell below 0°F as far south as San Angelo and Dallas–Fort Worth — territory unaccustomed to such cold and infrastructure-wise unprepared for it. The Texas grid (ERCOT) collapsed in cascading failures starting Feb 15, leaving over 4.5 million customers without power for days during the coldest stretch. The state's official death toll was revised to 246; independent estimates ran substantially higher.",
      "What you'll see in the replay: a vast precipitation shield draped over Texas and the southern Plains, with dBZ values consistent with mixed snow / sleet / freezing rain — not the towering convective cores you'd expect over Texas. The visual story is breadth, not intensity: the storm's footprint covered 200+ million people.",
      "Notable: this event is the canonical example of weather-driven infrastructure failure in the modern US. Subsequent ERCOT post-mortems and federal reports made clear that the grid wasn't winterised because the design assumption was 'this kind of cold doesn't happen here.' The forecast was excellent days in advance; the systemic preparation was not.",
    ],
    reference: {
      label: "NWS Fort Worth event page",
      url: "https://www.weather.gov/fwd/uri",
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
    // Pre-MRMS — radar replay disabled. WFOs left populated so the
    // historical-alerts overlay still works if a future enhancement
    // shows the warning timeline alongside the commentary card.
    wfos: ["BMX", "HUN", "MEG", "JAN"],
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
