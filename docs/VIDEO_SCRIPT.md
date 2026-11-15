# Video script: EzyShop Big Data Prototype (target 10:00, acceptable 9:00–11:00)

Speaker notes are written to be read almost verbatim at a calm pace (about 140 words per minute). Numbers in **bold** come from the committed results (`tests/bench/results/`, `docs/TEST_LOG.md`, README §6). Re-check them if you re-run the benchmarks.

---

## Pre-recording checklist

- [ ] Docker Desktop running with ≥ 8 GB RAM; **other Docker stacks stopped** (they compete for memory)
- [ ] `make up && make init && make backfill && make nightly` done **before** recording (pre-warmed; the backfill and first build are too slow to show live)
- [ ] Stream checkpoints are warm: run `make stream` + `make generate` for 2 minutes, then stop both, so the first live batch is fast
- [ ] Browser: dashboard open at http://localhost:8501, zoom 110–125%, light theme, bookmarks bar hidden
- [ ] Terminal: font ≥ 18 pt, dark-on-light or high-contrast theme, window ~120 columns; three tabs ready: `stream`, `generate`, `commands`
- [ ] `docs/architecture.png`, `tests/bench/results/throughput.png`, `tests/bench/results/latency.png` and `docs/TEST_LOG.md` open in tabs
- [ ] Title slide + problem slide + summary slide ready (any presentation tool)
- [ ] Notifications off (macOS Focus mode), Slack/Mail closed, phone silent
- [ ] Microphone level checked; record a 10-second test
- [ ] Clock visible on the dashboard: "Fresh" badge should be green before you start the demo segment

---

## 0:00–1:30 · Business problem (≈ 200 words)

**On screen:** title slide → problem slide (silos diagram, three Vs with numbers).

> Hello, I'm Yvonne Kinoti. This is my DAT801A Assessment 3 prototype, which builds on our Assessment 2 report for EzyShop.
>
> EzyShop is a mid-sized online retailer. It runs everything on one relational database, with purchases, web clicks, reviews, inventory and campaigns in five separate silos. The case study names four consequences: customer retention fell twelve percent in six months; marketing can't see current customer behaviour; the system fails at the ten thousand transactions per second of Black Friday; and the warehouse swings between overstock and shortage.
>
> That's a Big Data problem on all three Vs. Volume: one and a half terabytes a day, almost two petabytes in three years. Velocity: ten-thousand-TPS peaks, and marketing wants a dashboard no more than five minutes old. Variety: relational tables, clickstream JSON and free-text reviews, and the value lies in joining them.
>
> In Assessment 2 we proposed a Lambda architecture and recommended validating it with a small prototype. This video shows that prototype: what it does, how I tested it, and what it can and can't prove.

## 1:30–3:30 · Design (≈ 280 words)

**On screen:** `docs/architecture.png`; then README §7 justification table.

> Here is the architecture. Same five layers as Assessment 2, laptop-sized.
>
> On the left, a generator produces clicks, orders and reviews. Customer IDs are hashed with a secret salt before anything leaves the generator, so the platform only ever sees pseudonyms. Kafka is the buffer, three topics with three partitions, keyed by that pseudonym.
>
> Spark runs both paths. The speed layer, in Structured Streaming, does two things with every micro-batch: it appends the raw events to the bronze layer of a Delta Lake on object storage, and it computes one-minute windows per category, with a ten-minute watermark for late events and de-duplication on the event ID, and upserts them into MongoDB for the dashboard.
>
> The batch layer runs nightly. It cleans bronze into silver, joining orders to the product master from PostgreSQL over JDBC: that's the structured plus semi-structured integration. It scores every review with VADER sentiment: the unstructured part. Then it trains a demand forecast in Spark MLlib, flags at-risk customers, and compacts the small files streaming creates.
>
> Let me follow one Black Friday add-to-cart: generator, Kafka partition, within one thirty-second trigger it's in bronze and in the electronics window in Mongo, and the dashboard shows it on its next refresh. Overnight it becomes part of the demand history.
>
> The key trade-offs: Lambda over Kappa because the forecast needs eighteen months of history and Kafka keeps one day. Spark over Flink: one engine for SQL, streaming and ML, and the target is five minutes, not five milliseconds. Delta over plain Parquet for transactions and GDPR deletes. And VADER over a transformer: explainable and cheap, but it misreads sarcasm, as we'll see.

## 3:30–7:30 · Live demo (≈ 500 words, mostly narration over actions)

**On screen:** terminal + dashboard side by side.

| Time | Action | Say |
|---|---|---|
| 3:30 | Terminal: `docker compose ps` | "All seven services are up and healthy: Kafka, object storage, Mongo, Postgres, Spark, generator and dashboard. Everything runs in Docker; the only requirements are Docker and make." |
| 3:50 | Tab *stream*: `make stream` | "This starts the speed layer: two streaming queries, thirty-second trigger." |
| 4:05 | Tab *generate*: `make generate` | "And this is live traffic at two hundred events per second. Notice the counters for duplicates, late events and malformed messages: dirt injected on purpose to test the cleaning." |
| 4:30 | Dashboard → Marketing tab | "Within about a minute the dashboard fills. The freshness badge is green: the newest data is seconds old against a five-minute target. Revenue and orders per minute, the funnel from views to cart to orders, revenue by category, and live review sentiment." |
| 5:10 | Tab *generate*: Ctrl-C, then `make burst` | "Now Black Friday: ten times the rate, about two thousand events per second." |
| 5:20 | Dashboard (wait for one refresh) | "Orders per minute jump. The question is whether freshness holds. Kafka absorbs the burst; Spark simply takes bigger batches. The badge stays green. In the benchmark the average event was visible after **38 seconds at p95** in burst mode, and the worst event in any batch after **107 seconds**, against the three-hundred-second target." |
| 6:00 | Dashboard → Operations tab | "This comes from the nightly batch. Weekly demand for electronics: actuals, the eight-week test where the model was evaluated, and the next four weeks, including the Black Friday spike. Accuracy per category against the ninety-percent line: gradient-boosted trees reach **93.4 percent** overall, the seasonal-naive baseline **82.0 percent**; fed back on itself for eight weeks it still holds **90.5 percent**. Below, reorder alerts against the Postgres inventory." |
| 6:30 | Dashboard → Customers tab | "At-risk customers, pseudonymised IDs only, each with a reason, for example 'orders per thirty days fell a hundred percent; two negative reviews'. On the right, the lowest-sentiment products and weekly sentiment per category; you can see dips where I planted quality problems in the synthetic data." |
| 6:55 | Tab *commands*: `make erase CUSTOMER=C00042` | "Finally GDPR. I erase customer C00042: the ID is hashed with the salt, and rows are deleted from every Delta table, Mongo and Postgres. Before and after counts: **476 rows → 0**. Note the last column: the rows are still readable through Delta time travel. That's why VACUUM must run within the retention window; deleting is not the same as physically erasing. The pseudonym also goes on a suppression list so new events are dropped at ingestion." |

## 7:30–8:45 · Testing (≈ 180 words)

**On screen:** `throughput.png` → `latency.png` → `TEST_LOG.md` (scroll to T05, T07, T10).

> How do I know it works? Twenty-nine unit tests, an end-to-end smoke test and a GDPR test all pass.
>
> Throughput: I drained a fixed Kafka backlog with one, three and six cores. The limiting stage, writing raw events to the Delta lake, went from **3,842** to **5,317** events per second from one to three cores. Six cores ran out of memory on this shared laptop, and the live aggregation actually got slower with more cores: the Spark profile shows tasks waiting, not computing. So the honest conclusion: a laptop cannot reach ten thousand TPS, and I'm not claiming it does. The per-core rate, about 1,800 events per second, is the building block; the README extrapolates to roughly twelve cores for ten thousand events per second.
>
> Latency: p95 for the average event was **28 seconds** at normal load and **38 seconds** in burst mode, about eight times inside the five-minute target.
>
> Recovery: I killed Spark in the middle of a batch. It had written clicks and orders to bronze but not reviews. After the restart Delta skipped the two tables already committed and wrote only reviews; Kafka, bronze and Mongo counts matched exactly.
>
> And the test log has fifteen real issues: MinIO images that are no longer pullable, the Spark JVM being killed for memory during the benchmarks until I bounded the batch size, and at-risk precision of only thirty-six percent until I tuned the thresholds. One optimisation, lowering shuffle partitions from 200 to 6, made the stream almost six times faster.

## 8:45–10:00 · Value, limitations, improvements (≈ 170 words)

**On screen:** summary slide: three columns *What it proves / What it can't prove / Next steps*.

> What does EzyShop gain? A buffer that turns Black Friday into bigger batches instead of an outage; a marketing view that's seconds old instead of a day; a forecast that beats last year's numbers by about eleven points; and a churn list with reasons, all on pseudonymised data with tested erasure.
>
> What can't the prototype prove? Ten thousand TPS: it's a single broker on a laptop. Real forecast accuracy: the data is synthetic and contains the patterns the model looks for. And the plus-twenty-five-percent personalisation target needs real users and an A/B test.
>
> Next steps: deploy on managed Kafka and Databricks and load-test at full scale; make the nightly job incremental with MERGE; add ALS recommendations with an A/B test for the personalisation target; and put orchestration, a schema registry, CI/CD and infrastructure as code around it. Thank you.

---

**Timing guide:** if you run long, shorten the design walkthrough (keep the add-to-cart example) and skip the Customers tab sentiment chart. If short, show `make test` running (≈ 20 s).
