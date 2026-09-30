# commerce-production-v1 — ci profile

Run: 2026-09-30T13:14:17.765Z → 2026-09-30T13:15:13.297Z. Engines: mysql, pintail. Scale: 0.01.

## Phase: cold

| Query | Engine | Status | Median ms | p95 ms |
|---|---|---|---:|---:|
| q01-tenant-revenue | mysql | ok | 29.0 | 92.5 |
| q01-tenant-revenue | pintail | ok | 2.9 | 28.2 |
| q02-customer-history | mysql | ok | 0.3 | 1.1 |
| q02-customer-history | pintail | ok | 3.6 | 7.6 |
| q03-fulfillment-backlog | mysql | ok | 0.4 | 1.1 |
| q03-fulfillment-backlog | pintail | ok | 2.1 | 6.9 |
| q04-inventory-risk | mysql | ok | 0.9 | 0.9 |
| q04-inventory-risk | pintail | ok | 2.2 | 2.2 |
| q05-payment-failures | mysql | ok | 52.4 | 65.7 |
| q05-payment-failures | pintail | ok | 3.0 | 155.9 |
| q06-refund-rate | mysql | ok | 497.1 | 510.2 |
| q06-refund-rate | pintail | ok | 194.3 | 196.5 |
| q07-product-performance | mysql | ok | 469.4 | 471.9 |
| q07-product-performance | pintail | ok | 193.0 | 193.0 |
| q08-regional-cohorts | mysql | ok | 218.6 | 226.2 |
| q08-regional-cohorts | pintail | ok | 349.3 | 351.7 |
| q09-order-lifecycle | mysql | ok | 159.6 | 161.0 |
| q09-order-lifecycle | pintail | ok | 367.4 | 367.6 |
| q10-wide-operational-join | mysql | ok | 160.2 | 193.2 |
| q10-wide-operational-join | pintail | ok | 151.1 | 156.1 |
| q11-dormant-customers | mysql | ok | 6.0 | 10.2 |
| q11-dormant-customers | pintail | ok | 9.7 | 13.5 |
| q12-per-customer-revenue | mysql | ok | 5.5 | 7.9 |
| q12-per-customer-revenue | pintail | ok | 4.1 | 9.5 |
