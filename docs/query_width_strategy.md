# Query Width Strategy Summary

- Samples: 5 watch model runs

## Recommended default waterfall

1. eBay: start `L1_precise_new`; if count < 20, expand to `L2_precise`; avoid `L4_broad`.
2. Yahoo: start `L2_precise`; if count < 20, expand to `L3_mid`; avoid `L4_broad`.
3. Rakuten: start `L2_precise`; if count < 10, expand to `L3_mid`; only then consider `L4_broad`.

## Recommendation frequency

- ebay: {'L1_precise_new': 4, 'L2_precise': 1}
- yahoo: {'L2_precise': 1, 'L3_mid': 4}
- rakuten: {'L2_precise': 2, 'L3_mid': 3}

## Median hit count per stage

- ebay: {'L1_precise_new': 312.0, 'L2_precise': 185.0, 'L3_mid': 207.0, 'L4_broad': 241253.0}
- yahoo: {'L1_precise_new': 0.0, 'L2_precise': 4.0, 'L3_mid': 155.0, 'L4_broad': 6389.0}
- rakuten: {'L1_precise_new': 0.0, 'L2_precise': 9.0, 'L3_mid': 23.0, 'L4_broad': 5877.0}

## Per-case result

- casio ga2100 (query_width_report_casio_ga2100.json)
  - ebay: rec=L1_precise_new counts={'L1_precise_new': 3288, 'L2_precise': 4904, 'L3_mid': 5465, 'L4_broad': 241253}
  - yahoo: rec=L2_precise counts={'L1_precise_new': 9, 'L2_precise': 273, 'L3_mid': 6322, 'L4_broad': 6389}
  - rakuten: rec=L2_precise counts={'L1_precise_new': 0, 'L2_precise': 478, 'L3_mid': 1542, 'L4_broad': 10686}
- casio gwm5610 (query_width_report_casio_gwm5610.json)
  - ebay: rec=L1_precise_new counts={'L1_precise_new': 315, 'L2_precise': 1221, 'L3_mid': 1571, 'L4_broad': 241253}
  - yahoo: rec=L3_mid counts={'L1_precise_new': 0, 'L2_precise': 10, 'L3_mid': 131, 'L4_broad': 6389}
  - rakuten: rec=L2_precise counts={'L1_precise_new': 0, 'L2_precise': 135, 'L3_mid': 514, 'L4_broad': 10686}
- citizen bn0150 (query_width_report_citizen_bn0150.json)
  - ebay: rec=L1_precise_new counts={'L1_precise_new': 107, 'L2_precise': 162, 'L3_mid': 196, 'L4_broad': 155900}
  - yahoo: rec=L3_mid counts={'L1_precise_new': 0, 'L2_precise': 4, 'L3_mid': 32, 'L4_broad': 5542}
  - rakuten: rec=L3_mid counts={'L1_precise_new': 0, 'L2_precise': 9, 'L3_mid': 16, 'L4_broad': 4770}
- seiko sbdc101 (query_width_report_seiko_sbdc101.json)
  - ebay: rec=L1_precise_new counts={'L1_precise_new': 312, 'L2_precise': 185, 'L3_mid': 207, 'L4_broad': 311396}
  - yahoo: rec=L3_mid counts={'L1_precise_new': 0, 'L2_precise': 2, 'L3_mid': 200, 'L4_broad': 15951}
  - rakuten: rec=L3_mid counts={'L1_precise_new': 0, 'L2_precise': 1, 'L3_mid': 23, 'L4_broad': 5877}
- seiko sbga211 (query_width_report_seiko_sbga211.json)
  - ebay: rec=L2_precise counts={'L1_precise_new': 15, 'L2_precise': 44, 'L3_mid': 47, 'L4_broad': 311396}
  - yahoo: rec=L3_mid counts={'L1_precise_new': 0, 'L2_precise': 0, 'L3_mid': 155, 'L4_broad': 15951}
  - rakuten: rec=L3_mid counts={'L1_precise_new': 0, 'L2_precise': 1, 'L3_mid': 15, 'L4_broad': 5877}
