# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/slaclab/canopy/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                         |    Stmts |     Miss |   Cover |   Missing |
|------------------------------------------------------------- | -------: | -------: | ------: | --------: |
| src/canopy/\_\_init\_\_.py                                   |        5 |        2 |     60% |       5-6 |
| src/canopy/analytics/\_\_init\_\_.py                         |        0 |        0 |    100% |           |
| src/canopy/analytics/api/\_\_init\_\_.py                     |        0 |        0 |    100% |           |
| src/canopy/analytics/api/v1/\_\_init\_\_.py                  |        0 |        0 |    100% |           |
| src/canopy/analytics/api/v1/events.py                        |        9 |        0 |    100% |           |
| src/canopy/analytics/api/v1/health.py                        |        9 |        0 |    100% |           |
| src/canopy/analytics/api/v1/router.py                        |        6 |        0 |    100% |           |
| src/canopy/analytics/app.py                                  |       23 |        0 |    100% |           |
| src/canopy/analytics/config.py                               |        7 |        0 |    100% |           |
| src/canopy/analytics/dependencies.py                         |       16 |        0 |    100% |           |
| src/canopy/analytics/errors.py                               |        5 |        0 |    100% |           |
| src/canopy/analytics/gateway.py                              |        8 |        0 |    100% |           |
| src/canopy/analytics/runtime.py                              |        6 |        0 |    100% |           |
| src/canopy/analytics/schemas/\_\_init\_\_.py                 |        0 |        0 |    100% |           |
| src/canopy/analytics/schemas/events.py                       |       10 |        0 |    100% |           |
| src/canopy/analytics/schemas/health.py                       |        2 |        0 |    100% |           |
| src/canopy/analytics/services/\_\_init\_\_.py                |        0 |        0 |    100% |           |
| src/canopy/analytics/services/analytics\_service.py          |       22 |        0 |    100% |           |
| src/canopy/contracts/\_\_init\_\_.py                         |       11 |        0 |    100% |           |
| src/canopy/contracts/base.py                                 |        4 |        0 |    100% |           |
| src/canopy/contracts/formula.py                              |       82 |        0 |    100% |           |
| src/canopy/contracts/health.py                               |       29 |        0 |    100% |           |
| src/canopy/contracts/jobs.py                                 |        3 |        0 |    100% |           |
| src/canopy/contracts/pagination.py                           |        4 |        0 |    100% |           |
| src/canopy/contracts/pv.py                                   |       35 |        0 |    100% |           |
| src/canopy/contracts/sample.py                               |       11 |        0 |    100% |           |
| src/canopy/converter/\_\_init\_\_.py                         |        0 |        0 |    100% |           |
| src/canopy/converter/adapters/\_\_init\_\_.py                |        0 |        0 |    100% |           |
| src/canopy/converter/adapters/edm\_lists.py                  |       17 |        0 |    100% |           |
| src/canopy/converter/adapters/formula\_engine\_validation.py |       18 |        0 |    100% |           |
| src/canopy/converter/api/\_\_init\_\_.py                     |        0 |        0 |    100% |           |
| src/canopy/converter/api/v1/\_\_init\_\_.py                  |        0 |        0 |    100% |           |
| src/canopy/converter/api/v1/conversions.py                   |       80 |        1 |     99% |        59 |
| src/canopy/converter/api/v1/health.py                        |        3 |        0 |    100% |           |
| src/canopy/converter/api/v1/router.py                        |        6 |        0 |    100% |           |
| src/canopy/converter/app.py                                  |       26 |        0 |    100% |           |
| src/canopy/converter/config.py                               |       16 |        0 |    100% |           |
| src/canopy/converter/dependencies.py                         |       16 |        0 |    100% |           |
| src/canopy/converter/errors.py                               |       14 |        0 |    100% |           |
| src/canopy/converter/gateway.py                              |       15 |        0 |    100% |           |
| src/canopy/converter/runtime.py                              |        4 |        0 |    100% |           |
| src/canopy/converter/schemas/\_\_init\_\_.py                 |        0 |        0 |    100% |           |
| src/canopy/converter/schemas/conversions.py                  |       21 |        0 |    100% |           |
| src/canopy/converter/services/\_\_init\_\_.py                |        0 |        0 |    100% |           |
| src/canopy/converter/services/conversion\_service.py         |       90 |        1 |     99% |       155 |
| src/canopy/converter/tasks.py                                |       99 |        5 |     95% |98, 120, 125, 152-153 |
| src/canopy/core/\_\_init\_\_.py                              |        0 |        0 |    100% |           |
| src/canopy/core/app\_names.py                                |        5 |        0 |    100% |           |
| src/canopy/core/clock.py                                     |       16 |        1 |     94% |        29 |
| src/canopy/core/config.py                                    |       39 |        0 |    100% |           |
| src/canopy/core/db.py                                        |       73 |        5 |     93% |90-91, 169-171 |
| src/canopy/core/decimation.py                                |       35 |        1 |     97% |        53 |
| src/canopy/core/errors.py                                    |      149 |        0 |    100% |           |
| src/canopy/core/facilities/\_\_init\_\_.py                   |        0 |        0 |    100% |           |
| src/canopy/core/facilities/dependencies.py                   |        9 |        0 |    100% |           |
| src/canopy/core/facilities/gateway.py                        |       37 |        2 |     95% |     62-63 |
| src/canopy/core/facilities/models.py                         |       17 |        0 |    100% |           |
| src/canopy/core/facilities/repositories.py                   |       48 |        1 |     98% |        34 |
| src/canopy/core/facilities/routes.py                         |       28 |        0 |    100% |           |
| src/canopy/core/facilities/schemas.py                        |       24 |        0 |    100% |           |
| src/canopy/core/facilities/service.py                        |      103 |       11 |     89% |63-64, 75-76, 79-80, 131-132, 138, 150, 159 |
| src/canopy/core/health.py                                    |       11 |        0 |    100% |           |
| src/canopy/core/keys/\_\_init\_\_.py                         |        0 |        0 |    100% |           |
| src/canopy/core/keys/dependencies.py                         |        9 |        0 |    100% |           |
| src/canopy/core/keys/models.py                               |       19 |        0 |    100% |           |
| src/canopy/core/keys/registry.py                             |      121 |        0 |    100% |           |
| src/canopy/core/keys/repositories.py                         |       39 |        0 |    100% |           |
| src/canopy/core/keys/routes.py                               |       17 |        0 |    100% |           |
| src/canopy/core/keys/schemas.py                              |       14 |        0 |    100% |           |
| src/canopy/core/keys/service.py                              |       97 |        0 |    100% |           |
| src/canopy/core/keys/tokens.py                               |        8 |        0 |    100% |           |
| src/canopy/core/logging.py                                   |       66 |        0 |    100% |           |
| src/canopy/core/metrics.py                                   |       55 |        2 |     96% |   122-123 |
| src/canopy/core/modules.py                                   |       15 |        0 |    100% |           |
| src/canopy/core/runtime.py                                   |        8 |        0 |    100% |           |
| src/canopy/core/security.py                                  |      214 |        7 |     97% |411-412, 450, 533, 541-546 |
| src/canopy/core/time\_parse.py                               |       52 |        1 |     98% |       131 |
| src/canopy/core/validation.py                                |       18 |        0 |    100% |           |
| src/canopy/elog/\_\_init\_\_.py                              |        1 |        0 |    100% |           |
| src/canopy/elog/adapters/\_\_init\_\_.py                     |        0 |        0 |    100% |           |
| src/canopy/elog/adapters/base.py                             |       35 |        5 |     86% |52, 62, 68, 74, 78 |
| src/canopy/elog/adapters/elog\_plus.py                       |      176 |        6 |     97% |112, 115-116, 137, 195, 333 |
| src/canopy/elog/adapters/registry.py                         |       82 |        5 |     94% |74-75, 88-89, 110 |
| src/canopy/elog/api/\_\_init\_\_.py                          |        0 |        0 |    100% |           |
| src/canopy/elog/api/v1/\_\_init\_\_.py                       |        0 |        0 |    100% |           |
| src/canopy/elog/api/v1/apps.py                               |       33 |        0 |    100% |           |
| src/canopy/elog/api/v1/health.py                             |       10 |        0 |    100% |           |
| src/canopy/elog/api/v1/router.py                             |        6 |        0 |    100% |           |
| src/canopy/elog/app.py                                       |       36 |        0 |    100% |           |
| src/canopy/elog/config.py                                    |       48 |        2 |     96% |   49, 119 |
| src/canopy/elog/dependencies.py                              |        7 |        1 |     86% |        15 |
| src/canopy/elog/errors.py                                    |       23 |        0 |    100% |           |
| src/canopy/elog/gateway.py                                   |       15 |        0 |    100% |           |
| src/canopy/elog/runtime.py                                   |        4 |        0 |    100% |           |
| src/canopy/elog/schemas/\_\_init\_\_.py                      |        0 |        0 |    100% |           |
| src/canopy/elog/schemas/elog.py                              |       28 |        0 |    100% |           |
| src/canopy/elog/schemas/health.py                            |        9 |        0 |    100% |           |
| src/canopy/elog/services/\_\_init\_\_.py                     |        0 |        0 |    100% |           |
| src/canopy/elog/services/elog\_service.py                    |       79 |        1 |     99% |        56 |
| src/canopy/epics/\_\_init\_\_.py                             |        0 |        0 |    100% |           |
| src/canopy/epics/backends/\_\_init\_\_.py                    |        0 |        0 |    100% |           |
| src/canopy/epics/backends/epics\_backend.py                  |      456 |       61 |     87% |87, 106-107, 115, 119, 149-150, 196-197, 202, 260-261, 428, 439-441, 455-457, 465-466, 477-478, 534-535, 538-562, 568-569, 593, 602-604, 637-638, 666, 671-672, 730-731, 773, 791, 812-813, 815, 825, 874, 884-885, 904-905 |
| src/canopy/epics/backends/simulation.py                      |      164 |        7 |     96% |160, 162-165, 385, 389 |
| src/canopy/epics/base.py                                     |       57 |        8 |     86% |162, 166, 170, 174, 201, 205, 211, 257 |
| src/canopy/epics/config.py                                   |       25 |        0 |    100% |           |
| src/canopy/epics/gateway.py                                  |       31 |        0 |    100% |           |
| src/canopy/epics/pv\_protocol.py                             |       49 |        0 |    100% |           |
| src/canopy/epics/testing/\_\_init\_\_.py                     |        0 |        0 |    100% |           |
| src/canopy/epics/testing/conformance.py                      |      170 |       42 |     75% |88, 93, 121, 124, 134, 146, 158, 182-183, 245-276, 317-327 |
| src/canopy/formula\_engine/\_\_init\_\_.py                   |        0 |        0 |    100% |           |
| src/canopy/formula\_engine/api/\_\_init\_\_.py               |        0 |        0 |    100% |           |
| src/canopy/formula\_engine/api/v1/\_\_init\_\_.py            |        0 |        0 |    100% |           |
| src/canopy/formula\_engine/api/v1/evaluate.py                |        9 |        0 |    100% |           |
| src/canopy/formula\_engine/api/v1/health.py                  |        3 |        0 |    100% |           |
| src/canopy/formula\_engine/api/v1/router.py                  |       10 |        0 |    100% |           |
| src/canopy/formula\_engine/api/v1/validate.py                |       10 |        0 |    100% |           |
| src/canopy/formula\_engine/api/v1/ws.py                      |      136 |       14 |     90% |170-174, 179-180, 205-207, 225, 240-242 |
| src/canopy/formula\_engine/app.py                            |       45 |        0 |    100% |           |
| src/canopy/formula\_engine/config.py                         |       31 |        0 |    100% |           |
| src/canopy/formula\_engine/dependencies.py                   |       11 |        2 |     82% |    16, 20 |
| src/canopy/formula\_engine/errors.py                         |       49 |        0 |    100% |           |
| src/canopy/formula\_engine/gateway.py                        |       28 |        7 |     75% |31-33, 44, 55-57 |
| src/canopy/formula\_engine/language/\_\_init\_\_.py          |        0 |        0 |    100% |           |
| src/canopy/formula\_engine/language/grammar.py               |       18 |        2 |     89% |     72-73 |
| src/canopy/formula\_engine/language/namespace.py             |       48 |        3 |     94% |17, 35, 39 |
| src/canopy/formula\_engine/ports.py                          |       41 |        7 |     83% |121, 137-139, 149-151, 154-156 |
| src/canopy/formula\_engine/runtime.py                        |        4 |        0 |    100% |           |
| src/canopy/formula\_engine/schemas/\_\_init\_\_.py           |        0 |        0 |    100% |           |
| src/canopy/formula\_engine/schemas/formula.py                |       21 |        0 |    100% |           |
| src/canopy/formula\_engine/services/\_\_init\_\_.py          |        0 |        0 |    100% |           |
| src/canopy/formula\_engine/services/evaluator.py             |      214 |       13 |     94% |74, 121, 130, 134, 139, 171-173, 216, 271, 335, 355, 416 |
| src/canopy/formula\_engine/services/formula\_service.py      |       98 |        2 |     98% |   87, 170 |
| src/canopy/formula\_engine/services/parser.py                |      130 |       11 |     92% |105-109, 157-158, 163, 165, 167, 170, 176, 202, 212 |
| src/canopy/formula\_engine/services/scalar\_evaluator.py     |       34 |        1 |     97% |        43 |
| src/canopy/formula\_engine/services/subscription\_manager.py |      533 |       28 |     95% |420, 648, 663, 666, 672, 682, 747, 767, 770-776, 833-834, 856, 859-860, 874-877, 897, 916, 922, 1128 |
| src/canopy/formula\_engine/services/table\_store.py          |      954 |       30 |     97% |269, 272-274, 289-290, 319-320, 330-333, 476, 483, 621, 670, 694-696, 928, 949, 997-998, 1014, 1079, 1084, 1260, 1383, 1426, 1438-1439 |
| src/canopy/formula\_engine/services/timeseries\_evaluator.py |       42 |        8 |     81% |58, 67, 80-85 |
| src/canopy/formula\_engine/services/waveform\_evaluator.py   |       40 |        2 |     95% |    52, 69 |
| src/canopy/hare/\_\_init\_\_.py                              |        0 |        0 |    100% |           |
| src/canopy/hare/api/\_\_init\_\_.py                          |        0 |        0 |    100% |           |
| src/canopy/hare/api/v1/\_\_init\_\_.py                       |        0 |        0 |    100% |           |
| src/canopy/hare/api/v1/health.py                             |       44 |        0 |    100% |           |
| src/canopy/hare/api/v1/pvs.py                                |       18 |        0 |    100% |           |
| src/canopy/hare/api/v1/router.py                             |        8 |        0 |    100% |           |
| src/canopy/hare/api/v1/ws.py                                 |       42 |        4 |     90% | 92, 95-97 |
| src/canopy/hare/app.py                                       |       32 |        0 |    100% |           |
| src/canopy/hare/config.py                                    |       16 |        0 |    100% |           |
| src/canopy/hare/dependencies.py                              |        7 |        0 |    100% |           |
| src/canopy/hare/errors.py                                    |        2 |        2 |      0% |      9-12 |
| src/canopy/hare/gateway.py                                   |       11 |        1 |     91% |        29 |
| src/canopy/hare/metrics.py                                   |       23 |        0 |    100% |           |
| src/canopy/hare/runtime.py                                   |        4 |        0 |    100% |           |
| src/canopy/hare/schemas/\_\_init\_\_.py                      |        0 |        0 |    100% |           |
| src/canopy/hare/schemas/pv.py                                |       12 |        0 |    100% |           |
| src/canopy/hare/schemas/ws.py                                |       23 |        0 |    100% |           |
| src/canopy/hare/services/\_\_init\_\_.py                     |        0 |        0 |    100% |           |
| src/canopy/hare/services/broker.py                           |      542 |       51 |     91% |122, 159, 188-189, 196-197, 260, 325, 445, 467, 479-480, 508, 541, 543, 550-551, 640, 675-676, 705, 708-710, 738-739, 754-755, 765-766, 821, 827, 890-891, 950-953, 958, 961-968, 985, 1069-1070, 1080-1081 |
| src/canopy/hare/services/subscriptions.py                    |       51 |        2 |     96% |    30, 72 |
| src/canopy/jobs/\_\_init\_\_.py                              |        0 |        0 |    100% |           |
| src/canopy/jobs/api/\_\_init\_\_.py                          |        0 |        0 |    100% |           |
| src/canopy/jobs/api/v1/\_\_init\_\_.py                       |        0 |        0 |    100% |           |
| src/canopy/jobs/api/v1/health.py                             |        3 |        0 |    100% |           |
| src/canopy/jobs/api/v1/jobs.py                               |       12 |        0 |    100% |           |
| src/canopy/jobs/api/v1/router.py                             |        6 |        0 |    100% |           |
| src/canopy/jobs/app.py                                       |       21 |        0 |    100% |           |
| src/canopy/jobs/errors.py                                    |        8 |        0 |    100% |           |
| src/canopy/jobs/gateway.py                                   |       62 |        9 |     85% |42-47, 53-54, 63 |
| src/canopy/jobs/runtime.py                                   |        4 |        0 |    100% |           |
| src/canopy/jobs/schemas/\_\_init\_\_.py                      |        0 |        0 |    100% |           |
| src/canopy/jobs/schemas/job.py                               |       18 |        0 |    100% |           |
| src/canopy/magpie/\_\_init\_\_.py                            |        0 |        0 |    100% |           |
| src/canopy/magpie/api/\_\_init\_\_.py                        |        0 |        0 |    100% |           |
| src/canopy/magpie/api/v1/\_\_init\_\_.py                     |        0 |        0 |    100% |           |
| src/canopy/magpie/api/v1/\_validation.py                     |        4 |        0 |    100% |           |
| src/canopy/magpie/api/v1/apps.py                             |        8 |        0 |    100% |           |
| src/canopy/magpie/api/v1/configs.py                          |       46 |        2 |     96% |    86, 97 |
| src/canopy/magpie/api/v1/debug.py                            |       20 |        0 |    100% |           |
| src/canopy/magpie/api/v1/health.py                           |        9 |        1 |     89% |        15 |
| src/canopy/magpie/api/v1/router.py                           |       10 |        0 |    100% |           |
| src/canopy/magpie/app.py                                     |       17 |        0 |    100% |           |
| src/canopy/magpie/data/\_\_init\_\_.py                       |        0 |        0 |    100% |           |
| src/canopy/magpie/data/models.py                             |       15 |        0 |    100% |           |
| src/canopy/magpie/data/repositories.py                       |       52 |        3 |     94% |     92-97 |
| src/canopy/magpie/dependencies.py                            |        7 |        0 |    100% |           |
| src/canopy/magpie/errors.py                                  |       11 |        0 |    100% |           |
| src/canopy/magpie/gateway.py                                 |        7 |        7 |      0% |     12-23 |
| src/canopy/magpie/runtime.py                                 |        6 |        1 |     83% |        19 |
| src/canopy/magpie/schemas/\_\_init\_\_.py                    |        0 |        0 |    100% |           |
| src/canopy/magpie/schemas/config.py                          |       15 |        0 |    100% |           |
| src/canopy/magpie/schemas/health.py                          |        2 |        0 |    100% |           |
| src/canopy/magpie/services/\_\_init\_\_.py                   |        0 |        0 |    100% |           |
| src/canopy/magpie/services/config\_service.py                |       97 |        8 |     92% |49, 118-121, 124, 201-202 |
| src/canopy/main.py                                           |      150 |        6 |     96% |     77-82 |
| src/canopy/mole/\_\_init\_\_.py                              |        0 |        0 |    100% |           |
| src/canopy/mole/adapters/\_\_init\_\_.py                     |        1 |        0 |    100% |           |
| src/canopy/mole/adapters/appliance.py                        |      225 |       19 |     92% |43-44, 46, 49, 57-58, 90, 263, 307-310, 351, 358-359, 373-374, 385-386 |
| src/canopy/mole/adapters/base.py                             |      120 |       11 |     91% |86, 181, 190-191, 232, 243, 260, 273, 281, 285, 314 |
| src/canopy/mole/adapters/registry.py                         |       69 |        6 |     91% |61-69, 84-85 |
| src/canopy/mole/api/\_\_init\_\_.py                          |        0 |        0 |    100% |           |
| src/canopy/mole/api/v1/\_\_init\_\_.py                       |        0 |        0 |    100% |           |
| src/canopy/mole/api/v1/archivers.py                          |        8 |        0 |    100% |           |
| src/canopy/mole/api/v1/data.py                               |       37 |        2 |     95% |    54, 57 |
| src/canopy/mole/api/v1/health.py                             |       10 |        0 |    100% |           |
| src/canopy/mole/api/v1/metadata.py                           |       11 |        0 |    100% |           |
| src/canopy/mole/api/v1/router.py                             |       12 |        0 |    100% |           |
| src/canopy/mole/api/v1/search.py                             |       10 |        0 |    100% |           |
| src/canopy/mole/app.py                                       |       27 |        0 |    100% |           |
| src/canopy/mole/config.py                                    |       12 |        0 |    100% |           |
| src/canopy/mole/dependencies.py                              |        7 |        1 |     86% |        15 |
| src/canopy/mole/gateway.py                                   |       33 |        6 |     82% |108-117, 122, 132-133, 138 |
| src/canopy/mole/runtime.py                                   |        4 |        0 |    100% |           |
| src/canopy/mole/schemas/\_\_init\_\_.py                      |        0 |        0 |    100% |           |
| src/canopy/mole/schemas/archiver.py                          |       10 |        0 |    100% |           |
| src/canopy/mole/schemas/pv.py                                |       12 |        0 |    100% |           |
| src/canopy/mole/schemas/search.py                            |        2 |        0 |    100% |           |
| src/canopy/mole/schemas/series.py                            |       17 |        0 |    100% |           |
| src/canopy/mole/services/\_\_init\_\_.py                     |        0 |        0 |    100% |           |
| src/canopy/mole/services/archiver\_service.py                |      110 |        4 |     96% |121, 142, 252-253 |
| src/canopy/raccoon/\_\_init\_\_.py                           |        1 |        0 |    100% |           |
| src/canopy/raccoon/adapters/\_\_init\_\_.py                  |        0 |        0 |    100% |           |
| src/canopy/raccoon/adapters/base.py                          |       42 |        3 |     93% |96, 127, 131 |
| src/canopy/raccoon/adapters/meme.py                          |       84 |       12 |     86% |66-67, 76-77, 86, 98, 134, 136, 138, 151, 153, 155 |
| src/canopy/raccoon/adapters/registry.py                      |       45 |        4 |     91% |42-43, 69, 84 |
| src/canopy/raccoon/api/\_\_init\_\_.py                       |        0 |        0 |    100% |           |
| src/canopy/raccoon/api/v1/\_\_init\_\_.py                    |        0 |        0 |    100% |           |
| src/canopy/raccoon/api/v1/devices.py                         |       11 |        0 |    100% |           |
| src/canopy/raccoon/api/v1/elements.py                        |       11 |        0 |    100% |           |
| src/canopy/raccoon/api/v1/health.py                          |       10 |        0 |    100% |           |
| src/canopy/raccoon/api/v1/pvs.py                             |       17 |        0 |    100% |           |
| src/canopy/raccoon/api/v1/router.py                          |       10 |        0 |    100% |           |
| src/canopy/raccoon/app.py                                    |       36 |        1 |     97% |        46 |
| src/canopy/raccoon/cache/\_\_init\_\_.py                     |        0 |        0 |    100% |           |
| src/canopy/raccoon/cache/flight.py                           |       32 |        0 |    100% |           |
| src/canopy/raccoon/cache/keys.py                             |       11 |        0 |    100% |           |
| src/canopy/raccoon/cache/name\_cache.py                      |       39 |        1 |     97% |        73 |
| src/canopy/raccoon/cache/pattern\_cache.py                   |       38 |        1 |     97% |        63 |
| src/canopy/raccoon/config.py                                 |       18 |        0 |    100% |           |
| src/canopy/raccoon/dependencies.py                           |        7 |        1 |     86% |        15 |
| src/canopy/raccoon/gateway.py                                |       22 |        0 |    100% |           |
| src/canopy/raccoon/runtime.py                                |        4 |        0 |    100% |           |
| src/canopy/raccoon/schemas/\_\_init\_\_.py                   |        0 |        0 |    100% |           |
| src/canopy/raccoon/schemas/directory.py                      |       10 |        0 |    100% |           |
| src/canopy/raccoon/services/\_\_init\_\_.py                  |        0 |        0 |    100% |           |
| src/canopy/raccoon/services/directory\_service.py            |      107 |        5 |     95% |83, 136, 192-198 |
| src/canopy/squirrel/\_\_init\_\_.py                          |        0 |        0 |    100% |           |
| src/canopy/squirrel/adapters/\_\_init\_\_.py                 |        0 |        0 |    100% |           |
| src/canopy/squirrel/adapters/epics\_io.py                    |       93 |        1 |     99% |       199 |
| src/canopy/squirrel/api/\_\_init\_\_.py                      |        0 |        0 |    100% |           |
| src/canopy/squirrel/api/v1/\_\_init\_\_.py                   |        0 |        0 |    100% |           |
| src/canopy/squirrel/api/v1/health.py                         |       73 |        2 |     97% |   106-107 |
| src/canopy/squirrel/api/v1/pvs.py                            |       88 |        1 |     99% |       182 |
| src/canopy/squirrel/api/v1/router.py                         |       10 |        0 |    100% |           |
| src/canopy/squirrel/api/v1/snapshots.py                      |       70 |        3 |     96% |142, 191, 200 |
| src/canopy/squirrel/api/v1/tags.py                           |       89 |       14 |     84% |54-58, 72, 87, 98, 109, 145-147, 159-162 |
| src/canopy/squirrel/app.py                                   |       24 |        0 |    100% |           |
| src/canopy/squirrel/config.py                                |       29 |        0 |    100% |           |
| src/canopy/squirrel/data/\_\_init\_\_.py                     |        0 |        0 |    100% |           |
| src/canopy/squirrel/data/models.py                           |       64 |        0 |    100% |           |
| src/canopy/squirrel/data/repositories.py                     |      323 |       39 |     88% |107, 131-137, 250, 273-288, 456-467, 656-657, 663-665, 668-670, 686-688, 728, 758-762 |
| src/canopy/squirrel/dependencies.py                          |       20 |        0 |    100% |           |
| src/canopy/squirrel/errors.py                                |       26 |        0 |    100% |           |
| src/canopy/squirrel/gateway.py                               |       12 |        0 |    100% |           |
| src/canopy/squirrel/pv\_cache/\_\_init\_\_.py                |        4 |        0 |    100% |           |
| src/canopy/squirrel/pv\_cache/keys.py                        |       46 |        0 |    100% |           |
| src/canopy/squirrel/pv\_cache/monitor.py                     |      260 |       27 |     90% |57-59, 64, 111, 156-157, 220-221, 237, 259-263, 276, 305-306, 325, 349-350, 371-376, 414-415 |
| src/canopy/squirrel/pv\_cache/reader.py                      |       64 |        2 |     97% |     81-82 |
| src/canopy/squirrel/restore\_hooks/\_\_init\_\_.py           |        4 |        0 |    100% |           |
| src/canopy/squirrel/restore\_hooks/base.py                   |        7 |        0 |    100% |           |
| src/canopy/squirrel/restore\_hooks/magnet\_trim.py           |       22 |        0 |    100% |           |
| src/canopy/squirrel/restore\_hooks/registry.py               |       52 |        0 |    100% |           |
| src/canopy/squirrel/restore\_hooks/worker\_status.py         |       15 |        0 |    100% |           |
| src/canopy/squirrel/restore\_hooks/write\_to\_con.py         |       11 |        0 |    100% |           |
| src/canopy/squirrel/runtime.py                               |        4 |        0 |    100% |           |
| src/canopy/squirrel/schemas/\_\_init\_\_.py                  |        0 |        0 |    100% |           |
| src/canopy/squirrel/schemas/health.py                        |       29 |        0 |    100% |           |
| src/canopy/squirrel/schemas/meta\_pv.py                      |       10 |        0 |    100% |           |
| src/canopy/squirrel/schemas/pv.py                            |       33 |        0 |    100% |           |
| src/canopy/squirrel/schemas/snapshot.py                      |       47 |        0 |    100% |           |
| src/canopy/squirrel/schemas/tag.py                           |       26 |        0 |    100% |           |
| src/canopy/squirrel/services/\_\_init\_\_.py                 |        0 |        0 |    100% |           |
| src/canopy/squirrel/services/bulk\_insert.py                 |       36 |        1 |     97% |        88 |
| src/canopy/squirrel/services/pv\_service.py                  |      247 |       23 |     91% |299, 313-314, 439-440, 482, 484, 486-489, 500-504, 510-511, 544-547, 552, 558, 572, 597 |
| src/canopy/squirrel/services/snapshot\_service.py            |      439 |       27 |     94% |131-136, 148, 150, 153, 157-159, 285-287, 498, 502, 549, 577, 773, 790, 983, 1029-1030, 1032-1033, 1039 |
| src/canopy/squirrel/services/tag\_service.py                 |       79 |       18 |     77% |59-70, 75, 94, 114, 118, 121, 128 |
| src/canopy/squirrel/tasks.py                                 |       56 |       25 |     55% |35-36, 56-58, 79-100, 128-131 |
| src/canopy/vole/\_\_init\_\_.py                              |        0 |        0 |    100% |           |
| src/canopy/vole/adapter.py                                   |      156 |       45 |     71% |155-206, 233, 259, 266 |
| src/canopy/vole/api/\_\_init\_\_.py                          |        0 |        0 |    100% |           |
| src/canopy/vole/api/v1/\_\_init\_\_.py                       |        0 |        0 |    100% |           |
| src/canopy/vole/api/v1/debug.py                              |       15 |        0 |    100% |           |
| src/canopy/vole/api/v1/health.py                             |       14 |        0 |    100% |           |
| src/canopy/vole/api/v1/pvs.py                                |       50 |        3 |     94% |43, 46, 96 |
| src/canopy/vole/api/v1/router.py                             |        8 |        0 |    100% |           |
| src/canopy/vole/app.py                                       |       58 |        0 |    100% |           |
| src/canopy/vole/buffer/\_\_init\_\_.py                       |        0 |        0 |    100% |           |
| src/canopy/vole/buffer/ring.py                               |      169 |        2 |     99% |  130, 158 |
| src/canopy/vole/buffer/store.py                              |      101 |        2 |     98% |   88, 106 |
| src/canopy/vole/config.py                                    |       12 |        0 |    100% |           |
| src/canopy/vole/dependencies.py                              |       11 |        1 |     91% |        11 |
| src/canopy/vole/errors.py                                    |       17 |        0 |    100% |           |
| src/canopy/vole/gateway.py                                   |       81 |       16 |     80% |50, 56, 70, 79, 83, 97, 109, 113, 120, 124, 128, 132, 137, 152, 165, 193 |
| src/canopy/vole/runtime.py                                   |        6 |        0 |    100% |           |
| src/canopy/vole/schemas/\_\_init\_\_.py                      |        0 |        0 |    100% |           |
| src/canopy/vole/schemas/buffer.py                            |       26 |        1 |     96% |        38 |
| src/canopy/vole/services/\_\_init\_\_.py                     |        0 |        0 |    100% |           |
| src/canopy/vole/services/alignment\_service.py               |      149 |       23 |     85% |86, 95, 100, 128, 187, 202-220, 232, 264, 266, 271-272 |
| src/canopy/vole/services/buffer\_service.py                  |      198 |        3 |     98% |73, 340, 393 |
| src/canopy/vole/services/eviction\_service.py                |       43 |        0 |    100% |           |
| src/canopy/vole/services/grace\_manager.py                   |       66 |        0 |    100% |           |
| src/canopy/vole/services/hare\_ingest.py                     |      135 |        9 |     93% |60-61, 68, 97, 127, 191, 236-239 |
| src/canopy/vole/services/notifier.py                         |       35 |        2 |     94% |     33-34 |
| src/canopy/worker.py                                         |       47 |        1 |     98% |        34 |
| **TOTAL**                                                    | **12677** |  **773** | **94%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/slaclab/canopy/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/slaclab/canopy/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/slaclab/canopy/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/slaclab/canopy/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2Fslaclab%2Fcanopy%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/slaclab/canopy/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.