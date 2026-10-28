# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/slaclab/canopy/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                 |    Stmts |     Miss |   Cover |   Missing |
|----------------------------------------------------- | -------: | -------: | ------: | --------: |
| src/canopy/\_\_init\_\_.py                           |        5 |        2 |     60% |       5-6 |
| src/canopy/analytics/\_\_init\_\_.py                 |        0 |        0 |    100% |           |
| src/canopy/analytics/api/\_\_init\_\_.py             |        0 |        0 |    100% |           |
| src/canopy/analytics/api/v1/\_\_init\_\_.py          |        0 |        0 |    100% |           |
| src/canopy/analytics/api/v1/events.py                |        9 |        0 |    100% |           |
| src/canopy/analytics/api/v1/health.py                |        9 |        0 |    100% |           |
| src/canopy/analytics/api/v1/router.py                |        6 |        0 |    100% |           |
| src/canopy/analytics/app.py                          |       23 |        0 |    100% |           |
| src/canopy/analytics/config.py                       |        7 |        0 |    100% |           |
| src/canopy/analytics/dependencies.py                 |       16 |        0 |    100% |           |
| src/canopy/analytics/errors.py                       |        5 |        0 |    100% |           |
| src/canopy/analytics/gateway.py                      |        8 |        0 |    100% |           |
| src/canopy/analytics/runtime.py                      |        6 |        0 |    100% |           |
| src/canopy/analytics/schemas/\_\_init\_\_.py         |        0 |        0 |    100% |           |
| src/canopy/analytics/schemas/events.py               |       10 |        0 |    100% |           |
| src/canopy/analytics/schemas/health.py               |        2 |        0 |    100% |           |
| src/canopy/analytics/services/\_\_init\_\_.py        |        0 |        0 |    100% |           |
| src/canopy/analytics/services/analytics\_service.py  |       22 |        0 |    100% |           |
| src/canopy/contracts/\_\_init\_\_.py                 |        0 |        0 |    100% |           |
| src/canopy/contracts/base.py                         |        4 |        0 |    100% |           |
| src/canopy/contracts/formula.py                      |       24 |        0 |    100% |           |
| src/canopy/contracts/health.py                       |        2 |        0 |    100% |           |
| src/canopy/contracts/jobs.py                         |        3 |        0 |    100% |           |
| src/canopy/contracts/pagination.py                   |        4 |        0 |    100% |           |
| src/canopy/contracts/pv.py                           |       35 |        0 |    100% |           |
| src/canopy/contracts/sample.py                       |       10 |        0 |    100% |           |
| src/canopy/converter/\_\_init\_\_.py                 |        0 |        0 |    100% |           |
| src/canopy/converter/adapters/\_\_init\_\_.py        |        0 |        0 |    100% |           |
| src/canopy/converter/adapters/edm\_lists.py          |       17 |        0 |    100% |           |
| src/canopy/converter/adapters/fox\_validation.py     |       18 |        0 |    100% |           |
| src/canopy/converter/api/\_\_init\_\_.py             |        0 |        0 |    100% |           |
| src/canopy/converter/api/v1/\_\_init\_\_.py          |        0 |        0 |    100% |           |
| src/canopy/converter/api/v1/conversions.py           |       72 |        1 |     99% |        57 |
| src/canopy/converter/api/v1/health.py                |        3 |        0 |    100% |           |
| src/canopy/converter/api/v1/router.py                |        6 |        0 |    100% |           |
| src/canopy/converter/app.py                          |       26 |        0 |    100% |           |
| src/canopy/converter/config.py                       |       16 |        0 |    100% |           |
| src/canopy/converter/dependencies.py                 |       16 |        0 |    100% |           |
| src/canopy/converter/errors.py                       |       14 |        0 |    100% |           |
| src/canopy/converter/gateway.py                      |       15 |        0 |    100% |           |
| src/canopy/converter/runtime.py                      |        4 |        0 |    100% |           |
| src/canopy/converter/schemas/\_\_init\_\_.py         |        0 |        0 |    100% |           |
| src/canopy/converter/schemas/conversions.py          |       21 |        0 |    100% |           |
| src/canopy/converter/services/\_\_init\_\_.py        |        0 |        0 |    100% |           |
| src/canopy/converter/services/conversion\_service.py |       76 |        1 |     99% |       128 |
| src/canopy/converter/tasks.py                        |       99 |        5 |     95% |98, 120, 125, 152-153 |
| src/canopy/core/\_\_init\_\_.py                      |        0 |        0 |    100% |           |
| src/canopy/core/clock.py                             |       16 |        3 |     81% |13, 26, 29 |
| src/canopy/core/config.py                            |       20 |        0 |    100% |           |
| src/canopy/core/db.py                                |       73 |       17 |     77% |71-72, 90-91, 107-116, 120-123, 168-171 |
| src/canopy/core/decimation.py                        |       35 |        1 |     97% |        53 |
| src/canopy/core/errors.py                            |      112 |        0 |    100% |           |
| src/canopy/core/health.py                            |       11 |        0 |    100% |           |
| src/canopy/core/keys/\_\_init\_\_.py                 |        0 |        0 |    100% |           |
| src/canopy/core/keys/dependencies.py                 |        9 |        0 |    100% |           |
| src/canopy/core/keys/models.py                       |       18 |        0 |    100% |           |
| src/canopy/core/keys/registry.py                     |      121 |        0 |    100% |           |
| src/canopy/core/keys/repositories.py                 |       36 |        0 |    100% |           |
| src/canopy/core/keys/routes.py                       |       17 |        0 |    100% |           |
| src/canopy/core/keys/schemas.py                      |       13 |        0 |    100% |           |
| src/canopy/core/keys/service.py                      |       71 |        0 |    100% |           |
| src/canopy/core/keys/tokens.py                       |        8 |        0 |    100% |           |
| src/canopy/core/logging.py                           |       62 |        0 |    100% |           |
| src/canopy/core/metrics.py                           |       54 |        2 |     96% |   121-122 |
| src/canopy/core/modules.py                           |       14 |        0 |    100% |           |
| src/canopy/core/runtime.py                           |        8 |        0 |    100% |           |
| src/canopy/core/security.py                          |      128 |        7 |     95% |238-239, 247, 307, 315-320 |
| src/canopy/core/time\_parse.py                       |       52 |        1 |     98% |       131 |
| src/canopy/epics/\_\_init\_\_.py                     |        0 |        0 |    100% |           |
| src/canopy/epics/backends/\_\_init\_\_.py            |        0 |        0 |    100% |           |
| src/canopy/epics/backends/epics\_backend.py          |      328 |       79 |     76% |56, 75-76, 84, 88, 118-119, 165-166, 171, 211-212, 227-232, 235-240, 247-248, 286, 289-318, 324-325, 349, 358-360, 370, 373-389, 410, 415-416, 474-475, 517, 535, 556-557, 559, 569, 589-592, 602-603 |
| src/canopy/epics/backends/simulation.py              |      143 |        8 |     94% |143, 145-148, 334, 336, 368 |
| src/canopy/epics/base.py                             |       53 |        8 |     85% |64, 157, 161, 165, 169, 175, 179, 185 |
| src/canopy/epics/config.py                           |       24 |        0 |    100% |           |
| src/canopy/epics/gateway.py                          |       10 |        0 |    100% |           |
| src/canopy/epics/pv\_protocol.py                     |       49 |        0 |    100% |           |
| src/canopy/fox/\_\_init\_\_.py                       |        0 |        0 |    100% |           |
| src/canopy/fox/api/\_\_init\_\_.py                   |        0 |        0 |    100% |           |
| src/canopy/fox/api/v1/\_\_init\_\_.py                |        0 |        0 |    100% |           |
| src/canopy/fox/api/v1/evaluate.py                    |        9 |        0 |    100% |           |
| src/canopy/fox/api/v1/health.py                      |        3 |        0 |    100% |           |
| src/canopy/fox/api/v1/router.py                      |       10 |        0 |    100% |           |
| src/canopy/fox/api/v1/validate.py                    |       10 |        0 |    100% |           |
| src/canopy/fox/api/v1/ws.py                          |      114 |       23 |     80% |120-124, 131-133, 137-138, 142-146, 156-157, 163-166, 179-181 |
| src/canopy/fox/app.py                                |       36 |        0 |    100% |           |
| src/canopy/fox/config.py                             |       10 |        0 |    100% |           |
| src/canopy/fox/dependencies.py                       |       11 |        2 |     82% |    16, 20 |
| src/canopy/fox/errors.py                             |       26 |        0 |    100% |           |
| src/canopy/fox/gateway.py                            |       21 |        8 |     62% |25-27, 37-39, 53, 58 |
| src/canopy/fox/language/\_\_init\_\_.py              |        0 |        0 |    100% |           |
| src/canopy/fox/language/grammar.py                   |       18 |        4 |     78% |     64-67 |
| src/canopy/fox/language/namespace.py                 |       13 |        0 |    100% |           |
| src/canopy/fox/ports.py                              |       22 |        6 |     73% |71-73, 83-85, 88-90 |
| src/canopy/fox/runtime.py                            |        4 |        0 |    100% |           |
| src/canopy/fox/schemas/\_\_init\_\_.py               |        0 |        0 |    100% |           |
| src/canopy/fox/schemas/formula.py                    |       17 |        0 |    100% |           |
| src/canopy/fox/services/\_\_init\_\_.py              |        0 |        0 |    100% |           |
| src/canopy/fox/services/evaluator.py                 |       74 |        9 |     88% |39, 67-68, 70, 83-84, 86, 133, 135 |
| src/canopy/fox/services/formula\_service.py          |       48 |        3 |     94% |     48-50 |
| src/canopy/fox/services/parser.py                    |      120 |       16 |     87% |69, 105-109, 142, 157-158, 163, 165, 167, 170, 176, 194-197, 206 |
| src/canopy/fox/services/scalar\_evaluator.py         |       33 |        1 |     97% |        37 |
| src/canopy/fox/services/subscription\_manager.py     |      267 |       24 |     91% |272-275, 332, 343, 369, 372, 378, 391, 528, 573-577, 601-610 |
| src/canopy/fox/services/timeseries\_evaluator.py     |       42 |        8 |     81% |55, 64, 77-82 |
| src/canopy/fox/services/waveform\_evaluator.py       |       39 |       15 |     62% |39-40, 44, 46, 54-68 |
| src/canopy/hare/\_\_init\_\_.py                      |        0 |        0 |    100% |           |
| src/canopy/hare/api/\_\_init\_\_.py                  |        0 |        0 |    100% |           |
| src/canopy/hare/api/v1/\_\_init\_\_.py               |        0 |        0 |    100% |           |
| src/canopy/hare/api/v1/health.py                     |       11 |        0 |    100% |           |
| src/canopy/hare/api/v1/pvs.py                        |       17 |        0 |    100% |           |
| src/canopy/hare/api/v1/router.py                     |        8 |        0 |    100% |           |
| src/canopy/hare/api/v1/ws.py                         |       41 |        4 |     90% | 80, 83-85 |
| src/canopy/hare/app.py                               |       32 |        0 |    100% |           |
| src/canopy/hare/config.py                            |       15 |        0 |    100% |           |
| src/canopy/hare/dependencies.py                      |        7 |        0 |    100% |           |
| src/canopy/hare/errors.py                            |        2 |        2 |      0% |      9-12 |
| src/canopy/hare/gateway.py                           |       11 |        1 |     91% |        29 |
| src/canopy/hare/metrics.py                           |       23 |        0 |    100% |           |
| src/canopy/hare/runtime.py                           |        4 |        0 |    100% |           |
| src/canopy/hare/schemas/\_\_init\_\_.py              |        0 |        0 |    100% |           |
| src/canopy/hare/schemas/health.py                    |        3 |        0 |    100% |           |
| src/canopy/hare/schemas/pv.py                        |        7 |        0 |    100% |           |
| src/canopy/hare/schemas/ws.py                        |       23 |        0 |    100% |           |
| src/canopy/hare/services/\_\_init\_\_.py             |        0 |        0 |    100% |           |
| src/canopy/hare/services/broker.py                   |      440 |       51 |     88% |94, 129, 152-153, 160-161, 224, 286, 289, 386, 408, 420-421, 449, 482, 484, 491-492, 578, 608, 613-614, 635-636, 651-652, 662-663, 717, 739-750, 755, 758-765, 782, 835-836, 846-847 |
| src/canopy/hare/services/subscriptions.py            |       51 |        2 |     96% |    30, 72 |
| src/canopy/jobs/\_\_init\_\_.py                      |        0 |        0 |    100% |           |
| src/canopy/jobs/api/\_\_init\_\_.py                  |        0 |        0 |    100% |           |
| src/canopy/jobs/api/v1/\_\_init\_\_.py               |        0 |        0 |    100% |           |
| src/canopy/jobs/api/v1/health.py                     |        3 |        0 |    100% |           |
| src/canopy/jobs/api/v1/jobs.py                       |       12 |        0 |    100% |           |
| src/canopy/jobs/api/v1/router.py                     |        6 |        0 |    100% |           |
| src/canopy/jobs/app.py                               |       21 |        0 |    100% |           |
| src/canopy/jobs/errors.py                            |        8 |        0 |    100% |           |
| src/canopy/jobs/gateway.py                           |       62 |        9 |     85% |42-47, 53-54, 63 |
| src/canopy/jobs/runtime.py                           |        4 |        0 |    100% |           |
| src/canopy/jobs/schemas/\_\_init\_\_.py              |        0 |        0 |    100% |           |
| src/canopy/jobs/schemas/job.py                       |       18 |        0 |    100% |           |
| src/canopy/magpie/\_\_init\_\_.py                    |        0 |        0 |    100% |           |
| src/canopy/magpie/api/\_\_init\_\_.py                |        0 |        0 |    100% |           |
| src/canopy/magpie/api/v1/\_\_init\_\_.py             |        0 |        0 |    100% |           |
| src/canopy/magpie/api/v1/\_validation.py             |       15 |        0 |    100% |           |
| src/canopy/magpie/api/v1/apps.py                     |        8 |        0 |    100% |           |
| src/canopy/magpie/api/v1/configs.py                  |       47 |        2 |     96% |   89, 100 |
| src/canopy/magpie/api/v1/debug.py                    |       22 |        0 |    100% |           |
| src/canopy/magpie/api/v1/health.py                   |        9 |        1 |     89% |        15 |
| src/canopy/magpie/api/v1/router.py                   |       10 |        0 |    100% |           |
| src/canopy/magpie/app.py                             |       17 |        0 |    100% |           |
| src/canopy/magpie/data/\_\_init\_\_.py               |        0 |        0 |    100% |           |
| src/canopy/magpie/data/models.py                     |       15 |        0 |    100% |           |
| src/canopy/magpie/data/repositories.py               |       52 |        3 |     94% |     92-97 |
| src/canopy/magpie/dependencies.py                    |        7 |        0 |    100% |           |
| src/canopy/magpie/errors.py                          |       11 |        0 |    100% |           |
| src/canopy/magpie/gateway.py                         |        7 |        7 |      0% |     12-23 |
| src/canopy/magpie/runtime.py                         |        6 |        1 |     83% |        19 |
| src/canopy/magpie/schemas/\_\_init\_\_.py            |        0 |        0 |    100% |           |
| src/canopy/magpie/schemas/config.py                  |       15 |        0 |    100% |           |
| src/canopy/magpie/schemas/health.py                  |        2 |        0 |    100% |           |
| src/canopy/magpie/services/\_\_init\_\_.py           |        0 |        0 |    100% |           |
| src/canopy/magpie/services/config\_service.py        |       97 |        8 |     92% |49, 118-121, 124, 201-202 |
| src/canopy/main.py                                   |       89 |        0 |    100% |           |
| src/canopy/mole/\_\_init\_\_.py                      |        0 |        0 |    100% |           |
| src/canopy/mole/adapters/\_\_init\_\_.py             |        1 |        0 |    100% |           |
| src/canopy/mole/adapters/appliance.py                |      166 |       13 |     92% |55, 171, 215-218, 259, 266-267, 281-282, 293-294 |
| src/canopy/mole/adapters/base.py                     |       62 |        7 |     89% |83, 117, 128, 145, 158, 166, 170 |
| src/canopy/mole/adapters/registry.py                 |       69 |        6 |     91% |61-69, 84-85 |
| src/canopy/mole/api/\_\_init\_\_.py                  |        0 |        0 |    100% |           |
| src/canopy/mole/api/v1/\_\_init\_\_.py               |        0 |        0 |    100% |           |
| src/canopy/mole/api/v1/archivers.py                  |        8 |        0 |    100% |           |
| src/canopy/mole/api/v1/data.py                       |       37 |        2 |     95% |    54, 57 |
| src/canopy/mole/api/v1/health.py                     |       10 |        0 |    100% |           |
| src/canopy/mole/api/v1/metadata.py                   |       11 |        0 |    100% |           |
| src/canopy/mole/api/v1/router.py                     |       12 |        0 |    100% |           |
| src/canopy/mole/api/v1/search.py                     |       10 |        0 |    100% |           |
| src/canopy/mole/app.py                               |       27 |        0 |    100% |           |
| src/canopy/mole/config.py                            |       12 |        0 |    100% |           |
| src/canopy/mole/dependencies.py                      |        7 |        1 |     86% |        15 |
| src/canopy/mole/gateway.py                           |       25 |        6 |     76% |76-85, 90, 100-101, 106 |
| src/canopy/mole/runtime.py                           |        4 |        0 |    100% |           |
| src/canopy/mole/schemas/\_\_init\_\_.py              |        0 |        0 |    100% |           |
| src/canopy/mole/schemas/archiver.py                  |       10 |        0 |    100% |           |
| src/canopy/mole/schemas/pv.py                        |       12 |        0 |    100% |           |
| src/canopy/mole/schemas/search.py                    |        2 |        0 |    100% |           |
| src/canopy/mole/schemas/series.py                    |       17 |        0 |    100% |           |
| src/canopy/mole/services/\_\_init\_\_.py             |        0 |        0 |    100% |           |
| src/canopy/mole/services/archiver\_service.py        |       72 |        0 |    100% |           |
| src/canopy/raccoon/\_\_init\_\_.py                   |        1 |        0 |    100% |           |
| src/canopy/raccoon/adapters/\_\_init\_\_.py          |        0 |        0 |    100% |           |
| src/canopy/raccoon/adapters/base.py                  |       42 |        3 |     93% |96, 127, 131 |
| src/canopy/raccoon/adapters/meme.py                  |       84 |       12 |     86% |66-67, 76-77, 86, 98, 134, 136, 138, 151, 153, 155 |
| src/canopy/raccoon/adapters/registry.py              |       45 |        4 |     91% |42-43, 69, 84 |
| src/canopy/raccoon/api/\_\_init\_\_.py               |        0 |        0 |    100% |           |
| src/canopy/raccoon/api/v1/\_\_init\_\_.py            |        0 |        0 |    100% |           |
| src/canopy/raccoon/api/v1/devices.py                 |       11 |        0 |    100% |           |
| src/canopy/raccoon/api/v1/elements.py                |       11 |        0 |    100% |           |
| src/canopy/raccoon/api/v1/health.py                  |       10 |        0 |    100% |           |
| src/canopy/raccoon/api/v1/pvs.py                     |       17 |        0 |    100% |           |
| src/canopy/raccoon/api/v1/router.py                  |       10 |        0 |    100% |           |
| src/canopy/raccoon/app.py                            |       36 |        1 |     97% |        46 |
| src/canopy/raccoon/cache/\_\_init\_\_.py             |        0 |        0 |    100% |           |
| src/canopy/raccoon/cache/flight.py                   |       32 |        0 |    100% |           |
| src/canopy/raccoon/cache/keys.py                     |       11 |        0 |    100% |           |
| src/canopy/raccoon/cache/name\_cache.py              |       39 |        1 |     97% |        73 |
| src/canopy/raccoon/cache/pattern\_cache.py           |       38 |        1 |     97% |        63 |
| src/canopy/raccoon/config.py                         |       18 |        0 |    100% |           |
| src/canopy/raccoon/dependencies.py                   |        7 |        1 |     86% |        15 |
| src/canopy/raccoon/gateway.py                        |       22 |        0 |    100% |           |
| src/canopy/raccoon/runtime.py                        |        4 |        0 |    100% |           |
| src/canopy/raccoon/schemas/\_\_init\_\_.py           |        0 |        0 |    100% |           |
| src/canopy/raccoon/schemas/directory.py              |       10 |        0 |    100% |           |
| src/canopy/raccoon/services/\_\_init\_\_.py          |        0 |        0 |    100% |           |
| src/canopy/raccoon/services/directory\_service.py    |      107 |        5 |     95% |83, 136, 192-198 |
| src/canopy/squirrel/\_\_init\_\_.py                  |        0 |        0 |    100% |           |
| src/canopy/squirrel/adapters/\_\_init\_\_.py         |        0 |        0 |    100% |           |
| src/canopy/squirrel/adapters/epics\_io.py            |       84 |        1 |     99% |       172 |
| src/canopy/squirrel/api/\_\_init\_\_.py              |        0 |        0 |    100% |           |
| src/canopy/squirrel/api/v1/\_\_init\_\_.py           |        0 |        0 |    100% |           |
| src/canopy/squirrel/api/v1/\_validation.py           |        7 |        0 |    100% |           |
| src/canopy/squirrel/api/v1/health.py                 |       28 |        0 |    100% |           |
| src/canopy/squirrel/api/v1/pvs.py                    |       79 |        6 |     92% |109, 117, 142, 150, 157, 165 |
| src/canopy/squirrel/api/v1/router.py                 |       10 |        0 |    100% |           |
| src/canopy/squirrel/api/v1/snapshots.py              |       55 |        5 |     91% |141, 144, 152, 154, 181 |
| src/canopy/squirrel/api/v1/tags.py                   |       93 |       19 |     80% |55-60, 73, 75, 88, 91, 99, 102, 110, 113, 149-151, 163-166 |
| src/canopy/squirrel/app.py                           |       17 |        0 |    100% |           |
| src/canopy/squirrel/config.py                        |       15 |        1 |     93% |        60 |
| src/canopy/squirrel/data/\_\_init\_\_.py             |        0 |        0 |    100% |           |
| src/canopy/squirrel/data/models.py                   |       62 |        0 |    100% |           |
| src/canopy/squirrel/data/repositories.py             |      270 |       39 |     86% |98, 121-127, 230, 253-268, 421-432, 467, 479-480, 533-534, 540-542, 558-560, 600, 630-634 |
| src/canopy/squirrel/dependencies.py                  |       15 |        0 |    100% |           |
| src/canopy/squirrel/errors.py                        |       26 |        0 |    100% |           |
| src/canopy/squirrel/gateway.py                       |       12 |        0 |    100% |           |
| src/canopy/squirrel/restore\_hooks/\_\_init\_\_.py   |        4 |        0 |    100% |           |
| src/canopy/squirrel/restore\_hooks/base.py           |        7 |        0 |    100% |           |
| src/canopy/squirrel/restore\_hooks/magnet\_trim.py   |       24 |        0 |    100% |           |
| src/canopy/squirrel/restore\_hooks/registry.py       |       52 |        0 |    100% |           |
| src/canopy/squirrel/restore\_hooks/worker\_status.py |       15 |        0 |    100% |           |
| src/canopy/squirrel/restore\_hooks/write\_to\_con.py |       11 |        0 |    100% |           |
| src/canopy/squirrel/runtime.py                       |        4 |        0 |    100% |           |
| src/canopy/squirrel/schemas/\_\_init\_\_.py          |        0 |        0 |    100% |           |
| src/canopy/squirrel/schemas/health.py                |       15 |        0 |    100% |           |
| src/canopy/squirrel/schemas/meta\_pv.py              |       10 |        0 |    100% |           |
| src/canopy/squirrel/schemas/pv.py                    |       30 |        0 |    100% |           |
| src/canopy/squirrel/schemas/snapshot.py              |       45 |        0 |    100% |           |
| src/canopy/squirrel/schemas/tag.py                   |       26 |        0 |    100% |           |
| src/canopy/squirrel/services/\_\_init\_\_.py         |        0 |        0 |    100% |           |
| src/canopy/squirrel/services/bulk\_insert.py         |       36 |        2 |     94% |    88, 99 |
| src/canopy/squirrel/services/pv\_service.py          |      173 |       27 |     84% |176, 190-191, 248-249, 269-270, 276, 278, 282, 284, 286-289, 299-303, 309-310, 333-336, 341, 347, 361, 386 |
| src/canopy/squirrel/services/snapshot\_service.py    |      381 |       29 |     92% |129-134, 146, 148, 151, 155-157, 277-279, 435, 477-478, 483, 490, 547, 614, 626, 643, 832, 878-879, 881-882 |
| src/canopy/squirrel/services/tag\_service.py         |       79 |       18 |     77% |59-70, 75, 94, 114, 118, 121, 128 |
| src/canopy/squirrel/tasks.py                         |       51 |       29 |     43% |34-35, 55-57, 64-79, 84-102 |
| src/canopy/vole/\_\_init\_\_.py                      |        0 |        0 |    100% |           |
| src/canopy/vole/adapter.py                           |      131 |       48 |     63% |116-167, 181, 198, 202, 208, 215-216 |
| src/canopy/vole/api/\_\_init\_\_.py                  |        0 |        0 |    100% |           |
| src/canopy/vole/api/v1/\_\_init\_\_.py               |        0 |        0 |    100% |           |
| src/canopy/vole/api/v1/debug.py                      |       12 |        0 |    100% |           |
| src/canopy/vole/api/v1/health.py                     |       14 |        0 |    100% |           |
| src/canopy/vole/api/v1/pvs.py                        |       50 |        3 |     94% |43, 46, 96 |
| src/canopy/vole/api/v1/router.py                     |        8 |        0 |    100% |           |
| src/canopy/vole/app.py                               |       56 |        2 |     96% |     40-41 |
| src/canopy/vole/buffer/\_\_init\_\_.py               |        0 |        0 |    100% |           |
| src/canopy/vole/buffer/ring.py                       |       96 |        3 |     97% |95, 133-134 |
| src/canopy/vole/buffer/store.py                      |       44 |        1 |     98% |        61 |
| src/canopy/vole/config.py                            |       10 |        0 |    100% |           |
| src/canopy/vole/dependencies.py                      |       11 |        1 |     91% |        11 |
| src/canopy/vole/errors.py                            |       17 |        0 |    100% |           |
| src/canopy/vole/gateway.py                           |       56 |       20 |     64% |40-42, 47, 54, 61, 70, 74, 88, 100, 104, 111, 119, 128-132, 137, 148 |
| src/canopy/vole/runtime.py                           |        6 |        0 |    100% |           |
| src/canopy/vole/schemas/\_\_init\_\_.py              |        0 |        0 |    100% |           |
| src/canopy/vole/schemas/buffer.py                    |       26 |        1 |     96% |        38 |
| src/canopy/vole/services/\_\_init\_\_.py             |        0 |        0 |    100% |           |
| src/canopy/vole/services/alignment\_service.py       |      149 |       25 |     83% |86, 95, 100, 128, 187, 202-220, 232, 264, 266, 269-272 |
| src/canopy/vole/services/buffer\_service.py          |      115 |       10 |     91% |51-53, 63, 75, 77, 89, 194, 204, 248 |
| src/canopy/vole/services/eviction\_service.py        |       28 |        2 |     93% |    33, 40 |
| src/canopy/vole/services/grace\_manager.py           |       45 |        0 |    100% |           |
| src/canopy/vole/services/hare\_ingest.py             |       92 |       11 |     88% |47-48, 55, 77, 103, 121, 131-132, 152-155 |
| src/canopy/vole/services/notifier.py                 |       35 |        2 |     94% |     33-34 |
| src/canopy/worker.py                                 |       37 |        6 |     84% | 32, 61-65 |
| **TOTAL**                                            | **8206** |  **719** | **91%** |           |


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