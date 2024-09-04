dashboard_export <- jsonlite::toJSON(
  list(
    provider = cfg$dashboard_provider,
    dashboard_dataset_version = "0.5.2",
    exporttimestamp = as.numeric(Sys.time()),
    author = cfg$dashboard_author,
    dataitems = list(
      list(
        itemname = "kira.cumulative.diags.zipcode.all ",
        itemtype = "list",
        data = list(
          sort(df_result$Patient.PLZ)
        )
      ),
      list(
        itemname = "kira.cumulative.diags.age.disorders",
        itemtype = "stackedbarcharts",
        data = list(
          charts = list(
            "all_icd_codes"
          ),
          stacks = list(
            "all_patients",
          ),
          bars = list(
            df_result_agg_age$Alter.Gruppe
          ),
          values = list(
            df_result_agg_age$Anzahl
          )
        )
      ),
      list(
        itemname = "kira.timeline.diags.icdcodes",
        itemtype = "stackedbarcharts",
        data = list(
          charts = list(
            "all_icd_codes"
          ),
          bars = list(
            2016:2024
          ),
          stacks = list(
            "J20.5: Akute RSV-Bronchitis",
            "J21.0: Akute RSV-Bronchiolitis",
            "J12.1: RSV-Pneumonie",
            "B97.4!: RSV als Krankheitsursache"
          ),
          values = list(
            t(df_json_values)
          )
        )
      )
    )
  )
  , pretty = TRUE)
      
      
      
      {
        "itemname": "kira.timeline.diags.icdcodes",
        "itemtype": "stackedbarcharts",
        "data": {
          "charts": [
            "all_icd_codes"
          ],
          "bars": [
            [
              "2016",
              "2017",
              "…",
              "yyyy"
            ]
          ],
          "stacks": [
            [
              "acute_rsv_bronchitis_j20.5",
              "acute_rsv_bronchiolitis_j21.0",
              "rsv_pneumonia_j12.1",
              "rsv_caused_disease_b97.4"
            ]
          ],
          "values": [
            [
              [
                0,
                0,
                0,
                0
              ],
              [
                0,
                0,
                0,
                0
              ],
              "…",
              [
                0,
                0,
                0,
                0
              ]
            ]
          ]
        }
      }
      
      
    )
    
    
    {
      "itemname": "kira.cumulative.diags.age.disorders",
      "itemtype": "stackedbarcharts",
      "data": {
        "charts": [
          "all_icd_codes"
        ],
        "bars": [
          [
            "age_<3",
            "age_3<6",
            "age_6<9",
            "age_9<12",
            "age_12<15",
            "age_15<18",
            "age_18+"
          ]
        ],
        "stacks": [
          [
            "affective_disorders",
            "anxiety_disorders",
            "attachment_disorders",
            "eating_disorders",
            "neuronal_development_disorders",
            "pathological_media_consumptions",
            "personality_disorders"
          ]
        ],
        "values": [
          [
            [
              0,
              0,
              0,
              0,
              0,
              0,
              0
            ],
            [
              0,
              0,
              0,
              0,
              0,
              0,
              0
            ],
            [
              0,
              0,
              0,
              0,
              0,
              0,
              0
            ],
            [
              0,
              0,
              0,
              0,
              0,
              0,
              0
            ],
            [
              0,
              0,
              0,
              0,
              0,
              0,
              0
            ],
            [
              0,
              0,
              0,
              0,
              0,
              0,
              0
            ],
            [
              0,
              0,
              0,
              0,
              0,
              0,
              0
            ]
          ]
        ]
      }
    },
    
    {
      "itemname": "kira.timeline.diags.icdcodes",
      "itemtype": "stackedbarcharts",
      "data": {
        "charts": [
          "all_icd_codes"
        ],
        "bars": [
          [
            "2016",
            "2017",
            "…",
            "yyyy"
          ]
        ],
        "stacks": [
          [
            "J20.5: Akute RSV-Bronchitis",
            "J21.0: Akute RSV-Bronchiolitis",
            "J12.1: RSV-Pneumonie",
            "B97.4!: RSV als Krankheitsursache"
          ]
        ],
        "values": [
          [
            [
              0,
              0,
              0,
              0
            ],
            [
              0,
              0,
              0,
              0
            ],
            "…",
            [
              0,
              0,
              0,
              0
            ]
          ]
        ]
      }
    },
    
    
    
    
    
    itemname = "timeline.coverchild.diags.icdcodes",
    itemtype = "stackedbarcharts",
    data = list(
      charts = list("allicdcodes"),
      bars = 2016:2024,
      stacks = list(
        "J20.5: Akute RSV-Bronchitis",
        "J21.0: Akute RSV-Bronchiolitis",
        "J12.1: RSV-Pneumonie",
        "B97.4!: RSV als Krankheitsursache"
      ),
      values = list(
        t(df_json_values)
      )
    )
  ),
  pretty = TRUE) 