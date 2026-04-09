CC_US_CAN = "rfoster@amcastle.com; jchrestman@amcastle.com; RROCKWELL@amcastle.com; mreilly@amcastle.com"
CC_INTL_MX = "rfoster@amcastle.com; jchrestman@amcastle.com; RROCKWELL@amcastle.com; imendieta@amcastle.com; rhernandez@amcastle.com; sgarcia@amcastle.com; mreilly@amcastle.com"
CC_INTL    = "rfoster@amcastle.com; jchrestman@amcastle.com; RROCKWELL@amcastle.com; mreilly@amcastle.com"

# Keys must match org codes in analytics_intermediate.int_castle__productivity_03_employee_payouts
# active=False → file is generated but email is not sent
BRANCHES = {
    "ASC": {"to": "cfseetoh@amcastle.com; pwang@amcastle.com; hgao@amcastle.com",   "cc": CC_INTL,    "greeting": "Hi Seetoh/Peter/Hannah,", "active": True},
    "ATL": {"to": "whendry@amcastle.com",                                            "cc": CC_US_CAN,  "greeting": "Hi William,",             "active": True},
    "CHA": {"to": "lwilliams2@amcastle.com; llatham@amcastle.com",                   "cc": CC_US_CAN,  "greeting": "Hi Luke/Lauren,",         "active": False},
    "CLE": {"to": "trobinson@amcastle.com; lmiller@amcastle.com",                    "cc": CC_US_CAN,  "greeting": "Hi Len/Tim,",             "active": True},
    "DAL": {"to": "DLege@amcastle.com; ddecanio@amcastle.com",                       "cc": CC_US_CAN,  "greeting": "Hi Doug/Danielle,",       "active": True},
    "ENA": {"to": "poger@amcastle.com; droges@amcastle.com",                         "cc": CC_INTL,    "greeting": "Hi Pierre/David,",        "active": True},
    "ENT": {"to": "poger@amcastle.com; droges@amcastle.com",                         "cc": CC_INTL,    "greeting": "Hi Pierre/David,",        "active": True},
    "HAI": {"to": "jkoeppen@amcastle.com",                                           "cc": CC_US_CAN,  "greeting": "Hi Jesse,",               "active": True},
    "JVL": {"to": "rojones@amcastle.com",                                            "cc": CC_US_CAN,  "greeting": "Hi Rob/Kevin,",           "active": True},
    "LOS": {"to": "shage@amcastle.com",                                              "cc": CC_US_CAN,  "greeting": "Hi Salim,",               "active": True},
    "MCH": {"to": "asantiago@amcastle.com; crodriguez@amcastle.com",                 "cc": CC_INTL_MX, "greeting": "Hi Alonso/Carlos,",       "active": True},
    "MTY": {"to": "rlozano@amcastle.com; crodriguez@amcastle.com",                   "cc": CC_INTL_MX, "greeting": "Hi Raul/Carlos,",         "active": True},
    "MXM": {"to": "magonzalez@amcastle.com; crodriguez@amcastle.com",                "cc": CC_INTL_MX, "greeting": "Hi Miguel/Carlos,",       "active": True},
    "MXQ": {"to": "agarcia@amcastle.com; crodriguez@amcastle.com; rcruz@amcastle.com","cc": CC_INTL_MX, "greeting": "Hi Adad/Carlos/Ricardo,", "active": True},
    "PHI": {"to": "ddixon@amcastle.com; skilduff@amcastle.com",                      "cc": CC_US_CAN,  "greeting": "Hi Shane/Dan,",           "active": False},
    "SGP": {"to": "cfseetoh@amcastle.com; lyap@amcastle.com",                        "cc": CC_INTL,    "greeting": "Hi Seetoh/Lina,",         "active": True},
    "STO": {"to": "gcosio@amcastle.com; shage@amcastle.com",                         "cc": CC_US_CAN,  "greeting": "Hi Gus/Salim,",           "active": True},
    "TOR": {"to": "dmbrown@amcastle.com; gdedieu@amcastle.com",                      "cc": CC_US_CAN,  "greeting": "Hi Doug,",                "active": True},
    "WIE": {"to": "hgarcia@amcastle.com",                                            "cc": CC_US_CAN,  "greeting": "Hi Henry,",               "active": True},
    "WIN": {"to": "dsimpson@amcastle.com; astefanson@amcastle.com",                  "cc": CC_US_CAN,  "greeting": "Hi Dave/Andrew,",         "active": False},
}
