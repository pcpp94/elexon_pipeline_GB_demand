### Getting data functions

import pandas as pd
import holidays

def get_xlsx_files(directory_path):
  """recursively list path of all csv files in path directory """
  xlsx_files = []
  files_to_treat = dbutils.fs.ls(directory_path)
  while files_to_treat:
    path = files_to_treat.pop(0).path
    if path.endswith('/'):
      files_to_treat += dbutils.fs.ls(path)
    elif path.endswith('.xlsx'):
      xlsx_files.append(path)
  for i in range(len(xlsx_files)):
    xlsx_files[i] = xlsx_files[i].replace('dbfs:','/dbfs')
  return xlsx_files

def get_xls_files(directory_path):
  """recursively list path of all csv files in path directory """
  xls_files = []
  files_to_treat = dbutils.fs.ls(directory_path)
  while files_to_treat:
    path = files_to_treat.pop(0).path
    if path.endswith('/'):
      files_to_treat += dbutils.fs.ls(path)
    elif path.endswith('.xls'):
      xls_files.append(path)
  for i in range(len(xls_files)):
    xls_files[i] = xls_files[i].replace('dbfs:','/dbfs')
  return xls_files

def get_csv_files(directory_path):
  """recursively list path of all csv files in path directory """
  csv_files = []
  files_to_treat = dbutils.fs.ls(directory_path)
  while files_to_treat:
    path = files_to_treat.pop(0).path
    if path.endswith('/'):
      files_to_treat += dbutils.fs.ls(path)
    elif path.endswith('.csv'):
      csv_files.append(path)
  for i in range(len(csv_files)):
    csv_files[i] = csv_files[i].replace('dbfs:','/dbfs')
  return csv_files


def get_holidays_calendar():
  calendar = pd.DataFrame(index=holidays.GB(years=list(range(2015,2050))).keys(), columns=['holiday'], data=holidays.UK(years=list(range(2015,2050))).values()).reset_index().rename(columns={'index':'Date', 'holiday' : 'BH'})
  calendar['Date'] = pd.to_datetime(calendar['Date'])
  calendar['WD'] = 'NWD'
  holidays_df_to_add = calendar[calendar['BH'].str.contains('England')]
  holidays_df_to_add_2 = calendar[calendar['BH'].str.contains("New Year's Day ")]
  calendar = calendar[~calendar['BH'].str.contains('Scotland')]
  calendar = calendar[~calendar['BH'].str.contains('Northern Ireland')]
  calendar = pd.concat([calendar, holidays_df_to_add, holidays_df_to_add_2])
  calendar.rename(columns={"Date":"WD_Date"},inplace=True)
  return calendar


### Functions used like this (instead of a lambda) because there was a problem when using Koalas (Before Pyspark.pandas came out)

def peak_func_from_sp(x) -> 'str':
  if x >= 33 and x <= 38:
    return 'P'
  else:
    return 'N'

def wd_ok(x,y) -> 'string':
  if x != 1:
    return y
  else:
    return x

def string_to_numeric(x) -> 'int':
  if x == "W":
    return 0
  else:
    return 1

def numeric_to_string(x) -> 'str':
  if x == 0:
    return "WD"
  else:
    return "NWD"  

def peak_func(x) -> 'str':
  if x >= 33 and x <= 38:
    return 'P'
  else:
    return 'N'

def aux_date_func(x) -> 'int':
  if (x.dayofweek+1) <=5:
    return 0
  else:
    1  

### Dictionaries

sr_to_int = {
    'II' : 1,
    'SF' : 2,
    'R1' : 3,
    'R2' : 4,
    'R3' : 5,
    'RF' : 6,
    'DF' : 7    
}

ro_fit_sr_dic = {
  'II' : 0,
  'SF' : 1,
  'R1' : 2,
  'R2' : 3,
  'R3' : 4
}

RO_dic = {1: 3,
    2: 2,
    3: 2,
    4: 4,
    5: 4,
    6: 4,
    7: 4,
    8: 4,
    9: 4,
    10: 4,
    11: 3,
    12: 3}

FIT_dic = {1: 3,
     2: 3,
     3: 2,
     4: 4,
     5: 4,
     6: 4,
     7: 4,
     8: 4,
     9: 4,
     10: 4,
     11: 4,
     12: 3}

ccc_imports_dic = {
    1 : ['Above 100kW','Non-domestic','A'],
    2 : ['Unmetered','Non-domestic','A'],
    3 : ['Above 100kW','Non-domestic','A'],
    4 : ['Above 100kW','Non-domestic','A'],
    5 : ['Unmetered','Non-domestic','A'],
    9 : ['Above 100kW','Non-domestic','E'],
    10 : ['Unmetered','Non-domestic','E'],
    11 : ['Above 100kW','Non-domestic','E'],
    12 : ['Above 100kW','Non-domestic','E'],
    13 : ['Unmetered','Non-domestic','E'],
    23 : ['Below 100kW','Non-domestic','A'],
    25 : ['Below 100kW','Non-domestic','A'],
    26 : ['Below 100kW','Non-domestic','A'],
    28 : ['Below 100kW','Non-domestic','E'],
    30 : ['Below 100kW','Non-domestic','E'],
    31 : ['Below 100kW','Non-domestic','E'],
    42 : ['Below 100kW','Domestic','A'],
    43 : ['Below 100kW','Domestic','A'],
    44 : ['Below 100kW','Domestic','A'],
    45 : ['Below 100kW','Domestic','E'],
    46 : ['Below 100kW','Domestic','E'],
    47 : ['Below 100kW','Domestic','E'],
    54 : ['Below 100kW','Non-domestic','A'],
    55 : ['Below 100kW','Non-domestic','A'],
    56 : ['Below 100kW','Non-domestic','A'],
    57 : ['Below 100kW','Non-domestic','E'],
    58 : ['Below 100kW','Non-domestic','E'],
    59 : ['Below 100kW','Non-domestic','E']}

ccc_exports_dic = dic = {
    6 : ["Above 100kW", "Non-domestic", "A"],
    7 : ["Above 100kW", "Non-domestic", "A"],
    8 : ["Above 100kW", "Non-domestic", "A"],
    14 : ["Above 100kW", "Non-domestic", "E"],
    15 : ["Above 100kW", "Non-domestic", "E"],
    16 : ["Above 100kW", "Non-domestic", "E"],
    32 : ["NHH", "Non-domestic", "E"],
    33 : ["NHH", "Non-domestic", "A"],
    34 : ["NHH", "Non-domestic", "E"],
    35 : ["NHH", "Non-domestic", "A"],
    36 : ["Below 100kW", "Non-domestic", "A"],
    37 : ["Below 100kW", "Non-domestic", "A"],
    38 : ["Below 100kW", "Non-domestic", "A"],
    39 : ["Below 100kW", "Non-domestic", "E"],
    40 : ["Below 100kW", "Non-domestic", "E"],
    41 : ["Below 100kW", "Non-domestic", "E"],
    48 : ["Below 100kW", "Domestic", "A"],
    49 : ["Below 100kW", "Domestic", "A"],
    50 : ["Below 100kW", "Domestic", "A"],
    51 : ["Below 100kW", "Domestic", "E"],
    52 : ["Below 100kW", "Domestic", "E"],
    53 : ["Below 100kW", "Domestic", "E"],
    60 : ["Below 100kW", "Non-domestic", "A"],
    61 : ["Below 100kW", "Non-domestic", "A"],
    62 : ["Below 100kW", "Non-domestic", "A"],
    63 : ["Below 100kW", "Non-domestic", "E"],
    64 : ["Below 100kW", "Non-domestic", "E"],
    65 : ["Below 100kW", "Non-domestic", "E"]}

s014_col_names = {'SettlementDate' : 'settlement_date',
  'SettlementRunType' : 'settlement_run_type',
  'SettlementPeriod' : 'settlement_period',
  'BMUnitId' : 'bm_unit_id',
  'TradingUnitName' : 'trading_unit_name',
  'PeriodFPN' : 'period_fpn',
  'BMUnitMeteredVolume' : 'metered_volume',
  'TransmissionLossMultiplier' : 'tlm',
  'BMUnitApplicableBalancingServicesVolume' : 'balancing_volume',
  'ND' : 'nd',
  'player' : 'player',
  'bsc_party' : 'bsc_party',
  'enapp_name' : 'enapp_name',
  'ngc_name' : 'ngc_name',
  'ngc_bmu_id' : 'ngc_bmu_id',
  'ngc_bmu_flag' : 'ngc_bmu_flag',
  'enapp_class' : 'enapp_class',
  'enapp_category' : 'enapp_category',
  'all_notify' : 'all_notify',
  'ImportExportIndicator' : 'import_export_indicator'
  }


# BMU Dictionary related:

bmu_category_dic = {"enapp_class" :['BIOMASS','BRITNEDICD','BRITNEDICG','CHEMICAL','COALGAS_OPT_OUT','COAL_OPT_IN','COAL_OPT_OUT','COALOIL_OPT_OUT','CCGT','CHP','DEMAND','DSR','DIESEL','DIESELGAS','EASTWESTICD','EASTWESTICG','ELECICD','ELECICG','BATTERY','EFW','FRENCHICD','FRENCHICG','GAS','GASOIL','HYDRO','IFA2ICD','IFA2ICG','INERTIA','MANN','SOLAR','MOYLEICD','MOYLEICG','NEMOICD','NEMOICG','NSLICD','NSLICG','NUCLEAR','OIL_OPT_OUT','CHIX','PUMP','RAIL','SCOTICD','SCOTICG','STX','STEEL','SVA_AGG','SVA_A','SVA_B','SVA_C','SVA_D','SVA_E','SVA_F','SVA_P','SVA_G','SVA_J','SVA_H','SVA_N','SVA_K','SVA_L','SVA_M','WIND','NONE'],"enapp_category" :['power_station','interconnector','interconnector','demand','power_station','power_station','power_station','power_station','power_station','power_station','demand','demand_side_response','power_station','power_station','interconnector','interconnector','interconnector','interconnector','storage','power_station','interconnector','interconnector','power_station','power_station','power_station','interconnector','interconnector','power_station','interconnector','power_station','interconnector','interconnector','interconnector','interconnector','interconnector','interconnector','power_station','power_station','power_station','storage','demand','interconnector','interconnector','station_load','demand','demand','demand','demand','demand','demand','demand','demand','demand','demand','demand','demand','demand','demand','demand','demand','power_station','other']}

bmu_types = ['BIOMASS',	'BRITNEDICD',	'BRITNEDICG',	'CHEMICAL',	'COALGAS_OPT_OUT',	'COAL_OPT_IN',	'COAL_OPT_OUT',	'COALOIL_OPT_OUT',	'CCGT',	'CHP',	'DEMAND',	'DSR',	'DIESEL',	'DIESELGAS',	'EASTWESTICD',	'EASTWESTICG',	'ELECICD',	'ELECICG',	'BATTERY',	'EFW',	'FRENCHICD',	'FRENCHICG',	'GAS',	'GASOIL',	'HYDRO',	'IFA2ICD',	'IFA2ICG',	'INERTIA',	'MANN',	'SOLAR',	'MOYLEICD',	'MOYLEICG',	'NEMOICD',	'NEMOICG',	'NSLICD',	'NSLICG',	'NUCLEAR',	'OIL_OPT_OUT',	'CHIX',	'PUMP',	'RAIL',	'SCOTICD',	'SCOTICG',	'STX',	'STEEL',	'SVA_AGG',	'SVA_A',	'SVA_B',	'SVA_C',	'SVA_D',	'SVA_E',	'SVA_F',	'SVA_P',	'SVA_G',	'SVA_J',	'SVA_H',	'SVA_N',	'SVA_K',	'SVA_L',	'SVA_M',	'WIND']

ngc_types = ['BATTERY',	'BIOMASS',	'INTNED',	'COAL',	'CCGT',	'DSR',	'INTEW',	'INTELEC',	'EFW',	'INTFR',	'GASENG',	'INTIFA2',	'INERTIA',	'INTIRL',	'INTNEM',	'NPSHYD',	'INTNSL',	'NA',	'NUCLEAR',	'OIL',	'OCGT',	'OTHER',	'PS',	'INTSC',	'WIND']

bmu_class = ['ALLNOTIFY',	'NOTIFYNOTSVA',	'NOTIFYCVA',	'NOTIFYSVA',	'EMBEDDEDCVA',	'DIRECTCONCVA',	'INTERCONS',	'SVAONLY']

fields_enapp_dic = ['bmu_id', 'player',	'bsc_party',	'ngc_bmu_id',	'ngc_bmu_flag',	'last_flag',	'currently_active',	'effective_from_date',	'effective_to_date',	'generation_capacity',	'demand_capacity',	'enapp_name',	'enapp_class',	'neta_type',	'neta_name',	'ngc_type',	'ngc_name',	'power_station_id',	'power_station_name',	'all_notify',	'notify_not_sva',	'notify_cva',	'notify_sva',	'embedded_cva',	'directcon_cva',	'intercons',	'sva_only']


standarize_dic = {'bmu_id' : 'bmu_id',
  'bscparty' : 'bsc_party',
  'effectivefromdate' : 'effective_from_date',
  'effectivetodate' : 'effective_to_date',
  'neta_type' : 'neta_type',
  'neta_name' : 'neta_name',
  'ngc_type' : 'ngc_type',
  'ngc_name' : 'ngc_name',
  'power_station_id' : 'power_station_id',
  'power_station_name' : 'power_station_name',
  'ALLNOTIFY' : 'all_notify',
  'NOTIFYNOTSVA' : 'notify_not_sva',
  'NOTIFYCVA' : 'notify_cva',
  'NOTIFYSVA' : 'notify_sva',
  'EMBEDDEDCVA' : 'embedded_cva',
  'DIRECTCONCVA' : 'directcon_cva',
  'INTERCONS' : 'intercons',
  'settlement_date' : 'settlement_date',
  'SVAONLY' : 'sva_only',
  'generation_capacity' : 'generation_capacity',
  'demand_capacity' : 'demand_capacity',
  'name' : 'enapp_name',
  'fueltype' : 'enapp_class',
  'ngcbmunitid' : 'ngc_bmu_id'}

neta_name_dic = {'BIOMASS' : 'Biomass',
  'BRITNEDICD' : 'Britned Interconnector Demand',
  'BRITNEDICG' : 'Britned Interconnector Generation',
  'CHEMICAL' : 'Chemical Works Demand',
  'COALGAS_OPT_OUT' : 'Coal/Gas (LCPD Opt-Out)',
  'COAL_OPT_IN' : 'Coal (LCPD Opt-In)',
  'COAL_OPT_OUT' : 'Coal (LCPD Opt-Out)',
  'COALOIL_OPT_OUT' : 'Coal/Oil (LCPD Opt-Out)',
  'CCGT' : 'Combined Cycle Gas Turbine',
  'CHP' : 'Combined Heat and Power',
  'DEMAND' : 'Demand',
  'DSR' : 'Demand Side Response',
  'DIESEL' : 'Diesel',
  'DIESELGAS' : 'Diesel/Gas',
  'EASTWESTICD' : 'East-West Interconnector Demand',
  'EASTWESTICG' : 'East-West Interconnector Generation',
  'ELECICD' : 'Eleclink Interconnector Demand',
  'ELECICG' : 'Eleclink Interconnector Generation',
  'BATTERY' : 'Electro-Chemical Battery',
  'EFW' : 'Energy from Waste',
  'FRENCHICD' : 'French Interconnector - Demand',
  'FRENCHICG' : 'French Interconnector - Generation',
  'GAS' : 'Gas',
  'GASOIL' : 'Gas/Oil',
  'HYDRO' : 'Hydro',
  'IFA2ICD' : 'IFA 2 Interconnector Demand',
  'IFA2ICG' : 'IFA 2 Interconnector Generation',
  'INERTIA' : 'Inertia Provider',
  'MANN' : 'Isle of Man',
  'SOLAR' : 'Large Solar',
  'MOYLEICD' : 'Moyle Interconnector - Demand',
  'MOYLEICG' : 'Moyle Interconnector - Generation',
  'NEMOICD' : 'NEMO Interconnector Demand',
  'NEMOICG' : 'NEMO Interconnector Generation',
  'NSLICD' : 'North Sea Link Interconnector Demand',
  'NSLICG' : 'North Sea Link Interconnector Generator',
  'NUCLEAR' : 'Nuclear',
  'OIL_OPT_OUT' : 'Oil (LCPD Opt-Out)',
  'CHIX' : 'Poultry Litter',
  'PUMP' : 'Pumped Storage',
  'RAIL' : 'Railway Demand',
  'SCOTICD' : 'Scottish Interconnector - Demand',
  'SCOTICG' : 'Scottish Interconnector - Generation',
  'STX' : 'Station Transformer / Demand',
  'STEEL' : 'Steelworks Demand',
  'SVA_AGG' : 'SVA Aggregate BM Unit',
  'SVA_A' : 'SVA Container - Eastern GSP Group',
  'SVA_B' : 'SVA Container - East Midlands GSP Group',
  'SVA_C' : 'SVA Container - London GSP Group',
  'SVA_D' : 'SVA Container - Merseyside and North Wales GSP Group',
  'SVA_E' : 'SVA Container - Midlands GSP Group',
  'SVA_F' : 'SVA Container - Northern GSP Group',
  'SVA_P' : 'SVA Container - North Scotland GSP Group',
  'SVA_G' : 'SVA Container - North Western GSP Group',
  'SVA_J' : 'SVA Container - South Eastern GSP Group',
  'SVA_H' : 'SVA Container - Southern GSP Group',
  'SVA_N' : 'SVA Container - South Scotland GSP Group',
  'SVA_K' : 'SVA Container - South Wales GSP Group',
  'SVA_L' : 'SVA Container - South Western GSP Group',
  'SVA_M' : 'SVA Container - Yorkshire GSP Group',
  'WIND' : 'Wind'}

ngc_name_dic = {'BATTERY' : 'Battery',
'BIOMASS' : 'Biomass',
'INTNED' : 'BritNed Interconnector',
'COAL' : 'Coal',
'CCGT' : 'Combined Cycle Gas Turbine',
'DSR' : 'Demand Side Response',
'INTEW' : 'East-West Interconnector',
'INTELEC' : 'Eleclink Interconnector',
'EFW' : 'Energy From Waste',
'INTFR' : 'French Interconnector',
'GASENG' : 'Gas Engine',
'INTIFA2' : 'IFA 2 Interconnector',
'INERTIA' : 'Inertia Provider',
'INTIRL' : 'Moyle Interconnector',
'INTNEM' : 'NEMO Interconnector',
'NPSHYD' : 'Non Pumped Storage Hydro',
'INTNSL' : 'North Sea Link Interconnector',
'NA' : 'Not Applicable',
'NUCLEAR' : 'Nuclear',
'OIL' : 'Oil',
'OCGT' : 'Open Cycle Gas Turbine',
'OTHER' : 'Other',
'PS' : 'Pumped Storage',
'INTSC' : 'Scottish Interconnector',
'WIND' : 'Wind'}

power_stations_dic = {'ABERDARE' : 'Aberdare',
  'ABRBO' : 'Aberdeen Offshore Wind Farm',
  'ABTH' : 'Aberthaw B',
  'ACHLW' : 'Achlachan Wind Farm',
  'ACHRW' : 'AChruach Wind Farm',
  'ADELA' : 'Adela Energy Flex and DSR',
  'AFTOW' : 'Afton Wind Farm',
  'AKGLW' : 'Aikengall Wind Farm',
  'AIRSW' : 'Airies Wind Farm',
  'ALCOA' : 'Alcoa Peaker',
  'ALDMG' : 'Aldershot Military Generator',
  'ASHWW' : 'Andershaw Wind Farm',
  'ANSUW' : 'An Suidhe Windfarm',
  'ARBRB' : 'Arbroath Battery Storage',
  'ARCHW' : 'Arecleoch Windfarm',
  'ASLVW' : 'Assel Valley Wind Farm',
  'ABRTW' : 'Auchrobert Wind Farm',
  'AXPOG' : 'AXPO Flex and DSR',
  'BDCHW' : 'Bad a Cheo Wind Farm',
  'BAGE' : 'Baglan Bay',
  'BABAW' : 'Baillie Wind Farm',
  'BARK' : 'Barking',
  'BOWLW' : 'Barrow Offshore Windfarm',
  'BRYP' : 'Barry',
  'BTNHL' : 'Barton Hill Peaker',
  'BEATO' : 'Beatrice Offshore Windfarm',
  'CAS-BEU' : 'Beauly Cascade',
  'BTUIW' : 'Beinn An Tuirc Windfarm',
  'BEINW' : 'Beinneun Wind Farm',
  'BETHW' : 'Beinn Tharsuinn Windfarm',
  'BRYBW' : 'Berry Burn Windfarm',
  'BHLAW' : 'Bhlaraidh Wind Farm',
  'BSPHM' : 'Bispham',
  'BLKWW' : 'Blackcraig Windfarm',
  'BLLA' : 'Black Law Wind Farm',
  'BLLAE' : 'Black Law Wind Farm Extension',
  'BLARW' : 'Blarsomething Wind Farm',
  'ARNKB' : 'Bloxwich Battery',
  'BLODW' : 'Blyth Offshore Demonstrator Wind Farm',
  'BRWE' : 'Bradwell',
  'BRDUW' : 'Braes of Doune Windfarm',
  'BRNGW' : 'Brenig Wind Farm',
  'BRIDGWTR' : 'Bridgewater',
  'BRGG' : 'Brigg',
  'BRITNEDICD' : 'Britned Interconnector - Demand',
  'BRITNEDICG' : 'Britned Interconnector - Generation',
  'WISTW' : 'Brockloch Rig Windfarm',
  'BROFB' : 'Brook Farm Battery',
  'BRNLW' : 'Brownieleys Wind Farm',
  'BRXES' : 'Broxburn Energy Storage',
  'BURBO' : 'Burbo Bank Offshore Windfarm',
  'BRBEO' : 'Burbo Bank Wind Farm Extension',
  'BURGH' : 'Burghfield',
  'BNWKW' : 'Burn of Whilk Windfarm',
  'BURWB' : 'Burwell Weirs Drove Battery',
  'BUSTB' : 'Bustleholme Battery Storage',
  'CALDH' : 'Calder Hall',
  'CAMSW' : 'Camster Wind Farm',
  'PINFB' : 'Capenhurst Battery',
  'CRSSB' : 'Carnegie Road Battery',
  'CRGHW' : 'Carraig Gheal Wind Farm',
  'CARR' : 'Carrington',
  'CASTLEFORD' : 'Castleford CCGT',
  'CENTR' : 'Centrica Flex and DSR',
  'CHAP' : 'Chapelcross',
  'CHFMS' : 'Charity Farm Solar Park',
  'CHTRY' : 'Chatterly Recip',
  'CHESP' : 'Cheshire Power Station',
  'CHICK' : 'Chickerell',
  'CLAC' : 'Clachan',
  'CLFLW' : 'Clachan Flats Windfarm',
  'CLDRW' : 'Clashindarroch Wind Farm',
  'CLOCA' : 'Clocaenog Forest Wind Farm',
  'CAS-CLU' : 'Clunie Cascade',
  'CLDSW' : 'Clyde Windfarm',
  'COCK' : 'Cockenzie',
  'CNCLW' : 'Coire Na Cloiche Windfarm',
  'CMNBW' : 'Common Barn Wind Farm',
  'CNQPS' : 'Connahs Quay',
  'CAS-CON' : 'Conon Cascade',
  'CONTB' : 'Contego Battery',
  'CORB' : 'Corby',
  'CGTHW' : 'Corriegarth Wind Farm',
  'CRMLW' : 'Corriemoillie Wind Farm',
  'COSO' : 'Coryton',
  'COTPS' : 'Cottam',
  'CDCL' : 'Cottam Development Centre',
  'COUWW' : 'Cour Wind Farm',
  'COWE' : 'Cowes',
  'COWB' : 'Cowley Battery',
  'CRGTW' : 'Craig 2 Wind Farm',
  'CREAW' : 'Creag Riabhach Wind Farm',
  'CRDEW' : 'Crossdykes Windfarm',
  'CROYD' : 'Croydon',
  'CRUA' : 'Cruachan',
  'CRYRW' : 'Crystal Rig Wind Farm',
  'CWMD' : 'Cwm Dyli',
  'DALQW' : 'Dalquhandy Wind Farm',
  'DALSW' : 'Dalswinton Wind Farm',
  'DAMC' : 'Damhead Creek',
  'DEEP' : 'Deeside',
  'DERBY' : 'Derby',
  'DRSLW' : 'Dersalloch Wind Farm',
  'DERW' : 'Derwent',
  'DIDCA' : 'Didcot A',
  'DIDCB' : 'Didcot B',
  'DINO' : 'Dinorwig',
  'DOLG' : 'Dolgarrog',
  'DOREW' : 'Dorenell Wind Farm',
  'DOUGW' : 'Douglas West Wind Farm',
  'DOWLS' : 'Dowlais',
  'DBFRM' : 'Down Barn Farm Peaker',
  'DRKPS' : 'Drakelow',
  'DRAX' : 'Drax',
  'HAVEG' : 'Drax and Haven SVA Generation',
  'DDGNO' : 'Dudgeon Offshore Wind Farm',
  'DNGA' : 'Dungeness A',
  'DNGB' : 'Dungeness B',
  'DNLWW' : 'Dun Law Windfarm',
  'DUNGW' : 'Dunmglass Wind Farm',
  'EAAOO' : 'East Anglia One Offshore Wind Farm',
  'EASTWESTICD' : 'East-West Interconnector - Demand',
  'EASTWESTICG' : 'East-West Interconnector - Generation',
  'ECOT' : 'Ecotricity VPP',
  'EDFDS' : 'EDF Energy Demand Side Response',
  'EDGAG' : 'Edgeware (RWE) Flex',
  'EDINW' : 'Edinbane Windfarm',
  'EGGPS' : 'Eggborough',
  'ELECICD' : 'Eleclink Interconnector Demand',
  'ELECICG' : 'Eleclink Interconnector Generation',
  'ENDCO' : 'EnDCo and EPG Flex and DSR',
  'ENELX' : 'Enel X',
  'E24PX' : 'Energy 24 - PX - Demand Side Response',
  'EWRKS' : 'Energy Works (Hull)',
  'EELC' : 'Enfield',
  'RWEDD' : 'Engie DSR',
  'EQUIN' : 'Equinicity DSR and Flex',
  'EROVA' : 'Erova VPP',
  'ERRO' : 'Errochty',
  'EWHLW' : 'Ewe Hill Wind Farm',
  'EXETR' : 'Exeter',
  'FALGW' : 'Fallago Rig Wind Farm',
  'FANDS' : 'F and S Energy Ltd',
  'FARR' : 'Farr Wind Farm',
  'FASN' : 'Fasnakyle',
  'FAWL' : 'Fawley',
  'FAWN' : 'Fawley Cogen',
  'FELL' : 'Fellside',
  'FERR' : 'Ferrybridge',
  'FFES' : 'Ffestiniog',
  'FIBRE' : 'FibroPower',
  'FIDL' : 'Fiddlers Ferry',
  'FIFE' : 'Fife CCGT',
  'FINL' : 'Finlarig',
  'FLEXI' : 'Flexitricity Demand Side Response',
  'FDUN' : 'Fort Dunlop',
  'FOYE' : 'Foyers',
  'FSDLW' : 'Freasdail Wind Farm',
  'FRENCHICD' : 'French Interconnector - Demand',
  'FRENCHICG' : 'French Interconnector - Generation',
  'GLWSW' : 'Galawhistle Wind Farm',
  'GANW' : 'Galloper Offshore Windfarm',
  'CAS-GAR' : 'Garry Cascade',
  'GAZPM' : 'Gazprom Flex and DSR',
  'GIPSY' : 'Gipsy Lane Battery',
  'GNAPW' : 'Glen App Wind Farm',
  'GLCHW' : 'Glenchamber Wind Farm',
  'GLNDO' : 'Glendoe',
  'GLNKW' : 'Glen Kyllachly Windfarm',
  'GFOUD' : 'Glens of Foudland Windfarm',
  'GFLDW' : 'Goole Fields Windfarm',
  'GOSHS' : 'Goose House Lane Peaker',
  'GORDW' : 'Gordonbush Windfarm',
  'GDSTW' : 'Gordonstown Wind Farm',
  'GRAI' : 'Grain',
  'GRAIC' : 'Grain CHP',
  'GRASC' : 'Grain Synchronous Compensator',
  'GRMO' : 'Grangemouth CHP',
  'GRGBW' : 'Greater Gabbard Offshore Windfarm',
  'GYAR' : 'Great Yarmouth',
  'GRFLB' : 'Greenfield Road Battery',
  'GRIDB' : 'Grid Beyond DSR',
  'GRIFW' : 'Griffin Windfarm',
  'GRMAP' : 'Grimsby A Power Station',
  'GNFSW' : 'Gunfleet Sands Windfarm',
  'GYMRW' : 'Gwynt y Mor Offshore Wind Farm',
  'HABIT' : 'Habitat - Batteries and Demand Side Response',
  'HADHW' : 'Hadyard Hill Wind Farm',
  'HALSW' : 'Halsary Windfarm',
  'HBHDW' : 'Harburnhead Wind Farm',
  'HRHLW' : 'Hare Hill Extension Wind Farm',
  'HRSTW' : 'Harestanes Windfarm',
  'HRTL' : 'Hartlepool',
  'HARTR' : 'Hartree Flex and DSR',
  'HAWKB' : 'Hawkers Hill Battery',
  'HEYM1' : 'Heysham 1',
  'HEYM2' : 'Heysham 2',
  'HMRPS' : 'High Marnham',
  'HLGLW' : 'Hill of Glaschyle Wind Farm',
  'HLTWW' : 'Hill of Towie Windfarm',
  'HINA' : 'Hinkley Point A',
  'HINB' : 'Hinkley Point B',
  'HINC' : 'Hinkley Point C',
  'BHOLB' : 'Holes Bay Battery',
  'HOWAO' : 'Hornsea A Offshore Windfarm',
  'HOWBO' : 'Hornsea B Offshore Windfarm',
  'HMGTO' : 'Humber Gateway Offshore Wind Farm',
  'BARNB' : 'Hunningley Stairfoot Battery',
  'HUNB' : 'Hunterston B',
  'HUTTB' : 'Hutton Battery Storage',
  'HYTHE' : 'Hythe CHP',
  'HYWDW' : 'Hywind Offshore Windfarm',
  'IFA2ICD' : 'IFA 2 Interconnector Demand',
  'IFA2ICG' : 'IFA 2 Interconnector Generation',
  'HUMR' : 'Immingham CHP',
  'IDNQ' : 'Indian Queens',
  'IRNPS' : 'Ironbridge',
  'JET' : 'Joint European Torus Fusion Reactor Prototype',
  'K3CHP' : 'K3 CHP Facility',
  'KEAD' : 'Keadby',
  'KTHLW' : 'Keith Hill Windfarm',
  'KTHRS' : 'Keith Storage',
  'KEMB' : 'Kemsley Battery',
  'KENNW' : 'Kennoxhead Wind Farm',
  'KILB' : 'Kilbraur Wind Farm',
  'KLGLW' : 'Kilgallioch',
  'CAS-KIL' : 'Killin Cascade',
  'KILNS' : 'Killingholme 1',
  'KILLPG' : 'Killingholme 2',
  'KILLSC' : 'Killingholme Synchronous Compensator',
  'KINCW' : 'Kincardine Offshore Windfarm',
  'KLYN' : 'Kings Lynn',
  'KINO' : 'Kingsnorth',
  'KNLCV' : 'Kinlochleven Hydro',
  'KPMRW' : 'Kype Muir Windfarm',
  'LAGA' : 'Langage',
  'LSTWY' : 'Lester Way',
  'LCHWT' : 'Letchworth',
  'ANGEL' : 'Limejump Demand Side Response',
  'LNCSW' : 'Lincs Windfarm',
  'LRDSC' : 'Lister Drive Synchronous Condenser',
  'LBAR' : 'Little Barford',
  'LITTD' : 'Littlebrook D',
  'LOCHA' : 'Lochaber Hydro',
  'LCLTW' : 'Lochluichart Windfarm',
  'LCKLB' : 'Lockleaze Battery',
  'LARYW' : 'London Array Windfarm',
  'LOAN' : 'Longannet',
  'JGPLM' : 'Low Marnham CHP',
  'LYNEM' : 'Lynemouth',
  'MAEN' : 'Maentwrog',
  'MANX' : 'Manx Power',
  'MRWD' : 'Marchwood',
  'MKHLW' : 'Mark Hill Windfarm',
  'MEDP' : 'Medway',
  'TSREP' : 'MGT Teesside Renewable Power Station',
  'MIDMW' : 'Middle Muir Wind Farm',
  'MIDDL' : 'Middlewich',
  'MDHLW' : 'Mid Hill Wind Farm',
  'MILWW' : 'Millenium Wind Farm',
  'MYGPW' : 'Minnygap Wind Farm',
  'MINSW' : 'Minsca Wind Farm',
  'MORFL' : 'Moorfield Drive Peaker',
  'MRHSW' : 'Moor House Wind Farm',
  'MOWEO' : 'Moray East Offshore Windfarm',
  'CAS-MOR' : 'Moriston Cascade',
  'MOYLEICD' : 'Moyle Interconnector - Demand',
  'MOYLEICG' : 'Moyle Interconnector - Generation',
  'MOYEW' : 'Moy Windfarm',
  'MYBWF' : 'Mynydd y Betws Windfarm',
  'MYGWW' : 'Mynydd Y Gwair Wind Farm',
  'NANCW' : 'Nanclach Wind Farm',
  'NANT' : 'Nant',
  'NEMOICD' : 'NEMO Interconnector Demand',
  'NEMOICG' : 'NEMO Interconnector Generation',
  'NSLICD' : 'North Sea Link Interconnector Demand',
  'NSLICG' : 'North Sea Link Interconnector Generator',
  'NATPV' : 'Npower VPP',
  'NURSB' : 'Nursling Battery Storage',
  'OCTOP' : 'Octopus Energy DSR',
  'OLDS' : 'Oldbury',
  'OMNDW' : 'Ormonde Windfarm',
  'PADDI' : 'Paddington Power DSR and Flex',
  'PAUHW' : 'Pauls Hill Windfarm',
  'PAULW' : 'Pauls Hill Wind Farm',
  'PGBRY' : 'PeakGEN Barry',
  'PGBRK' : 'PeakGEN Bracknell',
  'PGEXT' : 'PeakGEN Exeter',
  'PGFAR' : 'PeakGEN Fareham',
  'PGHVN' : 'PeakGEN Havant',
  'PGLAN' : 'PeakGEN Llandarcy',
  'PGPET' : 'PeakGEN Peterborough',
  'PEMB' : 'Pembroke',
  'PNYCB' : 'Pen y Cymoedd Battery',
  'PNYCW' : 'Pen y Cymoedd Wind Farm',
  'PETEM' : 'Peterborough',
  'PEHE' : 'Peterhead',
  'PILLB' : 'Pillswood Battery Storage',
  'PLYRK' : 'Plymouth Rock Peaker',
  'PGBIW' : 'Pogbie Wind Farm',
  'POTES' : 'Port of Tyne Energy Storage',
  'RCBKO' : 'Race Bank Offshore Wind Farm',
  'RAKEL' : 'Rake Lane Peaker',
  'RMPNO' : 'Rampion Offshore Windfarm',
  'RASSP' : 'Rassau Sync Condenser',
  'RATS' : 'Ratcliffe',
  'REDGT' : 'Redditch',
  'RDFRD' : 'Redfield Road',
  'RDSCR' : 'Redscar',
  'RHEI' : 'Rheidol',
  'RISEC' : 'Rise Carr Battery',
  'RRIGW' : 'Robin Rigg Windfarm',
  'ROCK' : 'Rocksavage',
  'ROOS' : 'Roosecote',
  'ROOSB' : 'Roosecote Battery',
  'RSHLW' : 'Rosehall Wind Farm',
  'MARK' : 'Rothes Bio-Plant CHP',
  'CAIRW' : 'Rothes (Cairn Uish) Wind Farm',
  'RUGPS' : 'Rugeley B',
  'RYHPS' : 'Rye House',
  'SCCL' : 'Saltend',
  'SANDBACH' : 'Sandbach CHP',
  'SAKNW' : 'Sandy Knowe Wind Farm',
  'SANQW' : 'Sanquhar Community Wind Farm Generation',
  'SCOTICD' : 'Scottish Interconnector - Demand',
  'SCOTICG' : 'Scottish Interconnector - Generation',
  'SEAB' : 'Seabank',
  'SGRO1' : 'Seagreen 1 Offshore Windfarm',
  'SVRP' : 'Severn Power',
  'SEVINGTON' : 'Sevington',
  'SHRSW' : 'Sheringham Shoals Windfarm',
  'SHOS' : 'Shoreham',
  'SHOT' : 'Shotton',
  'SIMEC' : 'Simec Biofuel',
  'SIZEA' : 'Sizewell A',
  'SIZB' : 'Sizewell B',
  'SKELB' : 'Skelmersdale Battery Storage',
  'SLOUG' : 'Slough Heat and Power',
  'SLOY' : 'Sloy',
  'SMART' : 'Smartest Demand Side Response',
  'SEDUN' : 'Smartest Energy Dunbar',
  'SOLUTIA' : 'Solutia',
  'SWBKW' : 'Solwaybank Wind Farm',
  'SHBA' : 'South Humber Bank',
  'SOKY1' : 'South Kyle 1 Windfarm',
  'SPLN' : 'Spalding',
  'SPEEX' : 'Spalding Energy Expansion',
  'STATK' : 'Statkraft Demand Side Response',
  'STAY' : 'Staythorpe',
  'STOKE' : 'Stoke CHP',
  'STRNW' : 'Strathy North Wind Farm',
  'STLGW' : 'Stronelairg Windfarm',
  'SUDME' : 'Sudmeadow',
  'SUTB' : 'Sutton Bridge',
  'TAYL' : 'Taylors Lane',
  'TESI' : 'Teesside',
  'TESLA' : 'Tesla Motors Company',
  'THNTW' : 'Thanet Offshore Windfarm',
  'TDRVE' : 'The Drove',
  'THORNHILL' : 'Thornhill CCGT',
  'TILB' : 'Tilbury B',
  'TGP' : 'Tilbury Green Power',
  'TDBNW' : 'Toddleburn Wind Farm',
  'TORN' : 'Torness',
  'TRFPK' : 'Trafalgar Park',
  'TRLGW' : 'Tralorg Wind Farm',
  'TRAWS' : 'Trawsfynydd',
  'TRNGS' : 'Triangle Farm Solar Park',
  'TKNLO' : 'Triton Knoll Offshore Windfarm',
  'TULWW' : 'Tullo Wind Farm',
  'TULW' : 'Tullow Wind Farm',
  'TLYMW' : 'Tullymurdoch Wind Farm',
  'TWSHW' : 'Twentyshilling Hill Windfarm',
  'UKPRD' : 'UKPR Demand Side Response',
  'UNKNOWN' : 'Unknown',
  'FIFUSK' : 'Uskmouth / Fifoots Point',
  'VKING' : 'Viking',
  'WLNYW' : 'Walney Offshore Windfarm',
  'WTRLN' : 'Water Lane',
  'WAVEH' : 'Wavehub',
  'WEAV' : 'Weaver',
  'WELSH' : 'Welsh Power Aggregated',
  'WBUPS' : 'West Burton',
  'WBURB' : 'West Burton B',
  'WBRBB' : 'West Burton B Battery',
  'WTMRW' : 'Westermost Rough Wind Farm',
  'WDNSW' : 'West of Duddon Sands Windfarm',
  'WESP' : 'Weston Point',
  'WHLWB' : 'Whitelee Battery',
  'WHILW' : 'Whitelee Wind Farm',
  'WHIHW' : 'Whiteside Hill Windfarm',
  'WDRGW' : 'Windy Rig Wind Farm',
  'WYLF' : 'Wylfa',
  'ZENOB' : 'Zenobe BESS Aggregated'}