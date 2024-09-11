import os
import pandas as pd
import numpy as np
import pyspark.pandas as ps
import datetime
import sys
from databricks.sdk.runtime import *


def transform_embedded_generation():

    # From the P315 Dataflow:

    # From dataflows sub-package
    p315 = ps.read_table('elexon_f.p315_embedded_sva')
    p315 = p315.groupby(by=['date', 'settlement_period'])[['total_tlm_corrected_consumption_gwh']].sum().reset_index(
    ).rename(columns={'date': 'settlement_date', 'total_tlm_corrected_consumption_gwh': 'p315_volume_gwh'})
    p315 = p315[p315['settlement_date'] >= '2015-01-01']

    limit = np.min([p315['settlement_date'].max(), ps.read_table(
        # From dataflows sub-package
        'elexon_f.p114_s014')['SettlementDate'].max()])
    limit_2 = np.max([p315['settlement_date'].min(), ps.read_table(
        # From dataflows sub-package
        'elexon_f.p114_s014')['SettlementDate'].min()])

    # From the P114 Dataflow:

    # SVA w/FPN
    sva_fpn = ps.read_table('elexon_f.p114_s014')  # From dataflows sub-package
    sva_fpn = sva_fpn[sva_fpn['SettlementDate'] >= '2015-01-01']
    sva_fpn = sva_fpn[sva_fpn['ImportExportIndicator'] == 'export']
    sva_fpn = sva_fpn[sva_fpn['enapp_class'].str.slice(stop=3) == 'SVA']
    sva_fpn['all_notify'] = sva_fpn['all_notify'].fillna(False)
    sva_no_fpn = sva_fpn.copy()
    sva_fpn = sva_fpn[sva_fpn['all_notify'] == True]
    sva_fpn['volume_gwh'] = sva_fpn['BMUnitMeteredVolume'] * \
        sva_fpn['TransmissionLossMultiplier']
    sva_fpn = sva_fpn.groupby(by=['SettlementDate', 'SettlementPeriod', 'enapp_class', 'all_notify'])[['volume_gwh']].sum(
    ).reset_index().rename(columns={'SettlementDate': 'settlement_date', 'SettlementPeriod': 'settlement_period'})
    sva_fpn[['volume_gwh']] = sva_fpn[['volume_gwh']]/1000
    sva_fpn['classification'] = 'sva_bmu'

    # SVA w/o FPN
    sva_no_fpn = sva_no_fpn[sva_no_fpn['all_notify'] == False]
    sva_no_fpn['volume_gwh'] = sva_no_fpn['BMUnitMeteredVolume'] * \
        sva_no_fpn['TransmissionLossMultiplier']
    sva_no_fpn = sva_no_fpn.groupby(by=['SettlementDate', 'SettlementPeriod', 'enapp_class', 'all_notify'])[['volume_gwh']].sum(
    ).reset_index().rename(columns={'SettlementDate': 'settlement_date', 'SettlementPeriod': 'settlement_period'})
    sva_no_fpn[['volume_gwh']] = sva_no_fpn[['volume_gwh']]/1000
    sva_no_fpn['classification'] = 'sva_bmu'

    # Embedded GSP (E_) w/FPN
    embedded_big_fpn = ps.read_table(
        'elexon_f.p114_s014')  # From dataflows sub-package
    embedded_big_fpn = embedded_big_fpn[embedded_big_fpn['SettlementDate'] >= '2015-01-01']
    embedded_big_fpn = embedded_big_fpn[embedded_big_fpn['ImportExportIndicator'] == 'export']
    embedded_big_fpn = embedded_big_fpn[embedded_big_fpn['player'] == 'E_']
    embedded_big_fpn['all_notify'] = embedded_big_fpn['all_notify'].fillna(
        False)
    embedded_big_no_fpn = embedded_big_fpn.copy()
    embedded_big_fpn = embedded_big_fpn[embedded_big_fpn['all_notify'] == True]
    embedded_big_fpn['volume_gwh'] = embedded_big_fpn['BMUnitMeteredVolume'] * \
        embedded_big_fpn['TransmissionLossMultiplier']
    embedded_big_fpn = embedded_big_fpn.groupby(by=['SettlementDate', 'SettlementPeriod', 'enapp_class', 'all_notify'])[
        ['volume_gwh']].sum().reset_index().rename(columns={'SettlementDate': 'settlement_date', 'SettlementPeriod': 'settlement_period'})
    embedded_big_fpn[['volume_gwh']] = embedded_big_fpn[['volume_gwh']]/1000
    embedded_big_fpn['classification'] = 'embedded_bmu'

    # Embedded GSP (E_) w/o FPN
    embedded_big_no_fpn = embedded_big_no_fpn[embedded_big_no_fpn['all_notify'] == False]
    embedded_big_no_fpn['volume_gwh'] = embedded_big_no_fpn['BMUnitMeteredVolume'] * \
        embedded_big_no_fpn['TransmissionLossMultiplier']
    embedded_big_no_fpn = embedded_big_no_fpn.groupby(by=['SettlementDate', 'SettlementPeriod', 'enapp_class', 'all_notify'])[
        ['volume_gwh']].sum().reset_index().rename(columns={'SettlementDate': 'settlement_date', 'SettlementPeriod': 'settlement_period'})
    embedded_big_no_fpn[['volume_gwh']
                        ] = embedded_big_no_fpn[['volume_gwh']]/1000
    embedded_big_no_fpn['classification'] = 'embedded_bmu'

    # Supplier's CfD embedded assets w/o FPN
    # From dataflows sub-package
    sva_cfd_no_fpn = ps.read_table('elexon_f.p114_s014')
    sva_cfd_no_fpn = sva_cfd_no_fpn[sva_cfd_no_fpn['SettlementDate']
                                    >= '2015-01-01']
    sva_cfd_no_fpn = sva_cfd_no_fpn[sva_cfd_no_fpn['ImportExportIndicator'] == 'export']
    sva_cfd_no_fpn = sva_cfd_no_fpn[sva_cfd_no_fpn['player'] == 'C_']
    sva_cfd_no_fpn['all_notify'] = sva_cfd_no_fpn['all_notify'].fillna(False)
    sva_cfd_fpn = sva_cfd_no_fpn.copy()
    sva_cfd_no_fpn = sva_cfd_no_fpn[sva_cfd_no_fpn['all_notify'] == False]
    sva_cfd_no_fpn['volume_gwh'] = sva_cfd_no_fpn['BMUnitMeteredVolume'] * \
        sva_cfd_no_fpn['TransmissionLossMultiplier']
    sva_cfd_no_fpn = sva_cfd_no_fpn.groupby(by=['SettlementDate', 'SettlementPeriod', 'enapp_class', 'all_notify'])[['volume_gwh']].sum(
    ).reset_index().rename(columns={'SettlementDate': 'settlement_date', 'SettlementPeriod': 'settlement_period'})
    sva_cfd_no_fpn[['volume_gwh']] = sva_cfd_no_fpn[['volume_gwh']]/1000
    sva_cfd_no_fpn['classification'] = 'supplier_cfd'

    # Supplier's CfD embedded assets w/FPN
    sva_cfd_fpn = sva_cfd_fpn[sva_cfd_fpn['all_notify'] == True]
    sva_cfd_fpn['volume_gwh'] = sva_cfd_fpn['BMUnitMeteredVolume'] * \
        sva_cfd_fpn['TransmissionLossMultiplier']
    sva_cfd_fpn = sva_cfd_fpn.groupby(by=['SettlementDate', 'SettlementPeriod', 'enapp_class', 'all_notify'])[['volume_gwh']].sum(
    ).reset_index().rename(columns={'SettlementDate': 'settlement_date', 'SettlementPeriod': 'settlement_period'})
    sva_cfd_fpn[['volume_gwh']] = sva_cfd_fpn[['volume_gwh']]/1000
    sva_cfd_fpn['classification'] = 'supplier_cfd'

    # Supplier's not CfD embedded assets w/o FPN
    supplier_no_cfd_gen = ps.read_table(
        'elexon_f.p114_s014')  # From dataflows sub-package
    supplier_no_cfd_gen = supplier_no_cfd_gen[supplier_no_cfd_gen['SettlementDate'] >= '2015-01-01']
    supplier_no_cfd_gen = supplier_no_cfd_gen[supplier_no_cfd_gen['ImportExportIndicator'] == 'export']
    supplier_no_cfd_gen = supplier_no_cfd_gen[supplier_no_cfd_gen['player'] == '2_']
    supplier_no_cfd_gen = supplier_no_cfd_gen[supplier_no_cfd_gen['enapp_class'].str.slice(
        stop=3) != 'SVA']
    supplier_no_cfd_gen['all_notify'] = supplier_no_cfd_gen['all_notify'].fillna(
        False)
    supplier_no_cfd_gen_no_fpn = supplier_no_cfd_gen[supplier_no_cfd_gen['all_notify'] == False]
    supplier_no_cfd_gen_no_fpn['volume_gwh'] = supplier_no_cfd_gen_no_fpn['BMUnitMeteredVolume'] * \
        supplier_no_cfd_gen_no_fpn['TransmissionLossMultiplier']
    supplier_no_cfd_gen_no_fpn = supplier_no_cfd_gen_no_fpn.groupby(by=['SettlementDate', 'SettlementPeriod', 'enapp_class', 'all_notify'])[
        ['volume_gwh']].sum().reset_index().rename(columns={'SettlementDate': 'settlement_date', 'SettlementPeriod': 'settlement_period'})
    supplier_no_cfd_gen_no_fpn[['volume_gwh']
                               ] = supplier_no_cfd_gen_no_fpn[['volume_gwh']]/1000
    supplier_no_cfd_gen_no_fpn['classification'] = 'supplier_embedded_no_cfd'

    # Supplier's not CfD embedded assets w/FPN
    supplier_no_cfd_gen_fpn = supplier_no_cfd_gen[supplier_no_cfd_gen['all_notify'] == True]
    supplier_no_cfd_gen_fpn['volume_gwh'] = supplier_no_cfd_gen_fpn['BMUnitMeteredVolume'] * \
        supplier_no_cfd_gen_fpn['TransmissionLossMultiplier']
    supplier_no_cfd_gen_fpn = supplier_no_cfd_gen_fpn.groupby(by=['SettlementDate', 'SettlementPeriod', 'enapp_class', 'all_notify'])[
        ['volume_gwh']].sum().reset_index().rename(columns={'SettlementDate': 'settlement_date', 'SettlementPeriod': 'settlement_period'})
    supplier_no_cfd_gen_fpn[['volume_gwh']
                            ] = supplier_no_cfd_gen_fpn[['volume_gwh']]/1000
    supplier_no_cfd_gen_fpn['classification'] = 'supplier_embedded_no_cfd'

    # Concatenating the different classifications of the embedded generation within the dataflow P114
    p114_embedded = ps.concat([sva_fpn, sva_no_fpn, embedded_big_fpn, embedded_big_no_fpn,
                              sva_cfd_fpn, sva_cfd_no_fpn, supplier_no_cfd_gen_fpn, supplier_no_cfd_gen_no_fpn])

    # Getting the missing volumes - the ones that are not part of the BMU volumes (both SVA and CVA)
    # In other words, exports that stay within a GSP but are metered.
    missing = p315.merge(p114_embedded.groupby(by=['settlement_date', 'settlement_period'])[
                         ['volume_gwh']].sum().reset_index(), how='left', on=['settlement_date', 'settlement_period'])
    missing['missing'] = missing['p315_volume_gwh'] - missing['volume_gwh']
    missing = missing.drop(columns=['p315_volume_gwh', 'volume_gwh']).rename(
        columns={'missing': 'volume_gwh'})
    missing['volume_gwh'] = missing['volume_gwh'].apply(
        lambda x: x if x >= 0 else 0)
    missing['all_notify'] = False
    missing['classification'] = 'metered_gsp_exports'
    missing['enapp_class'] = 'p315_complement'

    embedded_generation = ps.concat([p114_embedded, missing])
    embedded_generation = embedded_generation[embedded_generation['settlement_date'] <= limit]
    embedded_generation = embedded_generation[embedded_generation['settlement_date'] >= limit_2]

    embedded_generation['settlement_datetime'] = embedded_generation[['settlement_date', 'settlement_period']].apply(
        lambda x: x['settlement_date'] + datetime.timedelta(minutes=(x['settlement_period']-1)*30), axis=1)

    save_path = '/tmp/delta/embedded_generation'
    embedded_generation.to_spark().write.format('delta').save(save_path)
    # DATABRICKS: Saved as table: elexon_f.embedded_generation
