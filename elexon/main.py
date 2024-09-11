from mdd_tables import load_llfs, load_gcf, load_pc, load_tlm
from bmu_dictionaries import load_dictionary_elexon, load_dictionary_enappsys, load_merged_dictionary
from dataflows import load_elexon_p315_276, load_elexon_p315_276_exports, load_elexon_p315_277, load_elexon_p114_c042, load_elexon_p114_s014
from transform import transform_elexon_p315_total, transform_elexon_gross_demand, transform_cfd_cap_mech, transform_ro_fit, transform_embedded_generation, transform_nd_bkdwn_approx


def load_market_domain_data():
    # Load the latest mdd_tables: LLfs, TLMs, GCFs, PCs.
    load_llfs()
    load_gcf()
    load_pc()
    load_tlm()


def load_bmu_dictionary_data():
    # Load the BMU dictionary.
    load_dictionary_elexon()
    load_dictionary_enappsys()
    load_merged_dictionary()


def load_p315_p114_data():
    # Main dataflows of GB demand.
    load_elexon_p315_276()
    load_elexon_p315_276_exports()
    load_elexon_p315_277()
    load_elexon_p114_c042()
    load_elexon_p114_s014()


def transform_elexon_demand():
    # Load the created tables --> After manipulation and merging different dataflows to render the final tables
    # Gross Demand, Embedded Generation, BMU Categorized Volumes, ND bkdwn.
    transform_elexon_p315_total()
    transform_elexon_gross_demand()
    transform_cfd_cap_mech()
    transform_ro_fit()
    transform_embedded_generation()
    transform_nd_bkdwn_approx()


def main():
    load_market_domain_data()
    load_bmu_dictionary_data()
    load_p315_p114_data()
    transform_elexon_demand()


if __name__ == '__main__':
    main()
    print("ELEXON GB Demand ETL succesfully ran.")
