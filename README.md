# Elexon Gross Demand ETL Guideline*
*Code snippets from scripts running on Databricks, not complete pipeline. Although all necessary inputs' locations are pointed out.\
*For any further clarifications, contact me at: pcparedesp@hotmail.com

## Outputs:
- GB Demand at a half-hourly, sectoral (Domestic, Non-domestic), GSP-area (Grid Supply Point) granularity.
  - Settlement Demand (measured from final-users consumption).
  - Includes Big Demand (Transmission Line connected demand) by Category.
  - Embedded Generation at SVA (Suppliers Volume Allocation) level.
  - Basis to produce Base Demand Volumes for non-commodity costs: CfD (Contracts for Difference), RO (Renewable Obligations), FiT (Feed in Tariffs).

## Inputs:

#### Elexon Open Dataflows as Inputs: [https://www.elexonportal.co.uk/]
- **P315:** [https://www.elexonportal.co.uk/p315?] "Suppliers' Meter Volume and MPAN counts" (This is SVA demand only)  Available through FTP
  - P0276: GSP Group Consumption Totals Report.
  - P0277: GSP Group Market Matrix Report.
- **P114:** [https://www.elexonportal.co.uk/p114] (SVA and CVA demand) Available through FTP or the APIs below (get API_KEY from https://www.elexonportal.co.uk/)
  - SAA-i014: Settlement Reports [`https://downloads.elexonportal.co.uk/p114/list?key=<API_KEY>&date=2018-01-17&filter=s0142`]
  - SAA-i042: BM Unit Gross Demand Report. [`https://downloads.elexonportal.co.uk/p114/list?key=<API_KEY>&date=2018-01-17&filter=c0421`]

#### Elexon MDD (Market Domain Data) Tables as Inputs: [https://www.elexonportal.co.uk/mddviewer/]
- **TLMs:** Transmission Loss Multipliers. [https://www.elexonportal.co.uk/article/view/33407] [https://downloads.elexonportal.co.uk/file/download/TLM_FILE?key=YOUR_SCRIPTING_KEY_FROM_ELEXON_PORTAL]
- **LLFs:** Line Loss Factors. [https://www.elexonportal.co.uk/svallf/] [for bulk downloading/file transfer please contact llfs@elexon.co.uk]
- **GCFs:** Grid Correction Factors. [https://www.elexonportal.co.uk/article/view/1796] [www.elexonportal.co.uk/gspgroupcorrectionfactordata]
- **PCs:** Profile Coefficients. [https://www.elexonportal.co.uk/article/view/31709] [www.elexonportal.co.uk/d0018]

#### BMU Dictionaries:
- Classifications by different categories.
  - https://bmrs.elexon.co.uk/api-documentation/endpoint/reference/bmunits/all
  - https://www.netareports.com/data/elexon/bmu.jsp

#### Notes:
- If you need any bulk download of past data of GCF, TLMs, etc. contact ELEXON (links above).