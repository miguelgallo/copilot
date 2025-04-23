import h5py
import polars as pl
import numpy as np
import pandas as pd # Still needed for the temporary conversion during efficiency calculation

# Import necessary components for efficiency calculations (assuming proton_efficiency.py is available)
try:
    import ROOT # Dependency check
    from proton_efficiency import (
        efficiencies_2017,
        strict_zero_efficiencies,
        proton_efficiency_uncertainty,
        efficiencies_2018
    )
    PROTON_EFFICIENCY_AVAILABLE = True
except ImportError:
    print("WARNING: proton_efficiency.py or ROOT library not found. Efficiency calculations will be skipped or use defaults.")
    PROTON_EFFICIENCY_AVAILABLE = False
    # Define placeholders if the import fails, to avoid crashing later code sections
    proton_efficiency_uncertainty = {
        '2017': { '45': 0.0, '56': 0.0 },
        '2018': { '45': 0.0, '56': 0.0 }
    }
    def efficiencies_2017(): return {}, {}, {}, None, None
    def strict_zero_efficiencies(): return {}
    def efficiencies_2018(): return {}, {}, None, None


# --- Run Ranges and Luminosity Data (Identical to original) ---
run_ranges_periods_2017 = {
    "2017B"  : [297020,299329],
    "2017C1" : [299337,300785],
    "2017C2" : [300806,302029],
    "2017D"  : [302030,303434],
    "2017E"  : [303435,304826],
    "2017F1" : [304911,305114],
    "2017F2" : [305178,305902],
    "2017F3" : [305965,306462]
    }
df_run_ranges_2017 = pl.DataFrame([
    {"period": key, "min": val[0], "max": val[1]}
    for key, val in run_ranges_periods_2017.items()
    ])
run_ranges_periods_mixing_2017 = run_ranges_periods_2017
df_run_ranges_mixing_2017 = df_run_ranges_2017
run_ranges_periods_2018 = {
    "2018A"  : [315252,316995],
    "2018B1" : [316998,317696],
    "2018B2" : [318622,319312],
    "2018C"  : [319313,320393],
    "2018D1" : [320394,322633],
    "2018D2" : [323363,325273]
    }
df_run_ranges_2018 = pl.DataFrame([
    {"period": key, "min": val[0], "max": val[1]}
    for key, val in run_ranges_periods_2018.items()
    ])
run_ranges_periods_mixing_2018 = {
    "2018A"  : [315252,316995],
    "2018B"  : [316998,319312],
    "2018C"  : [319313,320393],
    "2018D1" : [320394,322633],
    "2018D2" : [323363,325273]
    }
df_run_ranges_mixing_2018 = pl.DataFrame([
    {"period": key, "min": val[0], "max": val[1]}
    for key, val in run_ranges_periods_mixing_2018.items()
    ])

# --- Luminosity Values (Identical) ---
L_2017B = 4.799881474;
L_2017C1 = 5.785813941;
L_2017E = 9.312832062;
L_2017F1 = 1.738905587;
L_2017C2 = 3.786684323;
L_2017D = 4.247682053;
L_2017F2 = 8.125575961;
L_2017F3 = 3.674404546;
lumi_periods_2017 = {}
lumi_periods_2017[ 'muon' ] = {}
lumi_periods_2017[ 'muon' ][ "2017B" ]  = L_2017B
lumi_periods_2017[ 'muon' ][ "2017C1" ] = L_2017C1
lumi_periods_2017[ 'muon' ][ "2017C2" ] = L_2017C2
lumi_periods_2017[ 'muon' ][ "2017D" ]  = L_2017D
lumi_periods_2017[ 'muon' ][ "2017E" ]  = L_2017E
lumi_periods_2017[ 'muon' ][ "2017F1" ] = L_2017F1
lumi_periods_2017[ 'muon' ][ "2017F2" ] = L_2017F2
lumi_periods_2017[ 'muon' ][ "2017F3" ] = L_2017F3
lumi_periods_2017[ 'electron' ] = {}
lumi_periods_2017[ 'electron' ][ "2017B" ]  = L_2017B * 0.957127
lumi_periods_2017[ 'electron' ][ "2017C1" ] = L_2017C1 * 0.954282
lumi_periods_2017[ 'electron' ][ "2017C2" ] = L_2017C2 * 0.954282
lumi_periods_2017[ 'electron' ][ "2017D" ]  = L_2017D * 0.9539
lumi_periods_2017[ 'electron' ][ "2017E" ]  = L_2017E * 0.956406
lumi_periods_2017[ 'electron' ][ "2017F1" ] = L_2017F1 * 0.953733
lumi_periods_2017[ 'electron' ][ "2017F2" ] = L_2017F2 * 0.953733
lumi_periods_2017[ 'electron' ][ "2017F3" ] = L_2017F3 * 0.953733
print ( lumi_periods_2017 )
print ( "Luminosity 2017 muon: {}".format( np.sum( list( lumi_periods_2017[ 'muon' ].values() ) ) ) )
print ( "Luminosity 2017 electron: {}".format( np.sum( list( lumi_periods_2017[ 'electron' ].values() ) ) ) )

L_2018A  = 14.027047499
L_2018B1 = 6.629673574
L_2018B2 = 0.430948924
L_2018C  = 6.891747024
L_2018D1 = 20.962647459
L_2018D2 = 10.868724698
lumi_periods_2018 = {}
lumi_periods_2018[ 'muon' ] = {}
lumi_periods_2018[ 'muon' ][ "2018A" ]  = L_2018A * 0.999913
lumi_periods_2018[ 'muon' ][ "2018B1" ] = L_2018B1 * 0.998672
lumi_periods_2018[ 'muon' ][ "2018B2" ] = L_2018B2 * 0.998672
lumi_periods_2018[ 'muon' ][ "2018C" ]  = L_2018C * 0.999991
lumi_periods_2018[ 'muon' ][ "2018D1" ] = L_2018D1 * 0.998915
lumi_periods_2018[ 'muon' ][ "2018D2" ] = L_2018D2 * 0.998915
lumi_periods_2018[ 'electron' ] = {}
lumi_periods_2018[ 'electron' ][ "2018A" ]  = L_2018A * 0.933083
lumi_periods_2018[ 'electron' ][ "2018B1" ] = L_2018B1 * 0.999977
lumi_periods_2018[ 'electron' ][ "2018B2" ] = L_2018B2 * 0.999977
lumi_periods_2018[ 'electron' ][ "2018C" ]  = L_2018C * 0.999978
lumi_periods_2018[ 'electron' ][ "2018D1" ] = L_2018D1 * 0.999389
lumi_periods_2018[ 'electron' ][ "2018D2" ] = L_2018D2 * 0.999389
print ( lumi_periods_2018 )
print ( "Luminosity 2018 muon: {}".format( np.sum( list( lumi_periods_2018[ 'muon' ].values() ) ) ) )
print ( "Luminosity 2018 electron: {}".format( np.sum( list( lumi_periods_2018[ 'electron' ].values() ) ) ) )

# --- Mapping Dictionaries (Identical) ---
aperture_period_map = {
    "2016_preTS2"  : "2016_preTS2",
    "2016_postTS2" : "2016_postTS2",
    "2017B"        : "2017_preTS2",
    "2017C1"       : "2017_preTS2",
    "2017C2"       : "2017_preTS2",
    "2017D"        : "2017_preTS2",
    "2017E"        : "2017_postTS2",
    "2017F1"       : "2017_postTS2",
    "2017F2"       : "2017_postTS2",
    "2017F3"       : "2017_postTS2",
    "2018A"        : "2018",
    "2018B1"       : "2018",
    "2018B2"       : "2018",
    "2018C"        : "2018",
    "2018D1"       : "2018",
    "2018D2"       : "2018"
}
reco_period_map = {
    "2016_preTS2"  : "2016_preTS2",
    "2016_postTS2" : "2016_postTS2",
    "2017B"        : "2017_preTS2",
    "2017C1"       : "2017_preTS2",
    "2017C2"       : "2017_preTS2",
    "2017D"        : "2017_preTS2",
    "2017E"        : "2017_postTS2",
    "2017F1"       : "2017_postTS2",
    "2017F2"       : "2017_postTS2",
    "2017F3"       : "2017_postTS2",
    "2018A"        : "2018_preTS1",
    "2018B1"       : "2018_TS1_TS2",
    "2018B2"       : "2018_TS1_TS2",
    "2018C"        : "2018_TS1_TS2",
    "2018D1"       : "2018_postTS2",
    "2018D2"       : "2018_postTS2"
}

# --- Fiducial Cuts (Identical) ---
def fiducial_cuts():
    # Per data period, arm=(0,1), station=(0,2)
    fiducialXLow_ = {}
    fiducialXHigh_ = {}
    fiducialYLow_ = {}
    fiducialYHigh_ = {}

    # Corrected: Include all periods used in fiducial_cuts_all
    data_periods = [
        "2017B", "2017C1", "2017C2", "2017D", "2017E", "2017F1", "2017F2", "2017F3",
        "2018A", "2018B1", "2018B2", "2018C", "2018D1", "2018D2"
    ]

    for period_ in data_periods:
        fiducialXLow_[ period_ ] = {}
        fiducialXLow_[ period_ ][ 0 ] = {}
        fiducialXLow_[ period_ ][ 1 ] = {}
        fiducialXHigh_[ period_ ] = {}
        fiducialXHigh_[ period_ ][ 0 ] = {}
        fiducialXHigh_[ period_ ][ 1 ] = {}
        fiducialYLow_[ period_ ] = {}
        fiducialYLow_[ period_ ][ 0 ] = {}
        fiducialYLow_[ period_ ][ 1 ] = {}
        fiducialYHigh_[ period_ ] = {}
        fiducialYHigh_[ period_ ][ 0 ] = {}
        fiducialYHigh_[ period_ ][ 1 ] = {}
        # Initialize station 2 for all periods to avoid KeyErrors later if not explicitly set
        for arm_ in [0, 1]:
             fiducialXLow_[period_][arm_][2] = np.nan
             fiducialXHigh_[period_][arm_][2] = np.nan
             fiducialYLow_[period_][arm_][2] = np.nan
             fiducialYHigh_[period_][arm_][2] = np.nan
             # Initialize station 0 for 2018 as well
             if period_.startswith("2018"):
                  fiducialXLow_[period_][arm_][0] = np.nan
                  fiducialXHigh_[period_][arm_][0] = np.nan
                  fiducialYLow_[period_][arm_][0] = np.nan
                  fiducialYHigh_[period_][arm_][0] = np.nan


    # 2017B
    fiducialXLow_[ "2017B" ][ 0 ][ 2 ]  =   1.995; fiducialXHigh_[ "2017B" ][ 0 ][ 2 ] =  24.479
    fiducialYLow_[ "2017B" ][ 0 ][ 2 ]  = -11.098; fiducialYHigh_[ "2017B" ][ 0 ][ 2 ] =   4.298
    fiducialXLow_[ "2017B" ][ 1 ][ 2 ]  =   2.422; fiducialXHigh_[ "2017B" ][ 1 ][ 2 ] =  24.620
    fiducialYLow_[ "2017B" ][ 1 ][ 2 ]  = -10.698; fiducialYHigh_[ "2017B" ][ 1 ][ 2 ] =   4.698

    # 2017C1
    fiducialXLow_[ "2017C1" ][ 0 ][ 2 ]  =   1.860; fiducialXHigh_[ "2017C1" ][ 0 ][ 2 ] =  24.334
    fiducialYLow_[ "2017C1" ][ 0 ][ 2 ]  = -11.098; fiducialYHigh_[ "2017C1" ][ 0 ][ 2 ] =   4.298
    fiducialXLow_[ "2017C1" ][ 1 ][ 2 ]  =   2.422; fiducialXHigh_[ "2017C1" ][ 1 ][ 2 ] =  24.620
    fiducialYLow_[ "2017C1" ][ 1 ][ 2 ]  = -10.698; fiducialYHigh_[ "2017C1" ][ 1 ][ 2 ] =   4.698

    # 2017C2 - Assuming same as C1 if not specified, adjust if needed
    fiducialXLow_[ "2017C2" ][ 0 ][ 2 ]  = fiducialXLow_[ "2017C1" ][ 0 ][ 2 ]; fiducialXHigh_[ "2017C2" ][ 0 ][ 2 ] = fiducialXHigh_[ "2017C1" ][ 0 ][ 2 ]
    fiducialYLow_[ "2017C2" ][ 0 ][ 2 ]  = fiducialYLow_[ "2017C1" ][ 0 ][ 2 ]; fiducialYHigh_[ "2017C2" ][ 0 ][ 2 ] = fiducialYHigh_[ "2017C1" ][ 0 ][ 2 ]
    fiducialXLow_[ "2017C2" ][ 1 ][ 2 ]  = fiducialXLow_[ "2017C1" ][ 1 ][ 2 ]; fiducialXHigh_[ "2017C2" ][ 1 ][ 2 ] = fiducialXHigh_[ "2017C1" ][ 1 ][ 2 ]
    fiducialYLow_[ "2017C2" ][ 1 ][ 2 ]  = fiducialYLow_[ "2017C1" ][ 1 ][ 2 ]; fiducialYHigh_[ "2017C2" ][ 1 ][ 2 ] = fiducialYHigh_[ "2017C1" ][ 1 ][ 2 ]

    # 2017D - Assuming same as C1 if not specified, adjust if needed
    fiducialXLow_[ "2017D" ][ 0 ][ 2 ]  = fiducialXLow_[ "2017C1" ][ 0 ][ 2 ]; fiducialXHigh_[ "2017D" ][ 0 ][ 2 ] = fiducialXHigh_[ "2017C1" ][ 0 ][ 2 ]
    fiducialYLow_[ "2017D" ][ 0 ][ 2 ]  = fiducialYLow_[ "2017C1" ][ 0 ][ 2 ]; fiducialYHigh_[ "2017D" ][ 0 ][ 2 ] = fiducialYHigh_[ "2017C1" ][ 0 ][ 2 ]
    fiducialXLow_[ "2017D" ][ 1 ][ 2 ]  = fiducialXLow_[ "2017C1" ][ 1 ][ 2 ]; fiducialXHigh_[ "2017D" ][ 1 ][ 2 ] = fiducialXHigh_[ "2017C1" ][ 1 ][ 2 ]
    fiducialYLow_[ "2017D" ][ 1 ][ 2 ]  = fiducialYLow_[ "2017C1" ][ 1 ][ 2 ]; fiducialYHigh_[ "2017D" ][ 1 ][ 2 ] = fiducialYHigh_[ "2017C1" ][ 1 ][ 2 ]

    # 2017E
    fiducialXLow_[ "2017E" ][ 0 ][ 2 ]  =   1.995; fiducialXHigh_[ "2017E" ][ 0 ][ 2 ] =  24.479
    fiducialYLow_[ "2017E" ][ 0 ][ 2 ]  = -10.098; fiducialYHigh_[ "2017E" ][ 0 ][ 2 ] =   4.998
    fiducialXLow_[ "2017E" ][ 1 ][ 2 ]  =  2.422; fiducialXHigh_[ "2017E" ][ 1 ][ 2 ] = 24.620
    fiducialYLow_[ "2017E" ][ 1 ][ 2 ]  = -9.698; fiducialYHigh_[ "2017E" ][ 1 ][ 2 ] =  5.398

    # 2017F1
    fiducialXLow_[ "2017F1" ][ 0 ][ 2 ]  =   1.995; fiducialXHigh_[ "2017F1" ][ 0 ][ 2 ] =  24.479
    fiducialYLow_[ "2017F1" ][ 0 ][ 2 ]  = -10.098; fiducialYHigh_[ "2017F1" ][ 0 ][ 2 ] =   4.998
    fiducialXLow_[ "2017F1" ][ 1 ][ 2 ]  =  2.422; fiducialXHigh_[ "2017F1" ][ 1 ][ 2 ] = 24.620
    fiducialYLow_[ "2017F1" ][ 1 ][ 2 ]  = -9.698; fiducialYHigh_[ "2017F1" ][ 1 ][ 2 ] =  5.398

    # 2017F2 - Assuming same as F1, adjust if needed
    fiducialXLow_[ "2017F2" ][ 0 ][ 2 ]  = fiducialXLow_[ "2017F1" ][ 0 ][ 2 ]; fiducialXHigh_[ "2017F2" ][ 0 ][ 2 ] = fiducialXHigh_[ "2017F1" ][ 0 ][ 2 ]
    fiducialYLow_[ "2017F2" ][ 0 ][ 2 ]  = fiducialYLow_[ "2017F1" ][ 0 ][ 2 ]; fiducialYHigh_[ "2017F2" ][ 0 ][ 2 ] = fiducialYHigh_[ "2017F1" ][ 0 ][ 2 ]
    fiducialXLow_[ "2017F2" ][ 1 ][ 2 ]  = fiducialXLow_[ "2017F1" ][ 1 ][ 2 ]; fiducialXHigh_[ "2017F2" ][ 1 ][ 2 ] = fiducialXHigh_[ "2017F1" ][ 1 ][ 2 ]
    fiducialYLow_[ "2017F2" ][ 1 ][ 2 ]  = fiducialYLow_[ "2017F1" ][ 1 ][ 2 ]; fiducialYHigh_[ "2017F2" ][ 1 ][ 2 ] = fiducialYHigh_[ "2017F1" ][ 1 ][ 2 ]

    # 2017F3 - Assuming same as F1, adjust if needed
    fiducialXLow_[ "2017F3" ][ 0 ][ 2 ]  = fiducialXLow_[ "2017F1" ][ 0 ][ 2 ]; fiducialXHigh_[ "2017F3" ][ 0 ][ 2 ] = fiducialXHigh_[ "2017F1" ][ 0 ][ 2 ]
    fiducialYLow_[ "2017F3" ][ 0 ][ 2 ]  = fiducialYLow_[ "2017F1" ][ 0 ][ 2 ]; fiducialYHigh_[ "2017F3" ][ 0 ][ 2 ] = fiducialYHigh_[ "2017F1" ][ 0 ][ 2 ]
    fiducialXLow_[ "2017F3" ][ 1 ][ 2 ]  = fiducialXLow_[ "2017F1" ][ 1 ][ 2 ]; fiducialXHigh_[ "2017F3" ][ 1 ][ 2 ] = fiducialXHigh_[ "2017F1" ][ 1 ][ 2 ]
    fiducialYLow_[ "2017F3" ][ 1 ][ 2 ]  = fiducialYLow_[ "2017F1" ][ 1 ][ 2 ]; fiducialYHigh_[ "2017F3" ][ 1 ][ 2 ] = fiducialYHigh_[ "2017F1" ][ 1 ][ 2 ]

    # 2018A
    fiducialXLow_[ "2018A" ][ 0 ][ 0 ]  =   2.850; fiducialXHigh_[ "2018A" ][ 0 ][ 0 ] =  17.927
    fiducialYLow_[ "2018A" ][ 0 ][ 0 ]  = -11.598; fiducialYHigh_[ "2018A" ][ 0 ][ 0 ] =   3.698
    fiducialXLow_[ "2018A" ][ 0 ][ 2 ]  =   2.421; fiducialXHigh_[ "2018A" ][ 0 ][ 2 ] =  24.620
    fiducialYLow_[ "2018A" ][ 0 ][ 2 ]  = -10.898; fiducialYHigh_[ "2018A" ][ 0 ][ 2 ] =   4.398
    fiducialXLow_[ "2018A" ][ 1 ][ 0 ]  =   3.275; fiducialXHigh_[ "2018A" ][ 1 ][ 0 ] =  18.498
    fiducialYLow_[ "2018A" ][ 1 ][ 0 ]  = -11.298; fiducialYHigh_[ "2018A" ][ 1 ][ 0 ] =   3.298
    fiducialXLow_[ "2018A" ][ 1 ][ 2 ]  =   2.421; fiducialXHigh_[ "2018A" ][ 1 ][ 2 ] =  20.045
    fiducialYLow_[ "2018A" ][ 1 ][ 2 ]  = -10.398; fiducialYHigh_[ "2018A" ][ 1 ][ 2 ] =   5.098

    # 2018B1
    fiducialXLow_[ "2018B1" ][ 0 ][ 0 ]  =   2.850; fiducialXHigh_[ "2018B1" ][ 0 ][ 0 ] =  17.927
    fiducialYLow_[ "2018B1" ][ 0 ][ 0 ]  = -11.598; fiducialYHigh_[ "2018B1" ][ 0 ][ 0 ] =   3.698
    fiducialXLow_[ "2018B1" ][ 0 ][ 2 ]  =   2.421; fiducialXHigh_[ "2018B1" ][ 0 ][ 2 ] =  24.620
    fiducialYLow_[ "2018B1" ][ 0 ][ 2 ]  = -10.898; fiducialYHigh_[ "2018B1" ][ 0 ][ 2 ] =   4.198
    fiducialXLow_[ "2018B1" ][ 1 ][ 0 ]  =   3.275; fiducialXHigh_[ "2018B1" ][ 1 ][ 0 ] =  18.070
    fiducialYLow_[ "2018B1" ][ 1 ][ 0 ]  = -11.198; fiducialYHigh_[ "2018B1" ][ 1 ][ 0 ] =   4.098
    fiducialXLow_[ "2018B1" ][ 1 ][ 2 ]  =   2.564; fiducialXHigh_[ "2018B1" ][ 1 ][ 2 ] =  20.045
    fiducialYLow_[ "2018B1" ][ 1 ][ 2 ]  = -10.398; fiducialYHigh_[ "2018B1" ][ 1 ][ 2 ] =   5.098

    # 2018B2
    fiducialXLow_[ "2018B2" ][ 0 ][ 0 ]  =   2.564; fiducialXHigh_[ "2018B2" ][ 0 ][ 0 ] =  17.640
    fiducialYLow_[ "2018B2" ][ 0 ][ 0 ]  = -11.598; fiducialYHigh_[ "2018B2" ][ 0 ][ 0 ] =   4.198
    fiducialXLow_[ "2018B2" ][ 0 ][ 2 ]  =   2.140; fiducialXHigh_[ "2018B2" ][ 0 ][ 2 ] =  24.479
    fiducialYLow_[ "2018B2" ][ 0 ][ 2 ]  = -11.398; fiducialYHigh_[ "2018B2" ][ 0 ][ 2 ] =   3.798
    fiducialXLow_[ "2018B2" ][ 1 ][ 0 ]  =   3.275; fiducialXHigh_[ "2018B2" ][ 1 ][ 0 ] =  17.931
    fiducialYLow_[ "2018B2" ][ 1 ][ 0 ]  = -10.498; fiducialYHigh_[ "2018B2" ][ 1 ][ 0 ] =   4.098
    fiducialXLow_[ "2018B2" ][ 1 ][ 2 ]  =   2.279; fiducialXHigh_[ "2018B2" ][ 1 ][ 2 ] =  24.760
    fiducialYLow_[ "2018B2" ][ 1 ][ 2 ]  = -10.598; fiducialYHigh_[ "2018B2" ][ 1 ][ 2 ] =   4.498

    # 2018C
    fiducialXLow_[ "2018C" ][ 0 ][ 0 ]  =   2.564; fiducialXHigh_[ "2018C" ][ 0 ][ 0 ] =  17.930
    fiducialYLow_[ "2018C" ][ 0 ][ 0 ]  = -11.098; fiducialYHigh_[ "2018C" ][ 0 ][ 0 ] =   4.198
    fiducialXLow_[ "2018C" ][ 0 ][ 2 ]  =   2.421; fiducialXHigh_[ "2018C" ][ 0 ][ 2 ] =  24.620
    fiducialYLow_[ "2018C" ][ 0 ][ 2 ]  = -11.398; fiducialYHigh_[ "2018C" ][ 0 ][ 2 ] =   3.698
    fiducialXLow_[ "2018C" ][ 1 ][ 0 ]  =   3.275; fiducialXHigh_[ "2018C" ][ 1 ][ 0 ] =  17.931
    fiducialYLow_[ "2018C" ][ 1 ][ 0 ]  = -10.498; fiducialYHigh_[ "2018C" ][ 1 ][ 0 ] =   4.698
    fiducialXLow_[ "2018C" ][ 1 ][ 2 ]  =   2.279; fiducialXHigh_[ "2018C" ][ 1 ][ 2 ] =  24.760
    fiducialYLow_[ "2018C" ][ 1 ][ 2 ]  = -10.598; fiducialYHigh_[ "2018C" ][ 1 ][ 2 ] =   4.398

    # 2018D1
    fiducialXLow_[ "2018D1" ][ 0 ][ 0 ]  =   2.850; fiducialXHigh_[ "2018D1" ][ 0 ][ 0 ] =  17.931
    fiducialYLow_[ "2018D1" ][ 0 ][ 0 ]  = -11.098; fiducialYHigh_[ "2018D1" ][ 0 ][ 0 ] =   4.098
    fiducialXLow_[ "2018D1" ][ 0 ][ 2 ]  =   2.421; fiducialXHigh_[ "2018D1" ][ 0 ][ 2 ] =  24.620
    fiducialYLow_[ "2018D1" ][ 0 ][ 2 ]  = -11.398; fiducialYHigh_[ "2018D1" ][ 0 ][ 2 ] =   3.698
    fiducialXLow_[ "2018D1" ][ 1 ][ 0 ]  =   3.275; fiducialXHigh_[ "2018D1" ][ 1 ][ 0 ] =  17.931
    fiducialYLow_[ "2018D1" ][ 1 ][ 0 ]  = -10.498; fiducialYHigh_[ "2018D1" ][ 1 ][ 0 ] =   4.698
    fiducialXLow_[ "2018D1" ][ 1 ][ 2 ]  =   2.279; fiducialXHigh_[ "2018D1" ][ 1 ][ 2 ] =  24.760
    fiducialYLow_[ "2018D1" ][ 1 ][ 2 ]  = -10.598; fiducialYHigh_[ "2018D1" ][ 1 ][ 2 ] =   4.398

    # 2018D2
    fiducialXLow_[ "2018D2" ][ 0 ][ 0 ]  =   2.850; fiducialXHigh_[ "2018D2" ][ 0 ][ 0 ] =  17.931
    fiducialYLow_[ "2018D2" ][ 0 ][ 0 ]  = -10.598; fiducialYHigh_[ "2018D2" ][ 0 ][ 0 ] =   4.498
    fiducialXLow_[ "2018D2" ][ 0 ][ 2 ]  =   2.421; fiducialXHigh_[ "2018D2" ][ 0 ][ 2 ] =  24.620
    fiducialYLow_[ "2018D2" ][ 0 ][ 2 ]  = -11.698; fiducialYHigh_[ "2018D2" ][ 0 ][ 2 ] =   3.298
    fiducialXLow_[ "2018D2" ][ 1 ][ 0 ]  =  3.275; fiducialXHigh_[ "2018D2" ][ 1 ][ 0 ] = 17.931
    fiducialYLow_[ "2018D2" ][ 1 ][ 0 ]  = -9.998; fiducialYHigh_[ "2018D2" ][ 1 ][ 0 ] =  4.698
    fiducialXLow_[ "2018D2" ][ 1 ][ 2 ]  =   2.279; fiducialXHigh_[ "2018D2" ][ 1 ][ 2 ] =  24.760
    fiducialYLow_[ "2018D2" ][ 1 ][ 2 ]  = -10.598; fiducialYHigh_[ "2018D2" ][ 1 ][ 2 ] =   3.898

    return (  fiducialXLow_, fiducialXHigh_, fiducialYLow_, fiducialYHigh_ )

def fiducial_cuts_all( data_sample ):
    data_periods = None
    if data_sample == '2017':
        data_periods = [ "2017B", "2017C1", "2017C2", "2017D", "2017E", "2017F1", "2017F2", "2017F3" ]
    elif data_sample == '2018':
        data_periods = [ "2018A", "2018B1", "2018B2", "2018C", "2018D1", "2018D2" ]
    else:
        raise ValueError(f"Unknown data_sample: {data_sample}")

    fiducialXLow_, fiducialXHigh_, fiducialYLow_, fiducialYHigh_ = fiducial_cuts()
    print ( "Individual fiducial cuts:", fiducialXLow_, fiducialXHigh_, fiducialYLow_, fiducialYHigh_ )

    # Per data period, arm=(0,1), station=(0,2)
    fiducialXLow_all = {}
    fiducialXHigh_all = {}
    fiducialYLow_all = {}
    fiducialYHigh_all = {}
    for arm_ in (0,1):
        fiducialXLow_all[ arm_ ] = {}
        fiducialXLow_all[ arm_ ][ 2 ] = []
        fiducialXHigh_all[ arm_ ] = {}
        fiducialXHigh_all[ arm_ ][ 2 ] = []
        fiducialYLow_all[ arm_ ] = {}
        fiducialYLow_all[ arm_ ][ 2 ] = []
        fiducialYHigh_all[ arm_ ] = {}
        fiducialYHigh_all[ arm_ ][ 2 ] = []

    for period_ in data_periods:
        for arm_ in (0,1):
             # Check if the key exists before appending
            if 2 in fiducialXLow_[period_][arm_]:
                 fiducialXLow_all[ arm_ ][ 2 ].append( fiducialXLow_[ period_ ][ arm_][ 2 ] )
                 fiducialXHigh_all[ arm_ ][ 2 ].append( fiducialXHigh_[ period_ ][ arm_][ 2 ] )
                 fiducialYLow_all[ arm_ ][ 2 ].append( fiducialYLow_[ period_ ][ arm_][ 2 ] )
                 fiducialYHigh_all[ arm_ ][ 2 ].append( fiducialYHigh_[ period_ ][ arm_][ 2 ] )
            else:
                 print(f"Warning: Station 2 cuts not found for period {period_}, arm {arm_}. Skipping.")


    for arm_ in (0,1):
        # Filter out potential NaNs introduced earlier before aggregation
        fiducialXLow_all[ arm_ ][ 2 ] = [x for x in fiducialXLow_all[ arm_ ][ 2 ] if pd.notna(x)]
        fiducialXHigh_all[ arm_ ][ 2 ] = [x for x in fiducialXHigh_all[ arm_ ][ 2 ] if pd.notna(x)]
        fiducialYLow_all[ arm_ ][ 2 ] = [x for x in fiducialYLow_all[ arm_ ][ 2 ] if pd.notna(x)]
        fiducialYHigh_all[ arm_ ][ 2 ] = [x for x in fiducialYHigh_all[ arm_ ][ 2 ] if pd.notna(x)]

        # Ensure list is not empty before calculating max/min
        if fiducialXLow_all[ arm_ ][ 2 ]:
             fiducialXLow_all[ arm_ ][ 2 ] = np.max( fiducialXLow_all[ arm_ ][ 2 ] )
             fiducialXHigh_all[ arm_ ][ 2 ] = np.min( fiducialXHigh_all[ arm_ ][ 2 ] )
             fiducialYLow_all[ arm_ ][ 2 ] = np.max( fiducialYLow_all[ arm_ ][ 2 ] )
             fiducialYHigh_all[ arm_ ][ 2 ] = np.min( fiducialYHigh_all[ arm_ ][ 2 ] )
        else: # Handle cases where no valid cuts were found
             fiducialXLow_all[ arm_ ][ 2 ] = np.nan
             fiducialXHigh_all[ arm_ ][ 2 ] = np.nan
             fiducialYLow_all[ arm_ ][ 2 ] = np.nan
             fiducialYHigh_all[ arm_ ][ 2 ] = np.nan

    print ( "Combined fiducial cuts:", fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all )

    return ( fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all )

# --- Aperture Parametrisation (Helper function - Identical) ---
def aperture_parametrisation( period, arm, xangle, xi ):
    #https://github.com/cms-sw/cmssw/tree/916cb3d20213734a0465240720c8c8c392b92eac/Validation/CTPPS/python/simu_config
    if (period == "2016_preTS2"):
        if   (arm == 0): return 3.76296E-05+((xi<0.117122)*0.00712775+(xi>=0.117122)*0.0148651)*(xi-0.117122);
        elif (arm == 1): return 1.85954E-05+((xi<0.14324)*0.00475349+(xi>=0.14324)*0.00629514)*(xi-0.14324);
    elif (period == "2016_postTS2"):
        if   (arm == 0): return 6.10374E-05+((xi<0.113491)*0.00795942+(xi>=0.113491)*0.01935)*(xi-0.113491);
        elif (arm == 1): return (xi-0.110)/130.0;
    elif (period == "2017_preTS2"):
        if   (arm == 0): return -(8.71198E-07*xangle-0.000134726)+((xi<(0.000264704*xangle+0.081951))*-(4.32065E-05*xangle-0.0130746)+(xi>=(0.000264704*xangle+0.081951))*-(0.000183472*xangle-0.0395241))*(xi-(0.000264704*xangle+0.081951));
        elif (arm == 1): return 3.43116E-05+((xi<(0.000626936*xangle+0.061324))*0.00654394+(xi>=(0.000626936*xangle+0.061324))*-(0.000145164*xangle-0.0272919))*(xi-(0.000626936*xangle+0.061324));
    elif (period == "2017_postTS2"):
        if   (arm == 0): return -(8.92079E-07*xangle-0.000150214)+((xi<(0.000278622*xangle+0.0964383))*-(3.9541e-05*xangle-0.0115104)+(xi>=(0.000278622*xangle+0.0964383))*-(0.000108249*xangle-0.0249303))*(xi-(0.000278622*xangle+0.0964383));
        elif (arm == 1): return 4.56961E-05+((xi<(0.00075625*xangle+0.0643361))*-(3.01107e-05*xangle-0.00985126)+(xi>=(0.00075625*xangle+0.0643361))*-(8.95437e-05*xangle-0.0169474))*(xi-(0.00075625*xangle+0.0643361));
    elif (period == "2018"):
        if   (arm == 0): return -(8.44219E-07*xangle-0.000100957)+((xi<(0.000247185*xangle+0.101599))*-(1.40289E-05*xangle-0.00727237)+(xi>=(0.000247185*xangle+0.101599))*-(0.000107811*xangle-0.0261867))*(xi-(0.000247185*xangle+0.101599));
        elif (arm == 1): return -(-4.74758E-07*xangle+3.0881E-05)+((xi<(0.000727859*xangle+0.0722653))*-(2.43968E-05*xangle-0.0085461)+(xi>=(0.000727859*xangle+0.0722653))*-(7.19216E-05*xangle-0.0148267))*(xi-(0.000727859*xangle+0.0722653));
    else:
        # Return NaN or raise error for unknown periods? Returning NaN for safety.
        return np.nan # Was -999


def polars_aperture_parametrisation_expr(period_col, arm_col, xangle_col, xi_col):
    expr = None

    def cond(x): return x  # Só para clareza

    def arm_expr(period, arm_id, expr_body):
        return pl.when((period_col == period) & (arm_col == arm_id)).then(expr_body)

    def segment_split_expr(threshold_expr, below_expr, above_expr):
        return pl.when(xi_col < threshold_expr).then(below_expr).otherwise(above_expr)

    # Agora, para cada período e braço:
    exprs = [
        # 2016_preTS2
        arm_expr("2016_preTS2", 0,
                 3.76296E-05 + segment_split_expr(
                     0.117122,
                     0.00712775 * (xi_col - 0.117122),
                     0.0148651 * (xi_col - 0.117122))
        ),
        arm_expr("2016_preTS2", 1,
                 1.85954E-05 + segment_split_expr(
                     0.14324,
                     0.00475349 * (xi_col - 0.14324),
                     0.00629514 * (xi_col - 0.14324))
        ),

        # 2016_postTS2
        arm_expr("2016_postTS2", 0,
                 6.10374E-05 + segment_split_expr(
                     0.113491,
                     0.00795942 * (xi_col - 0.113491),
                     0.01935 * (xi_col - 0.113491))
        ),
        arm_expr("2016_postTS2", 1,
                 (xi_col - 0.110) / 130.0
        ),

        # 2017_preTS2
        arm_expr("2017_preTS2", 0,
                 -(8.71198E-07 * xangle_col - 0.000134726) + segment_split_expr(
                     0.000264704 * xangle_col + 0.081951,
                     -(4.32065E-05 * xangle_col - 0.0130746) * (xi_col - (0.000264704 * xangle_col + 0.081951)),
                     -(0.000183472 * xangle_col - 0.0395241) * (xi_col - (0.000264704 * xangle_col + 0.081951))
                 )
        ),
        arm_expr("2017_preTS2", 1,
                 3.43116E-05 + segment_split_expr(
                     0.000626936 * xangle_col + 0.061324,
                     0.00654394 * (xi_col - (0.000626936 * xangle_col + 0.061324)),
                     -(0.000145164 * xangle_col - 0.0272919) * (xi_col - (0.000626936 * xangle_col + 0.061324))
                 )
        ),

        # 2017_postTS2
        arm_expr("2017_postTS2", 0,
                 -(8.92079E-07 * xangle_col - 0.000150214) + segment_split_expr(
                     0.000278622 * xangle_col + 0.0964383,
                     -(3.9541e-05 * xangle_col - 0.0115104) * (xi_col - (0.000278622 * xangle_col + 0.0964383)),
                     -(0.000108249 * xangle_col - 0.0249303) * (xi_col - (0.000278622 * xangle_col + 0.0964383))
                 )
        ),
        arm_expr("2017_postTS2", 1,
                 4.56961E-05 + segment_split_expr(
                     0.00075625 * xangle_col + 0.0643361,
                     -(3.01107e-05 * xangle_col - 0.00985126) * (xi_col - (0.00075625 * xangle_col + 0.0643361)),
                     -(8.95437e-05 * xangle_col - 0.0169474) * (xi_col - (0.00075625 * xangle_col + 0.0643361))
                 )
        ),

        # 2018
        arm_expr("2018", 0,
                 -(8.44219E-07 * xangle_col - 0.000100957) + segment_split_expr(
                     0.000247185 * xangle_col + 0.101599,
                     -(1.40289E-05 * xangle_col - 0.00727237) * (xi_col - (0.000247185 * xangle_col + 0.101599)),
                     -(0.000107811 * xangle_col - 0.0261867) * (xi_col - (0.000247185 * xangle_col + 0.101599))
                 )
        ),
        arm_expr("2018", 1,
                 -(-4.74758E-07 * xangle_col + 3.0881E-05) + segment_split_expr(
                     0.000727859 * xangle_col + 0.0722653,
                     -(2.43968E-05 * xangle_col - 0.0085461) * (xi_col - (0.000727859 * xangle_col + 0.0722653)),
                     -(7.19216E-05 * xangle_col - 0.0148267) * (xi_col - (0.000727859 * xangle_col + 0.0722653))
                 )
        ),
    ]

    # Junta todas as expressões com `.otherwise`
    expr = exprs[0]
    for sub_expr in exprs[1:]:
        expr = expr.otherwise(sub_expr)

    return expr.otherwise(pl.lit(np.nan))



def check_aperture( period, arm, xangle, xi, theta_x ):
    param = aperture_parametrisation( period, arm, xangle, xi )
    if pd.isna(param): # Check if parametrisation failed
        return False # Or handle appropriately
    return ( theta_x < -param )


# --- get_data Function (Largely unchanged, minor Polars adjustments) ---
def get_data( fileNames, selection=None, version="V1" ):

    if not version in ("V1", "V2"):
        print ( "Unsupported version: {}".format( version ) )
        exit()

    df_columns_types_ = {
        "Run": pl.Int64, "LumiSection": pl.Int64, "EventNum": pl.Int64,
        "Slice": pl.Int32, "MultiRP": pl.Int32, "Arm": pl.Int32,
        "RPId1": pl.Int32, "RPId2": pl.Int32,
        "TrackPixShift_SingleRP": pl.Int32, "Track1PixShift_MultiRP": pl.Int32, "Track2PixShift_MultiRP": pl.Int32,
        "nVertices": pl.Int32, "ExtraPfCands": pl.Int32,
        # Add other known float columns to avoid potential inference issues
        "CrossingAngle": pl.Float64, "PrimVertexZ": pl.Float64,
        "Muon0Pt": pl.Float64, "Muon0Eta": pl.Float64, "Muon0Phi": pl.Float64, "Muon0VtxZ": pl.Float64,
        "Muon1Pt": pl.Float64, "Muon1Eta": pl.Float64, "Muon1Phi": pl.Float64, "Muon1VtxZ": pl.Float64,
        "InvMass": pl.Float64, "Acopl": pl.Float64,
        "XiMuMuPlus": pl.Float64, "XiMuMuMinus": pl.Float64,
        "TrackX1": pl.Float64, "TrackY1": pl.Float64, "TrackX2": pl.Float64, "TrackY2": pl.Float64,
        "TrackThX_SingleRP": pl.Float64, "TrackThY_SingleRP": pl.Float64,
        "Track1ThX_MultiRP": pl.Float64, "Track1ThY_MultiRP": pl.Float64,
        "Track2ThX_MultiRP": pl.Float64, "Track2ThY_MultiRP": pl.Float64,
        "Xi": pl.Float64, "T": pl.Float64, "ThX": pl.Float64, "ThY": pl.Float64, "Time": pl.Float64,
        }

    if version == "V1":
        df_list = []
    elif version == "V2":
        df_protons_multiRP_list = []
        df_protons_singleRP_list = []

    df_counts_list = []

    for file_ in fileNames:
        print ( f"Processing file: {file_}" )
        try:
            with h5py.File( file_, 'r' ) as f:
                print ( f"Keys in HDF5 file: {list(f.keys())}" )

                dset = None
                dset_protons_multiRP = None
                dset_protons_singleRP = None

                if version == "V1":
                    if 'protons' not in f: raise KeyError("Dataset 'protons' not found in HDF5 file.")
                    dset = f['protons']
                    print ( f"Shape of 'protons' dataset: {dset.shape}" )
                elif version == "V2":
                    if 'protons_multiRP' not in f: raise KeyError("Dataset 'protons_multiRP' not found in HDF5 file.")
                    dset_protons_multiRP = f['protons_multiRP']
                    print ( f"Shape of 'protons_multiRP' dataset: {dset_protons_multiRP.shape}" )
                    if 'protons_singleRP' not in f: raise KeyError("Dataset 'protons_singleRP' not found in HDF5 file.")
                    dset_protons_singleRP = f['protons_singleRP']
                    print ( f"Shape of 'protons_singleRP' dataset: {dset_protons_singleRP.shape}" )

                if 'columns' not in f: raise KeyError("Dataset 'columns' not found in HDF5 file.")
                dset_columns = f['columns']
                columns = list( dset_columns )
                columns_str = [ item.decode("utf-8") for item in columns ]
                print ( f"Columns: {columns_str}" )

                if 'selections' not in f: raise KeyError("Dataset 'selections' not found in HDF5 file.")
                dset_selections = f['selections']
                selections_ = [ item.decode("utf-8") for item in dset_selections ]
                print ( f"Selections: {selections_}" )

                if 'event_counts' not in f: raise KeyError("Dataset 'event_counts' not found in HDF5 file.")
                dset_counts = f['event_counts']
                counts_array = np.array(dset_counts)
                # Create DataFrame for counts for this file
                if counts_array.ndim == 1 and len(counts_array) == len(selections_):
                     df_counts_file = pl.DataFrame({"selection": selections_, "counts": counts_array})
                elif counts_array.ndim == 2 and counts_array.shape[1] == 1 and counts_array.shape[0] == len(selections_): # Handle [[count1], [count2]] case
                     df_counts_file = pl.DataFrame({"selection": selections_, "counts": counts_array.flatten()})
                elif counts_array.ndim == 2 and counts_array.shape[1] == 2 and counts_array.shape[0] == len(selections_): # Handle [[sel, count], ...] case (unlikely based on pandas code)
                    # Assuming first column is selection index/name (matching selections_), second is count
                    # This case needs careful verification based on actual HDF5 structure
                     df_counts_file = pl.DataFrame(counts_array, schema=["selection_in_file", "counts"])
                     # Potentially map selection_in_file to selections_ if needed
                     # For now, assume the order matches selections_
                     df_counts_file = df_counts_file.with_columns(pl.Series("selection", selections_)).select(["selection", "counts"])
                else:
                    raise ValueError(f"Unexpected shape/format for 'event_counts': {counts_array.shape}, selections: {len(selections_)}")
                df_counts_list.append(df_counts_file)
                print(f"Counts for this file:\n{df_counts_list[-1]}")


                # Update types based on actual columns found
                current_df_types = {}
                if "Run_mc" in columns_str:
                    current_df_types["Run_mc"] = pl.Int64
                if "Run_rnd" in columns_str:
                    current_df_types["Run_rnd"] = pl.Int64
                    current_df_types["LumiSection_rnd"] = pl.Int64
                    current_df_types["EventNum_rnd"] = pl.Int64
                # Add other known types for columns present in this file
                for col, dtype in df_columns_types_.items():
                     if col in columns_str:
                          current_df_types[col] = dtype

                # Read main dataset(s) in chunks
                chunk_size = 1_000_000 # 1 million rows

                if version == "V1":
                    entries = dset.shape[0]
                    for start_ in range(0, entries, chunk_size):
                        stop_ = min(start_ + chunk_size, entries)
                        print (f"Reading V1 chunk: {start_} to {stop_}")
                        data = dset[start_:stop_]
                        data_dict = {col: data[:, i] for i, col in enumerate(columns_str)}
                        # Eagerly create DataFrame and cast types
                        df_chunk = pl.DataFrame(data_dict).with_columns([
                            pl.col(k).cast(v) for k, v in current_df_types.items() if k in columns_str
                        ])
                        if selection:
                            df_chunk = selection(df_chunk) # Apply selection if provided
                        df_list.append(df_chunk)
                        print(f" V1 Chunk head:\n{df_list[-1].head(2)}")
                        print(f" V1 Chunk length: {len(df_list[-1])}")

                elif version == "V2":
                    # Process multiRP
                    entries_multi = dset_protons_multiRP.shape[0]
                    for start_ in range(0, entries_multi, chunk_size):
                        stop_ = min(start_ + chunk_size, entries_multi)
                        print(f"Reading multiRP chunk: {start_} to {stop_}")
                        data = dset_protons_multiRP[start_:stop_]
                        data_dict = {col: data[:, i] for i, col in enumerate(columns_str)}
                        df_chunk = pl.DataFrame(data_dict).with_columns([
                            pl.col(k).cast(v) for k, v in current_df_types.items() if k in columns_str
                        ])
                        if selection:
                            df_chunk = selection(df_chunk)
                        df_protons_multiRP_list.append(df_chunk)
                        print(f" multiRP Chunk head:\n{df_protons_multiRP_list[-1].head(2)}")
                        print(f" multiRP Chunk length: {len(df_protons_multiRP_list[-1])}")

                    # Process singleRP
                    entries_single = dset_protons_singleRP.shape[0]
                    for start_ in range(0, entries_single, chunk_size):
                        stop_ = min(start_ + chunk_size, entries_single)
                        print(f"Reading singleRP chunk: {start_} to {stop_}")
                        data = dset_protons_singleRP[start_:stop_]
                        data_dict = {col: data[:, i] for i, col in enumerate(columns_str)}
                        df_chunk = pl.DataFrame(data_dict).with_columns([
                            pl.col(k).cast(v) for k, v in current_df_types.items() if k in columns_str
                        ])
                        if selection:
                            df_chunk = selection(df_chunk)
                        df_protons_singleRP_list.append(df_chunk)
                        print(f" singleRP Chunk head:\n{df_protons_singleRP_list[-1].head(2)}")
                        print(f" singleRP Chunk length: {len(df_protons_singleRP_list[-1])}")

        except FileNotFoundError:
             print(f"ERROR: File not found {file_}")
             continue # Skip to next file
        except KeyError as e:
             print(f"ERROR: Dataset missing in {file_}: {e}")
             continue # Skip to next file
        except Exception as e:
             print(f"ERROR: Failed to process file {file_}: {e}")
             continue # Skip to next file


    # Consolidate counts across all files
    if not df_counts_list:
         print("WARNING: No count data found in any processed file.")
         # Create an empty DataFrame with expected columns if needed downstream
         df_counts_total = pl.DataFrame({"selection": pl.Series(dtype=pl.Utf8), "counts": pl.Series(dtype=pl.Int64)})
    else:
         df_counts_concat = pl.concat(df_counts_list)
         df_counts_total = df_counts_concat.groupby("selection").agg(
             pl.col("counts").sum().cast(pl.Int64) # Ensure final count is Int64
         ).sort("selection")
    print(f"Total Counts:\n{df_counts_total}")

    # Consolidate main DataFrames
    if version == "V1":
        if not df_list:
             print("WARNING: No V1 data loaded.")
             return (df_counts_total, pl.DataFrame()) # Return empty DF
        df = pl.concat(df_list, rechunk=True)
        print(f"Final V1 DataFrame shape: {df.shape}\nHead:\n{df.head()}")
        return (df_counts_total, df)
    elif version == "V2":
        if not df_protons_multiRP_list:
             print("WARNING: No multiRP data loaded.")
             df_protons_multiRP = pl.DataFrame()
        else:
             df_protons_multiRP = pl.concat(df_protons_multiRP_list, rechunk=True)
             print(f"Final multiRP DataFrame shape: {df_protons_multiRP.shape}\nHead:\n{df_protons_multiRP.head()}")

        if not df_protons_singleRP_list:
             print("WARNING: No singleRP data loaded.")
             df_protons_singleRP = pl.DataFrame()
        else:
             df_protons_singleRP = pl.concat(df_protons_singleRP_list, rechunk=True)
             print(f"Final singleRP DataFrame shape: {df_protons_singleRP.shape}\nHead:\n{df_protons_singleRP.head()}")

        return (df_counts_total, df_protons_multiRP, df_protons_singleRP)


# --- process_data Function (Corrected) ---
def process_data( df, data_sample, lepton_type, proton_selection, min_mass=0., min_pt_1=-1, min_pt_2=-1, apply_fiducial=True, within_aperture=False, random_protons=False, mix_protons=False, runOnMC=False, select2protons=False ):

    if df.is_empty():
         print("Input DataFrame is empty, skipping processing.")
         # Return structure consistent with expected output format
         empty_events = pl.DataFrame()
         empty_protons = pl.DataFrame()
         if proton_selection == "SingleRP":
             return pl.DataFrame() # Returns df_index
         elif proton_selection == "MultiRP":
             if select2protons:
                 return (pl.DataFrame(), empty_events, empty_protons) # df_index, df_events_, df_2protons_
             else:
                 return (pl.DataFrame(), empty_protons) # df_index, df_ximax_


    if runOnMC:
        print ( "Turning within_aperture OFF for MC." )
        within_aperture = False

    # Base selection mask
    msk_base = ( pl.col("InvMass") >= min_mass )
    if min_pt_1 > 0: msk_base = msk_base & ( pl.col("Muon0Pt") >= min_pt_1 )
    if min_pt_2 > 0: msk_base = msk_base & ( pl.col("Muon1Pt") >= min_pt_2 )

    # Apply base selection early
    df = df.filter(msk_base)
    if df.is_empty():
         print("DataFrame empty after base selection (InvMass, MuonPt).")
         # Return empty structure matching expected output
         if proton_selection == "SingleRP": return pl.DataFrame()
         elif proton_selection == "MultiRP":
              if select2protons: return (pl.DataFrame(), pl.DataFrame(), pl.DataFrame())
              else: return (pl.DataFrame(), pl.DataFrame())


    # Fiducial cuts
    fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all = None, None, None, None
    if apply_fiducial:
        try:
             fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all = fiducial_cuts_all( data_sample )
        except ValueError as e:
             print(f"Error getting fiducial cuts: {e}. Skipping fiducial application.")
             apply_fiducial = False
        except KeyError as e:
             print(f"Error accessing fiducial cut dictionary key: {e}. Skipping fiducial application.")
             apply_fiducial = False


    track_angle_cut_ = 0.02

    # Determine Run and CrossingAngle columns
    run_str_ = "Run"
    if random_protons or mix_protons: run_str_ = "Run_rnd"
    elif runOnMC and not mix_protons: run_str_ = "Run_mc"

    xangle_str_ = "CrossingAngle"
    if random_protons or mix_protons: xangle_str_ = "CrossingAngle_rnd"

    # Check if needed columns exist
    required_cols = [run_str_, xangle_str_]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
         raise ValueError(f"Missing required columns in DataFrame: {missing_cols}")

    # Assign period
    df_run_ranges_ = None
    if data_sample == '2017': df_run_ranges_ = df_run_ranges_2017
    elif data_sample == '2018': df_run_ranges_ = df_run_ranges_2018
    else: raise ValueError(f"Unknown data_sample for run ranges: {data_sample}")

    print (f"Using run ranges:\n{df_run_ranges_}")

    if "period" not in df.columns:
        # Use join for potentially better performance than chained when()
        df_run_ranges_ = df_run_ranges_.rename({"min": f"{run_str_}_min", "max": f"{run_str_}_max"})

        # Create temporary columns for join condition
        df = df.with_columns(pl.col(run_str_).alias("__run_join_key__"))
        df_run_ranges_ = df_run_ranges_.with_columns(
             pl.int_ranges(pl.col(f"{run_str_}_min"), pl.col(f"{run_str_}_max") + 1).alias("__run_join_key__")
        ).explode("__run_join_key__") # Explode ranges to allow equi-join

        # Perform the join
        df = df.join(
             df_run_ranges_.select(["__run_join_key__", "period"]),
             on="__run_join_key__",
             how="left"
        ).drop("__run_join_key__")

        # Check counts per period
        print("Period counts after assignment:")
        print(df.groupby("period").agg(pl.count()))
        # Handle rows that didn't match any period (optional, fillna or filter)
        # df = df.fill_null(strategy="forward") # Or some default period? Or filter them out?

    # --- Proton Selection Logic ---
    msk_proton_selection = None # This will be the final mask

    if proton_selection == "SingleRP":
        msk_arm0 = (pl.col("RPId1") == 23)
        msk_arm1 = (pl.col("RPId1") == 123)
        msk_multiRP_flag = (pl.col("MultiRP") == 0)
        msk_pixshift = (pl.col("TrackPixShift_SingleRP") == 0)

        df = df.with_columns(
            pl.when(msk_arm0).then(pl.col("XiMuMuPlus"))
            .when(msk_arm1).then(pl.col("XiMuMuMinus"))
            .otherwise(None).alias("XiMuMu") # Assign XiMuMu based on arm
        )

        msk_combined = msk_multiRP_flag & msk_pixshift & (msk_arm0 | msk_arm1)

        if apply_fiducial and fiducialXLow_all:
            # Add fiducial limits based on arm
            df = df.with_columns([
                pl.when(msk_arm0).then(fiducialXLow_all[0][2])
                  .when(msk_arm1).then(fiducialXLow_all[1][2])
                  .otherwise(None).alias("xlow"),
                pl.when(msk_arm0).then(fiducialXHigh_all[0][2])
                  .when(msk_arm1).then(fiducialXHigh_all[1][2])
                  .otherwise(None).alias("xhigh"),
                pl.when(msk_arm0).then(fiducialYLow_all[0][2])
                  .when(msk_arm1).then(fiducialYLow_all[1][2])
                  .otherwise(None).alias("ylow"),
                pl.when(msk_arm0).then(fiducialYHigh_all[0][2])
                  .when(msk_arm1).then(fiducialYHigh_all[1][2])
                  .otherwise(None).alias("yhigh"),
            ])
            msk_fid = (
                (pl.col("TrackThX_SingleRP").abs() <= track_angle_cut_) &
                (pl.col("TrackThY_SingleRP").abs() <= track_angle_cut_) &
                (pl.col("TrackX1") >= pl.col("xlow")) &
                (pl.col("TrackX1") <= pl.col("xhigh")) &
                (pl.col("TrackY1") >= pl.col("ylow")) &
                (pl.col("TrackY1") <= pl.col("yhigh"))
            )
            msk_proton_selection = msk_combined & msk_fid
        else:
            msk_proton_selection = msk_combined

    elif proton_selection == "MultiRP":
        msk_arm0 = (pl.col("Arm") == 0)
        msk_arm1 = (pl.col("Arm") == 1)
        msk_multiRP_flag = (pl.col("MultiRP") == 1)
        msk_pixshift = (pl.col("Track1PixShift_MultiRP") == 0) & (pl.col("Track2PixShift_MultiRP") == 0)

        df = df.with_columns(
            pl.when(msk_arm0).then(pl.col("XiMuMuPlus"))
            .when(msk_arm1).then(pl.col("XiMuMuMinus"))
            .otherwise(None).alias("XiMuMu") # Assign XiMuMu based on arm
        )

        msk_combined = msk_multiRP_flag & msk_pixshift

        # Aperture check (vectorized placeholder)
        msk_aperture = pl.lit(True) # Default to True if not applying
        if within_aperture:
             # Map 'period' to 'aperture_period' using the dictionary
             df = df.with_columns(
                  pl.col("period").map_dict(aperture_period_map, default=None).alias("aperture_period")
             )
             # Check if aperture_period is null (period not in map)
             msk_valid_period = pl.col("aperture_period").is_not_null()

             # Calculate aperture limit using placeholder expression
             aperture_limit_expr = polars_aperture_parametrisation_expr(
                 pl.col("aperture_period"), pl.col("Arm"), pl.col(xangle_str_), pl.col("Xi")
             )
             msk_aperture = (
                  pl.when(msk_valid_period) # Only apply where period is valid
                  .then(pl.col("ThX") < -aperture_limit_expr)
                  .otherwise(False) # Fail aperture check if period is invalid or condition fails
             )
             df = df.with_columns(msk_aperture.alias("within_aperture_flag")) # Store flag if needed
             msk_combined = msk_combined & msk_aperture # Combine with main mask


        # Fiducial check
        msk_fid = pl.lit(True) # Default to True if not applying
        if apply_fiducial and fiducialXLow_all:
            df = df.with_columns([
                pl.when(msk_arm0).then(fiducialXLow_all[0][2])
                  .when(msk_arm1).then(fiducialXLow_all[1][2])
                  .otherwise(None).alias("xlow"),
                pl.when(msk_arm0).then(fiducialXHigh_all[0][2])
                  .when(msk_arm1).then(fiducialXHigh_all[1][2])
                  .otherwise(None).alias("xhigh"),
                pl.when(msk_arm0).then(fiducialYLow_all[0][2])
                  .when(msk_arm1).then(fiducialYLow_all[1][2])
                  .otherwise(None).alias("ylow"),
                pl.when(msk_arm0).then(fiducialYHigh_all[0][2])
                  .when(msk_arm1).then(fiducialYHigh_all[1][2])
                  .otherwise(None).alias("yhigh"),
            ])
            msk_fid = (
                (pl.col("Track2ThX_MultiRP").abs() <= track_angle_cut_) &
                (pl.col("Track2ThY_MultiRP").abs() <= track_angle_cut_) &
                (pl.col("TrackX2") >= pl.col("xlow")) &
                (pl.col("TrackX2") <= pl.col("xhigh")) &
                (pl.col("TrackY2") >= pl.col("ylow")) &
                (pl.col("TrackY2") <= pl.col("yhigh"))
            )
            msk_combined = msk_combined & msk_fid

        msk_proton_selection = msk_combined

    else:
        raise ValueError(f"Unknown proton_selection: {proton_selection}")

    # Apply final proton selection mask
    print ( f"Number of rows before final selection: {len(df)}" )
    df = df.filter( msk_proton_selection )
    print ( f"Number of rows after final selection: {len(df)}" )

    if df.is_empty():
         print("DataFrame empty after proton selection.")
         # Return empty structure matching expected output
         if proton_selection == "SingleRP": return pl.DataFrame()
         elif proton_selection == "MultiRP":
              if select2protons: return (pl.DataFrame(), pl.DataFrame(), pl.DataFrame())
              else: return (pl.DataFrame(), pl.DataFrame())


    # --- Efficiency Calculations ---
    columns_eff_ = [] # Track efficiency columns added for potential dropping later

    # Define expected efficiency columns for schema consistency
    base_eff_cols = {}
    data_periods_eff = []
    if data_sample == '2017':
         data_periods_eff = [ "2017B", "2017C1", "2017C2", "2017D", "2017E", "2017F1", "2017F2", "2017F3" ]
         for period_ in data_periods_eff:
              base_eff_cols[f'eff_proton_all_{period_}'] = np.nan
              base_eff_cols[f'eff_multitrack_{period_}'] = np.nan
              base_eff_cols[f'eff_strictzero_{period_}'] = np.nan
         base_eff_cols.update({
            'eff_proton_all_weighted': 0.0, 'eff_multitrack_weighted': 0.0, 'eff_strictzero_weighted': 0.0,
            'eff_proton_all': np.nan, 'eff_multitrack': np.nan, 'eff_strictzero': np.nan,
            'eff_proton_unc': np.nan
         })
    elif data_sample == '2018':
         data_periods_eff = [ "2018A", "2018B1", "2018B2", "2018C", "2018D1", "2018D2" ]
         for period_ in data_periods_eff:
              base_eff_cols[f'eff_proton_all_{period_}'] = np.nan
         base_eff_cols.update({
            'eff_proton_all_weighted': 0.0,
            'eff_proton_all': np.nan,
            'eff_proton_unc': np.nan
         })

    # Ensure base efficiency columns exist, initializing if needed
    df = df.with_columns([
         pl.lit(value).cast(pl.Float64).alias(name)
         for name, value in base_eff_cols.items() if name not in df.columns
    ])

    if runOnMC and not mix_protons and proton_selection == "MultiRP":
        if not PROTON_EFFICIENCY_AVAILABLE:
            print("SKIPPING MC efficiency calculation as proton_efficiency module or ROOT is unavailable.")
            # Ensure columns exist with default values (1.0 for efficiencies, 0.0 for unc)
            default_vals = {col: 1.0 for col in base_eff_cols if 'unc' not in col}
            default_vals.update({col: 0.0 for col in base_eff_cols if 'unc' in col})
            df = df.with_columns([

                pl.lit(default_vals[name]).cast(pl.Float64).alias(name) for name in base_eff_cols
                ])
            columns_eff_.extend(base_eff_cols.keys()) # Add columns for potential drop
        else:
            # --- Block requiring Pandas due to ROOT dependency ---
            print("WARNING: Converting Polars DataFrame to Pandas for MC efficiency calculation due to ROOT dependency. This is a performance bottleneck.")
            df_pd = df.to_pandas()
            columns_eff_ = [] # Reset columns_eff_ list

            if data_sample == '2017':
                strips_multitrack_efficiency, strips_sensor_efficiency, multiRP_efficiency, _, _ = efficiencies_2017()
                sz_efficiencies = strict_zero_efficiencies()
                lumi_periods_ = lumi_periods_2017.get(lepton_type)
                proton_eff_unc_per_arm_ = proton_efficiency_uncertainty["2017"]
                data_periods = data_periods_eff
            
                for period_ in data_periods:
                    if period_ not in lumi_periods_:
                        continue
            
                    def f_eff_strips_multitrack_(r):
                        return strips_multitrack_efficiency.get(period_, {}).get("45" if r["Arm"] == 0 else "56", ROOT.TH1D()).GetBinContent(1) \
                            if period_ in strips_multitrack_efficiency else np.nan
            
                    def f_eff_strips_sensor_(r):
                        if period_ in strips_sensor_efficiency:
                            th2 = strips_sensor_efficiency[period_]["45" if r["Arm"] == 0 else "56"]
                            return th2.GetBinContent(th2.FindBin(r["TrackX2"], r["TrackY2"]))
                        return np.nan
            
                    def f_eff_multiRP_(r):
                        if period_ in multiRP_efficiency:
                            th2 = multiRP_efficiency[period_]["45" if r["Arm"] == 0 else "56"]
                            return th2.GetBinContent(th2.FindBin(r["TrackX1"], r["TrackY1"]))
                        return np.nan
            
                    def f_eff_strips_strictzero_(r):
                        if period_ in sz_efficiencies:
                            arm = "45" if r["Arm"] == 0 else "56"
                            xangle_bin = int((r[xangle_str_] // 10) * 10)
                            return sz_efficiencies[period_].get(arm, {}).get(xangle_bin, np.nan)
                        return np.nan
            
                    def f_eff_proton_all_(r):
                        ss = f_eff_strips_sensor_(r)
                        mr = f_eff_multiRP_(r)
                        return ss * mr if pd.notna(ss) and pd.notna(mr) else np.nan
            
                    df_pd['eff_proton_all_' + period_] = df_pd.apply(f_eff_proton_all_, axis=1)
                    df_pd['eff_proton_all_weighted'] += lumi_periods_[period_] * df_pd['eff_proton_all_' + period_].fillna(0)
                    df_pd['eff_multitrack_' + period_] = df_pd.apply(f_eff_strips_multitrack_, axis=1)
                    df_pd['eff_multitrack_weighted'] += lumi_periods_[period_] * df_pd['eff_multitrack_' + period_].fillna(0)
                    df_pd['eff_strictzero_' + period_] = df_pd.apply(f_eff_strips_strictzero_, axis=1)
                    df_pd['eff_strictzero_weighted'] += lumi_periods_[period_] * df_pd['eff_strictzero_' + period_].fillna(0)
            
                    columns_eff_.extend([
                        f'eff_proton_all_{period_}',
                        f'eff_multitrack_{period_}',
                        f'eff_strictzero_{period_}'
                    ])
            
                lumi_total = np.sum(list(lumi_periods_.values()))
                if lumi_total > 0:
                    df_pd['eff_proton_all_weighted'] /= lumi_total
                    df_pd['eff_multitrack_weighted'] /= lumi_total
                    df_pd['eff_strictzero_weighted'] /= lumi_total
            
                # Non-weighted efficiencies
                def f_eff_strips_multitrack_(r):
                    return strips_multitrack_efficiency.get(r["period"], {}).get("45" if r["Arm"] == 0 else "56", ROOT.TH1D()).GetBinContent(1) \
                        if pd.notna(r["period"]) and r["period"] in strips_multitrack_efficiency else np.nan
            
                def f_eff_strips_sensor_(r):
                    if pd.notna(r["period"]) and r["period"] in strips_sensor_efficiency:
                        th2 = strips_sensor_efficiency[r["period"]]["45" if r["Arm"] == 0 else "56"]
                        return th2.GetBinContent(th2.FindBin(r["TrackX2"], r["TrackY2"]))
                    return np.nan
            
                def f_eff_multiRP_(r):
                    if pd.notna(r["period"]) and r["period"] in multiRP_efficiency:
                        th2 = multiRP_efficiency[r["period"]]["45" if r["Arm"] == 0 else "56"]
                        return th2.GetBinContent(th2.FindBin(r["TrackX1"], r["TrackY1"]))
                    return np.nan
            
                def f_eff_strips_strictzero_(r):
                    if pd.notna(r["period"]) and r["period"] in sz_efficiencies:
                        arm = "45" if r["Arm"] == 0 else "56"
                        xangle_bin = int((r[xangle_str_] // 10) * 10)
                        return sz_efficiencies[r["period"]].get(arm, {}).get(xangle_bin, np.nan)
                    return np.nan
            
                def f_eff_proton_all_(r):
                    ss = f_eff_strips_sensor_(r)
                    mr = f_eff_multiRP_(r)
                    return ss * mr if pd.notna(ss) and pd.notna(mr) else np.nan
            
                df_pd['eff_proton_all'] = df_pd.apply(f_eff_proton_all_, axis=1)
                df_pd['eff_multitrack'] = df_pd.apply(f_eff_strips_multitrack_, axis=1)
                df_pd['eff_strictzero'] = df_pd.apply(f_eff_strips_strictzero_, axis=1)
                columns_eff_.extend(['eff_proton_all', 'eff_multitrack', 'eff_strictzero'])
            
                f_eff_proton_unc_ = lambda r: proton_eff_unc_per_arm_["45" if r["Arm"] == 0 else "56"]
                df_pd['eff_proton_unc'] = df_pd.apply(f_eff_proton_unc_, axis=1)
                columns_eff_.extend(['eff_proton_all_weighted', 'eff_multitrack_weighted', 'eff_strictzero_weighted', 'eff_proton_unc'])
            
            elif data_sample == '2018':
                sensor_near_efficiency, multiRP_efficiency, _, _ = efficiencies_2018()
                lumi_periods_ = lumi_periods_2018.get(lepton_type)
                proton_eff_unc_per_arm_ = proton_efficiency_uncertainty["2018"]
                data_periods = data_periods_eff
            
                for period_ in data_periods:
                    if period_ not in lumi_periods_:
                        continue
            
                    def f_eff_sensor_near_(r):
                        if period_ in sensor_near_efficiency:
                            th2 = sensor_near_efficiency[period_]["45" if r["Arm"] == 0 else "56"]
                            return th2.GetBinContent(th2.FindBin(r["TrackX1"], r["TrackY1"]))
                        return np.nan
            
                    def f_eff_multiRP_(r):
                        if period_ in multiRP_efficiency:
                            th2 = multiRP_efficiency[period_]["45" if r["Arm"] == 0 else "56"]
                            return th2.GetBinContent(th2.FindBin(r["TrackX1"], r["TrackY1"]))
                        return np.nan
            
                    def f_eff_proton_all_(r):
                        sn = f_eff_sensor_near_(r)
                        mr = f_eff_multiRP_(r)
                        return sn * mr if pd.notna(sn) and pd.notna(mr) else np.nan
            
                    df_pd['eff_proton_all_' + period_] = df_pd.apply(f_eff_proton_all_, axis=1)
                    df_pd['eff_proton_all_weighted'] += lumi_periods_[period_] * df_pd['eff_proton_all_' + period_].fillna(0)
                    columns_eff_.append(f'eff_proton_all_{period_}')
            
                lumi_total = np.sum(list(lumi_periods_.values()))
                if lumi_total > 0:
                    df_pd['eff_proton_all_weighted'] /= lumi_total
            
                def f_eff_sensor_near_(r):
                    if pd.notna(r["period"]) and r["period"] in sensor_near_efficiency:
                        th2 = sensor_near_efficiency[r["period"]]["45" if r["Arm"] == 0 else "56"]
                        return th2.GetBinContent(th2.FindBin(r["TrackX1"], r["TrackY1"]))
                    return np.nan
            
                def f_eff_multiRP_(r):
                    if pd.notna(r["period"]) and r["period"] in multiRP_efficiency:
                        th2 = multiRP_efficiency[r["period"]]["45" if r["Arm"] == 0 else "56"]
                        return th2.GetBinContent(th2.FindBin(r["TrackX1"], r["TrackY1"]))
                    return np.nan
            
                def f_eff_proton_all_(r):
                    sn = f_eff_sensor_near_(r)
                    mr = f_eff_multiRP_(r)
                    return sn * mr if pd.notna(sn) and pd.notna(mr) else np.nan
            
                df_pd['eff_proton_all'] = df_pd.apply(f_eff_proton_all_, axis=1)
                columns_eff_.append('eff_proton_all')
            
                f_eff_proton_unc_ = lambda r: proton_eff_unc_per_arm_["45" if r["Arm"] == 0 else "56"]
                df_pd['eff_proton_unc'] = df_pd.apply(f_eff_proton_unc_, axis=1)
                columns_eff_.extend(['eff_proton_all_weighted', 'eff_proton_unc'])
            
            # Convert back to Polars
            df = pl.from_pandas(df_pd)
            print("WARNING: Conversion from Pandas back to Polars completed.")



    elif runOnMC and mix_protons and proton_selection == "MultiRP":
        # Set default efficiencies for mixed MC protons
        default_vals = {col: 1.0 for col in base_eff_cols if 'unc' not in col}
        default_vals.update({col: 0.0 for col in base_eff_cols if 'unc' in col})
        df = df.with_columns([
              pl.lit(default_vals[name]).cast(pl.Float64).alias(name) for name in base_eff_cols
        ])
        # No efficiency columns to drop for mixed MC? Or drop the base ones?
        # If they should be dropped, add keys to columns_eff_
        # columns_eff_.extend(base_eff_cols.keys())

    elif not runOnMC and proton_selection == "MultiRP":
        # Set default efficiencies for data
        default_vals = {col: 1.0 for col in base_eff_cols if 'unc' not in col}
        default_vals.update({col: 0.0 for col in base_eff_cols if 'unc' in col})
        df = df.with_columns([
              pl.lit(default_vals[name]).cast(pl.Float64).alias(name) for name in base_eff_cols
        ])
        # No efficiency columns to drop for data
        columns_eff_ = []

    # --- Indexing and Final Preparations ---
    use_hash_index_ = True
    index_vars_ = ['Run', 'LumiSection', 'EventNum', 'Slice']
    if use_hash_index_:
        # Check if hash_id already exists from a previous step, otherwise create it
        if 'hash_id' not in df.columns:
             # Use corrected hash calculation
             df = df.with_columns(
                 pl.col("Muon0Pt").hash().alias("hash_id")
             )
        print ( df.select( "hash_id" ).head(2) )
        index_vars_ = ['Run', 'LumiSection', 'EventNum', 'hash_id', 'Slice']
    else:
        index_vars_ = ['Run', 'LumiSection', 'EventNum', 'Slice']

    print (f"Index variables: {index_vars_}")

    # Define columns to drop *before* processing events
    columns_drop_ = [
        'MultiRP', 'Arm', 'RPId1', 'RPId2', # Proton specific intermediate info
        'TrackX1', 'TrackY1', 'TrackX2', 'TrackY2', # Track coordinates used for eff calc
        'TrackThX_SingleRP', 'TrackThY_SingleRP', 'Track1ThX_MultiRP', 'Track1ThY_MultiRP', 'Track2ThX_MultiRP', 'Track2ThY_MultiRP', # Track angles
        'TrackPixShift_SingleRP', 'Track1PixShift_MultiRP', 'Track2PixShift_MultiRP', # Pixel shifts
        'Xi', 'T', 'ThX', 'ThY', 'Time', # Raw proton kinematics
        # Intermediate calculation columns
        'xlow', 'xhigh', 'ylow', 'yhigh', # Fiducial boundaries if added
        'aperture_period', 'within_aperture_flag', # Aperture intermediates if added
    ]
    # Add efficiency columns to drop list only if they were calculated (runOnMC=True, non-mixed)
    # and not needed in the final output per proton.
    if runOnMC and not mix_protons:
         # Only drop the per-proton efficiencies, keep the combined ones calculated later
         eff_to_drop = [col for col in columns_eff_ if col not in ['eff_proton_all_weighted', 'eff_multitrack_weighted', 'eff_strictzero_weighted', 'eff_proton_unc']] # Adjust based on needs
         columns_drop_.extend(eff_to_drop)

    # Ensure columns to drop actually exist in the DataFrame to avoid errors
    columns_to_drop_existing = [col for col in columns_drop_ if col in df.columns]
    print(f"Columns to drop: {columns_to_drop_existing}")


    # --- Process Events (Select max Xi proton per arm/event) ---
    # Create df_index which contains all selected protons BEFORE selecting max Xi
    # This df_index matches the input structure for process_events in the pandas version
    df_index = df # Use the fully processed DataFrame up to this point

    df_events_ = None
    df_2protons_ = None
    df_ximax_ = None

    if proton_selection == "MultiRP":
         if select2protons:
             df_events_, df_2protons_ = process_events( data_sample, df_index, select2protons=select2protons, runOnMC=runOnMC, mix_protons=mix_protons, columns_drop=columns_to_drop_existing, use_hash_index=use_hash_index_ )
         else:
             df_ximax_ = process_events( data_sample, df_index, select2protons=select2protons, runOnMC=runOnMC, mix_protons=mix_protons, columns_drop=columns_to_drop_existing, use_hash_index=use_hash_index_ )

    # --- Return Results ---
    print(f"Final df_index head (all selected protons):\n{df_index.head()}")

    if proton_selection == "SingleRP":
         # For SingleRP, df_index contains the final selected protons per event/arm
         return df_index.select([col for col in df.columns if col not in columns_to_drop_existing]) # Drop intermediate columns
    elif proton_selection == "MultiRP":
         if select2protons:
             print(f"Final df_events head (one row per 2-proton event):\n{df_events_.head() if df_events_ is not None else 'None'}")
             print(f"Final df_2protons head (two rows per 2-proton event):\n{df_2protons_.head() if df_2protons_ is not None else 'None'}")
             return ( df_index.select([col for col in df.columns if col not in columns_to_drop_existing]), df_events_, df_2protons_ )
         else:
             print(f"Final df_ximax head (one row per max-xi proton):\n{df_ximax_.head() if df_ximax_ is not None else 'None'}")
             return ( df_index.select([col for col in df.columns if col not in columns_to_drop_existing]), df_ximax_ )



# --- process_events Function (Corrected) ---
def process_events(data_sample, df_protons_input, select2protons=False, runOnMC=False, mix_protons=False, columns_drop=None, use_hash_index=False):

    if df_protons_input.is_empty():
         print("process_events: Input DataFrame is empty.")
         if select2protons: return (pl.DataFrame(), pl.DataFrame())
         else: return pl.DataFrame()

    index_vars_ = ['Run', 'LumiSection', 'EventNum', 'Slice']
    if use_hash_index: index_vars_.append('hash_id')

    print (f"process_events using index: {index_vars_}")

    # --- Select max Xi proton per event/arm ---
    groupby_vars_ = index_vars_ + ['Arm']

    # Ensure Xi is float for max()
    df_protons_input = df_protons_input.with_columns(pl.col("Xi").cast(pl.Float64))

    # Find the row corresponding to max Xi within each group
    # Using window function is often more robust than sort/head for duplicates
    df_protons_multiRP_index_xi_max = df_protons_input.with_columns(
         pl.col("Xi").max().over(groupby_vars_).alias("max_xi_in_group")
    ).filter(
         pl.col("Xi") == pl.col("max_xi_in_group")
    ).drop("max_xi_in_group")

    # Handle potential ties in max Xi (keep first occurence within group) - Polars should handle this implicitly with filter
    # If explicit tie-breaking needed (e.g., by Muon0Pt):
    # df_protons_multiRP_index_xi_max = df_protons_multiRP_index_xi_max.sort(groupby_vars_ + ["Muon0Pt"], descending=True) # Example tie-breaker
    # df_protons_multiRP_index_xi_max = df_protons_multiRP_index_xi_max.unique(subset=groupby_vars_, keep='first')


    if not select2protons:
        print(f"Returning df_ximax with {len(df_protons_multiRP_index_xi_max)} rows.")
        return df_protons_multiRP_index_xi_max # Return max-xi protons per arm

    # --- Select events with exactly one proton in each arm (from the max-xi ones) ---
    valid_events = (
        df_protons_multiRP_index_xi_max
        .groupby(index_vars_)
        .agg(
            # Count occurrences of each arm within the event group
            pl.col("Arm").filter(pl.col("Arm") == 0).count().alias("count_arm0"),
            pl.col("Arm").filter(pl.col("Arm") == 1).count().alias("count_arm1")
        )
        .filter( # Keep only events with exactly one proton in each arm
            (pl.col("count_arm0") == 1) & (pl.col("count_arm1") == 1)
        )
        .select(index_vars_) # Select only the index columns of valid events
    )

    # Filter the max-xi protons DataFrame to keep only those belonging to valid 2-proton events
    df_protons_multiRP_index_2protons = df_protons_multiRP_index_xi_max.join(
        valid_events, on=index_vars_, how="inner"
    )
    print(f"Found {valid_events.height} valid 2-proton events. df_protons_multiRP_index_2protons has {len(df_protons_multiRP_index_2protons)} rows.")

    if df_protons_multiRP_index_2protons.is_empty():
         print("No valid 2-proton events found after selection.")
         return (pl.DataFrame(), pl.DataFrame())

    # --- Calculate Event-Level Variables (MX, YX, Combined Efficiencies) ---

    # Prepare for aggregation: Group by event, aggregate proton info into lists/structs
    # Ensure correct proton pair (Arm 0, Arm 1) for calculations
    df_protons_multiRP_2protons_grouped = df_protons_multiRP_index_2protons.groupby(index_vars_).agg(
        # Create a struct for each proton containing Arm and Xi, then sort by Arm to ensure order
        pl.struct(["Arm", "Xi"]).sort_by("Arm").alias("proton_pair_kin"),

        # Aggregate necessary efficiency columns into lists (also sorted by Arm)
        # Only include columns expected to be present based on runOnMC/data_sample
        *[pl.col(col).sort_by(pl.col("Arm")).alias(f"{col}_pair")
          for col in df_protons_multiRP_index_2protons.columns if col.startswith("eff_")]
    )

    # Calculate MX and YX from the sorted struct
    df_kinematics = df_protons_multiRP_2protons_grouped.with_columns([
        # proton_pair_kin is a list of 2 structs: [{Arm:0, Xi:val0}, {Arm:1, Xi:val1}]
        (13000.0 * (pl.col("proton_pair_kin").list.get(0).struct.field("Xi") *
                    pl.col("proton_pair_kin").list.get(1).struct.field("Xi")).sqrt()
        ).alias("MX"), # MX = E_beam * sqrt(xi_arm0 * xi_arm1)

        (0.5 * (pl.col("proton_pair_kin").list.get(0).struct.field("Xi").log() -
                pl.col("proton_pair_kin").list.get(1).struct.field("Xi").log())
        ).alias("YX"), # YX = 0.5 * ln(xi_arm0 / xi_arm1)
    ]).select(index_vars_ + ["MX", "YX"]) # Select only needed columns

    # Calculate combined efficiencies
    aggs_eff = {}
    if runOnMC:
         # Product for multiplicative efficiencies
         eff_prod_cols = [col for col in df_protons_multiRP_index_2protons.columns if col.startswith("eff_") and 'unc' not in col]
         for col in eff_prod_cols:
              aggs_eff[col] = pl.col(f"{col}_pair").list.eval(pl.element().product()).list.get(0) # Product of the pair

         # Quadratic sum for uncertainties
         eff_unc_cols = [col for col in df_protons_multiRP_index_2protons.columns if col == "eff_proton_unc"]
         for col in eff_unc_cols:
              aggs_eff[col] = pl.col(f"{col}_pair").list.eval(pl.element().pow(2)).list.sum().sqrt() # Sqrt(unc1^2 + unc2^2)

         # Apply aggregations if any efficiency columns were found
         if aggs_eff:
              df_aggregated_eff = df_protons_multiRP_2protons_grouped.with_columns(
                  **aggs_eff # Apply dictionary of expressions
              ).select(index_vars_ + list(aggs_eff.keys()))
         else:
              df_aggregated_eff = pl.DataFrame({idx: [] for idx in index_vars_}) # Empty if no eff columns
    else: # Data or mixed MC: Set default combined efficiencies
        eff_cols_expected = [col for col in base_eff_cols.keys() if col not in columns_drop] # Get expected final eff cols
        default_vals = {col: 1.0 for col in eff_cols_expected if 'unc' not in col}
        default_vals.update({col: 0.0 for col in eff_cols_expected if 'unc' in col})
        # Create a DataFrame with index vars to join defaults
        df_aggregated_eff = df_protons_multiRP_2protons_grouped.select(index_vars_).with_columns(
             [pl.lit(value).cast(pl.Float64).alias(name) for name, value in default_vals.items()]
        )


    # --- Create Final Event DataFrame ---
    # Start with unique events (first occurrence based on index)
    # Drop intermediate columns *before* joining results
    df_protons_multiRP_events = (
        df_protons_multiRP_index_2protons
        .select([col for col in df_protons_multiRP_index_2protons.columns if col not in columns_drop])
        .unique(subset=index_vars_, keep='first', maintain_order=True)
    )

    # Join calculated kinematics and efficiencies
    df_protons_multiRP_events = df_protons_multiRP_events.join(
        df_kinematics, on=index_vars_, how="left"
    )
    if not df_aggregated_eff.is_empty():
        df_protons_multiRP_events = df_protons_multiRP_events.join(
            df_aggregated_eff, on=index_vars_, how="left"
        )

    # Add aliases and derived variables (e.g., variations)
    df_protons_multiRP_events = df_protons_multiRP_events.with_columns([
        pl.col("MX").alias("MX_nom"),
        pl.col("YX").alias("YX_nom")
    ])

    if runOnMC and "eff_proton_unc" in df_protons_multiRP_events.columns:
        df_protons_multiRP_events = df_protons_multiRP_events.with_columns([
            (1.0 + pl.col("eff_proton_unc")).alias("eff_proton_var_up"),
            (1.0 - pl.col("eff_proton_unc").fill_null(0.0)).clip(lower_bound=0.0).alias("eff_proton_var_dw") # Ensure non-negative eff
        ])
    elif "eff_proton_unc" not in df_protons_multiRP_events.columns: # Ensure columns exist even if not MC
         df_protons_multiRP_events = df_protons_multiRP_events.with_columns([
              pl.lit(0.0).cast(pl.Float64).alias("eff_proton_unc"),
              pl.lit(1.0).cast(pl.Float64).alias("eff_proton_var_up"),
              pl.lit(1.0).cast(pl.Float64).alias("eff_proton_var_dw"),
         ])

    # Final sort
    df_protons_multiRP_events = df_protons_multiRP_events.sort(index_vars_)
    df_protons_multiRP_index_2protons = df_protons_multiRP_index_2protons.sort(index_vars_ + ["Arm"]) # Sort output protons too

    print(f"process_events returning {len(df_protons_multiRP_events)} events.")
    return (df_protons_multiRP_events, df_protons_multiRP_index_2protons) # Return df_events, df_2protons
