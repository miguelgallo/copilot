import h5py
import polars as pl
import numpy as np
import pandas as pd

# run_ranges_periods = {}
# run_ranges_periods[ "2017B" ]  = (297020,299329)
# run_ranges_periods[ "2017C1" ] = (299337,300785)
# run_ranges_periods[ "2017C2" ] = (300806,302029)
# run_ranges_periods[ "2017D" ]  = (302030,303434)
# run_ranges_periods[ "2017E" ]  = (303435,304826)
# run_ranges_periods[ "2017F1" ] = (304911,305114)
# run_ranges_periods[ "2017F2" ] = (305178,305902)
# run_ranges_periods[ "2017F3" ] = (305965,306462)
# df_run_ranges = pl.DataFrame( run_ranges_periods, index=("min","max") ).transpose()

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

# L_2017B = 2.360904801;
# L_2017C1 = 5.313012839;
# L_2017E = 8.958810514;
# L_2017F1 = 1.708478656;
# L_2017C2 = 3.264135878;
# L_2017D = 4.074723964;
# L_2017F2 = 7.877903151;
# L_2017F3 = 3.632463163;
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

# L_2018A  = 12.10
# L_2018B1 = 6.38
# L_2018B2 = 0.40
# L_2018C  = 6.5297
# L_2018D1 = 19.88
# L_2018D2 = 10.4157
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

# aperture_period_map = {
#     "2016_preTS2"  : "2016_preTS2",
#     "2016_postTS2" : "2016_postTS2",
#     "2017B"        : "2017_preTS2",
#     "2017C1"       : "2017_preTS2",
#     "2017C2"       : "2017_preTS2",
#     "2017D"        : "2017_preTS2",
#     "2017E"        : "2017_postTS2",
#     "2017F1"       : "2017_postTS2",
#     "2017F2"       : "2017_postTS2",
#     "2017F3"       : "2017_postTS2",
#     "2018"         : "2018"
# }
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

# Fiducial cuts (UL)
# Periods: "2017B", "2017C1", "2017E", "2017F1", "2018A", "2018B1", "2018B2", "2018C", "2018D1", "2018D2"
def fiducial_cuts():
    # Per data period, arm=(0,1), station=(0,2)
    fiducialXLow_ = {}
    fiducialXHigh_ = {}
    fiducialYLow_ = {}
    fiducialYHigh_ = {}

    data_periods = [ "2017B", "2017C1", "2017E", "2017F1", "2018A", "2018B1", "2018B2", "2018C", "2018D1", "2018D2" ]
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
        
    # 2017B
    # Sector 45, RP 220
    fiducialXLow_[ "2017B" ][ 0 ][ 2 ]  =   1.995;
    fiducialXHigh_[ "2017B" ][ 0 ][ 2 ] =  24.479;
    fiducialYLow_[ "2017B" ][ 0 ][ 2 ]  = -11.098;
    fiducialYHigh_[ "2017B" ][ 0 ][ 2 ] =   4.298;
    # Sector 56, RP 220
    fiducialXLow_[ "2017B" ][ 1 ][ 2 ]  =   2.422;
    fiducialXHigh_[ "2017B" ][ 1 ][ 2 ] =  24.620;
    fiducialYLow_[ "2017B" ][ 1 ][ 2 ]  = -10.698;
    fiducialYHigh_[ "2017B" ][ 1 ][ 2 ] =   4.698;

    # 2017C1
    # Sector 45, RP 220
    fiducialXLow_[ "2017C1" ][ 0 ][ 2 ]  =   1.860;
    fiducialXHigh_[ "2017C1" ][ 0 ][ 2 ] =  24.334;
    fiducialYLow_[ "2017C1" ][ 0 ][ 2 ]  = -11.098;
    fiducialYHigh_[ "2017C1" ][ 0 ][ 2 ] =   4.298;
    # Sector 56, RP 220
    fiducialXLow_[ "2017C1" ][ 1 ][ 2 ]  =   2.422;
    fiducialXHigh_[ "2017C1" ][ 1 ][ 2 ] =  24.620;
    fiducialYLow_[ "2017C1" ][ 1 ][ 2 ]  = -10.698;
    fiducialYHigh_[ "2017C1" ][ 1 ][ 2 ] =   4.698;

    # 2017E
    # Sector 45, RP 220
    fiducialXLow_[ "2017E" ][ 0 ][ 2 ]  =   1.995;
    fiducialXHigh_[ "2017E" ][ 0 ][ 2 ] =  24.479;
    fiducialYLow_[ "2017E" ][ 0 ][ 2 ]  = -10.098;
    fiducialYHigh_[ "2017E" ][ 0 ][ 2 ] =   4.998;
    # Sector 56, RP 220
    fiducialXLow_[ "2017E" ][ 1 ][ 2 ]  =  2.422;
    fiducialXHigh_[ "2017E" ][ 1 ][ 2 ] = 24.620;
    fiducialYLow_[ "2017E" ][ 1 ][ 2 ]  = -9.698;
    fiducialYHigh_[ "2017E" ][ 1 ][ 2 ] =  5.398;

    # 2017F1
    # Sector 45, RP 220
    fiducialXLow_[ "2017F1" ][ 0 ][ 2 ]  =   1.995;
    fiducialXHigh_[ "2017F1" ][ 0 ][ 2 ] =  24.479;
    fiducialYLow_[ "2017F1" ][ 0 ][ 2 ]  = -10.098;
    fiducialYHigh_[ "2017F1" ][ 0 ][ 2 ] =   4.998;
    # Sector 56, RP 220
    fiducialXLow_[ "2017F1" ][ 1 ][ 2 ]  =  2.422;
    fiducialXHigh_[ "2017F1" ][ 1 ][ 2 ] = 24.620;
    fiducialYLow_[ "2017F1" ][ 1 ][ 2 ]  = -9.698;
    fiducialYHigh_[ "2017F1" ][ 1 ][ 2 ] =  5.398;

    # 2018A
    # Sector 45, RP 210
    fiducialXLow_[ "2018A" ][ 0 ][ 0 ]  =   2.850;
    fiducialXHigh_[ "2018A" ][ 0 ][ 0 ] =  17.927;
    fiducialYLow_[ "2018A" ][ 0 ][ 0 ]  = -11.598;
    fiducialYHigh_[ "2018A" ][ 0 ][ 0 ] =   3.698;
    # Sector 45, RP 220
    fiducialXLow_[ "2018A" ][ 0 ][ 2 ]  =   2.421;
    fiducialXHigh_[ "2018A" ][ 0 ][ 2 ] =  24.620;
    fiducialYLow_[ "2018A" ][ 0 ][ 2 ]  = -10.898;
    fiducialYHigh_[ "2018A" ][ 0 ][ 2 ] =   4.398;
    # Sector 56, RP 210
    fiducialXLow_[ "2018A" ][ 1 ][ 0 ]  =   3.275;
    fiducialXHigh_[ "2018A" ][ 1 ][ 0 ] =  18.498;
    fiducialYLow_[ "2018A" ][ 1 ][ 0 ]  = -11.298;
    fiducialYHigh_[ "2018A" ][ 1 ][ 0 ] =   3.298;
    # Sector 56, RP 220
    fiducialXLow_[ "2018A" ][ 1 ][ 2 ]  =   2.421;
    fiducialXHigh_[ "2018A" ][ 1 ][ 2 ] =  20.045;
    fiducialYLow_[ "2018A" ][ 1 ][ 2 ]  = -10.398;
    fiducialYHigh_[ "2018A" ][ 1 ][ 2 ] =   5.098;

    # 2018B1
    # Sector 45, RP 210
    fiducialXLow_[ "2018B1" ][ 0 ][ 0 ]  =   2.850;
    fiducialXHigh_[ "2018B1" ][ 0 ][ 0 ] =  17.927;
    fiducialYLow_[ "2018B1" ][ 0 ][ 0 ]  = -11.598;
    fiducialYHigh_[ "2018B1" ][ 0 ][ 0 ] =   3.698;
    # Sector 45, RP 220
    fiducialXLow_[ "2018B1" ][ 0 ][ 2 ]  =   2.421;
    fiducialXHigh_[ "2018B1" ][ 0 ][ 2 ] =  24.620;
    fiducialYLow_[ "2018B1" ][ 0 ][ 2 ]  = -10.898;
    fiducialYHigh_[ "2018B1" ][ 0 ][ 2 ] =   4.198;
    # Sector 56, RP 210
    fiducialXLow_[ "2018B1" ][ 1 ][ 0 ]  =   3.275;
    fiducialXHigh_[ "2018B1" ][ 1 ][ 0 ] =  18.070;
    fiducialYLow_[ "2018B1" ][ 1 ][ 0 ]  = -11.198;
    fiducialYHigh_[ "2018B1" ][ 1 ][ 0 ] =   4.098;
    # Sector 56, RP 220
    fiducialXLow_[ "2018B1" ][ 1 ][ 2 ]  =   2.564;
    fiducialXHigh_[ "2018B1" ][ 1 ][ 2 ] =  20.045;
    fiducialYLow_[ "2018B1" ][ 1 ][ 2 ]  = -10.398;
    fiducialYHigh_[ "2018B1" ][ 1 ][ 2 ] =   5.098;

    # 2018B2
    # Sector 45, RP 210
    fiducialXLow_[ "2018B2" ][ 0 ][ 0 ]  =   2.564;
    fiducialXHigh_[ "2018B2" ][ 0 ][ 0 ] =  17.640;
    fiducialYLow_[ "2018B2" ][ 0 ][ 0 ]  = -11.598;
    fiducialYHigh_[ "2018B2" ][ 0 ][ 0 ] =   4.198;
    # Sector 45, RP 220
    fiducialXLow_[ "2018B2" ][ 0 ][ 2 ]  =   2.140;
    fiducialXHigh_[ "2018B2" ][ 0 ][ 2 ] =  24.479;
    fiducialYLow_[ "2018B2" ][ 0 ][ 2 ]  = -11.398;
    fiducialYHigh_[ "2018B2" ][ 0 ][ 2 ] =   3.798;
    # Sector 56, RP 210
    fiducialXLow_[ "2018B2" ][ 1 ][ 0 ]  =   3.275;
    fiducialXHigh_[ "2018B2" ][ 1 ][ 0 ] =  17.931;
    fiducialYLow_[ "2018B2" ][ 1 ][ 0 ]  = -10.498;
    fiducialYHigh_[ "2018B2" ][ 1 ][ 0 ] =   4.098;
    # Sector 56, RP 220
    fiducialXLow_[ "2018B2" ][ 1 ][ 2 ]  =   2.279;
    fiducialXHigh_[ "2018B2" ][ 1 ][ 2 ] =  24.760;
    fiducialYLow_[ "2018B2" ][ 1 ][ 2 ]  = -10.598;
    fiducialYHigh_[ "2018B2" ][ 1 ][ 2 ] =   4.498;

    # 2018C
    # Sector 45, RP 210
    fiducialXLow_[ "2018C" ][ 0 ][ 0 ]  =   2.564;
    fiducialXHigh_[ "2018C" ][ 0 ][ 0 ] =  17.930;
    fiducialYLow_[ "2018C" ][ 0 ][ 0 ]  = -11.098;
    fiducialYHigh_[ "2018C" ][ 0 ][ 0 ] =   4.198;
    # Sector 45, RP 220
    fiducialXLow_[ "2018C" ][ 0 ][ 2 ]  =   2.421;
    fiducialXHigh_[ "2018C" ][ 0 ][ 2 ] =  24.620;
    fiducialYLow_[ "2018C" ][ 0 ][ 2 ]  = -11.398;
    fiducialYHigh_[ "2018C" ][ 0 ][ 2 ] =   3.698;
    # Sector 56, RP 210
    fiducialXLow_[ "2018C" ][ 1 ][ 0 ]  =   3.275;
    fiducialXHigh_[ "2018C" ][ 1 ][ 0 ] =  17.931;
    fiducialYLow_[ "2018C" ][ 1 ][ 0 ]  = -10.498;
    fiducialYHigh_[ "2018C" ][ 1 ][ 0 ] =   4.698;
    # Sector 56, RP 220
    fiducialXLow_[ "2018C" ][ 1 ][ 2 ]  =   2.279;
    fiducialXHigh_[ "2018C" ][ 1 ][ 2 ] =  24.760;
    fiducialYLow_[ "2018C" ][ 1 ][ 2 ]  = -10.598;
    fiducialYHigh_[ "2018C" ][ 1 ][ 2 ] =   4.398;

    # 2018D1
    # Sector 45, RP 210
    fiducialXLow_[ "2018D1" ][ 0 ][ 0 ]  =   2.850;
    fiducialXHigh_[ "2018D1" ][ 0 ][ 0 ] =  17.931;
    fiducialYLow_[ "2018D1" ][ 0 ][ 0 ]  = -11.098;
    fiducialYHigh_[ "2018D1" ][ 0 ][ 0 ] =   4.098;
    # Sector 45, RP 220
    fiducialXLow_[ "2018D1" ][ 0 ][ 2 ]  =   2.421;
    fiducialXHigh_[ "2018D1" ][ 0 ][ 2 ] =  24.620;
    fiducialYLow_[ "2018D1" ][ 0 ][ 2 ]  = -11.398;
    fiducialYHigh_[ "2018D1" ][ 0 ][ 2 ] =   3.698;
    # Sector 56, RP 210
    fiducialXLow_[ "2018D1" ][ 1 ][ 0 ]  =   3.275;
    fiducialXHigh_[ "2018D1" ][ 1 ][ 0 ] =  17.931;
    fiducialYLow_[ "2018D1" ][ 1 ][ 0 ]  = -10.498;
    fiducialYHigh_[ "2018D1" ][ 1 ][ 0 ] =   4.698;
    # Sector 56, RP 220
    fiducialXLow_[ "2018D1" ][ 1 ][ 2 ]  =   2.279;
    fiducialXHigh_[ "2018D1" ][ 1 ][ 2 ] =  24.760;
    fiducialYLow_[ "2018D1" ][ 1 ][ 2 ]  = -10.598;
    fiducialYHigh_[ "2018D1" ][ 1 ][ 2 ] =   4.398;

    # 2018D2
    # Sector 45, RP 210
    fiducialXLow_[ "2018D2" ][ 0 ][ 0 ]  =   2.850;
    fiducialXHigh_[ "2018D2" ][ 0 ][ 0 ] =  17.931;
    fiducialYLow_[ "2018D2" ][ 0 ][ 0 ]  = -10.598;
    fiducialYHigh_[ "2018D2" ][ 0 ][ 0 ] =   4.498;
    # Sector 45, RP 220
    fiducialXLow_[ "2018D2" ][ 0 ][ 2 ]  =   2.421;
    fiducialXHigh_[ "2018D2" ][ 0 ][ 2 ] =  24.620;
    fiducialYLow_[ "2018D2" ][ 0 ][ 2 ]  = -11.698;
    fiducialYHigh_[ "2018D2" ][ 0 ][ 2 ] =   3.298;
    # Sector 56, RP 210
    fiducialXLow_[ "2018D2" ][ 1 ][ 0 ]  =  3.275;
    fiducialXHigh_[ "2018D2" ][ 1 ][ 0 ] = 17.931;
    fiducialYLow_[ "2018D2" ][ 1 ][ 0 ]  = -9.998;
    fiducialYHigh_[ "2018D2" ][ 1 ][ 0 ] =  4.698;
    # Sector 56, RP 220
    fiducialXLow_[ "2018D2" ][ 1 ][ 2 ]  =   2.279;
    fiducialXHigh_[ "2018D2" ][ 1 ][ 2 ] =  24.760;
    fiducialYLow_[ "2018D2" ][ 1 ][ 2 ]  = -10.598;
    fiducialYHigh_[ "2018D2" ][ 1 ][ 2 ] =   3.898;
      
    return (  fiducialXLow_, fiducialXHigh_, fiducialYLow_, fiducialYHigh_ )


# def fiducial_cuts_all():
# 
#     fiducialXLow_, fiducialXHigh_, fiducialYLow_, fiducialYHigh_ = fiducial_cuts()
#     print ( fiducialXLow_, fiducialXHigh_, fiducialYLow_, fiducialYHigh_ )
#     
#     # Per data period, arm=(0,1), station=(0,2)
#     fiducialXLow_all = {}
#     fiducialXHigh_all = {}
#     fiducialYLow_all = {}
#     fiducialYHigh_all = {}
#     for arm_ in (0,1):
#         fiducialXLow_all[ arm_ ] = {}
#         fiducialXLow_all[ arm_ ][ 2 ] = []
#         fiducialXHigh_all[ arm_ ] = {}
#         fiducialXHigh_all[ arm_ ][ 2 ] = []
#         fiducialYLow_all[ arm_ ] = {}
#         fiducialYLow_all[ arm_ ][ 2 ] = []
#         fiducialYHigh_all[ arm_ ] = {}
#         fiducialYHigh_all[ arm_ ][ 2 ] = []
# 
#     data_periods = [ "2017B", "2017C1", "2017E", "2017F1" ]
# 
#     for period_ in data_periods:
#         for arm_ in (0,1):
#             fiducialXLow_all[ arm_ ][ 2 ].append( fiducialXLow_[ period_ ][ arm_][ 2 ] )
#             fiducialXHigh_all[ arm_ ][ 2 ].append( fiducialXHigh_[ period_ ][ arm_][ 2 ] )
#             fiducialYLow_all[ arm_ ][ 2 ].append( fiducialYLow_[ period_ ][ arm_][ 2 ] )
#             fiducialYHigh_all[ arm_ ][ 2 ].append( fiducialYHigh_[ period_ ][ arm_][ 2 ] )
# 
#     for arm_ in (0,1):
#         fiducialXLow_all[ arm_ ][ 2 ] = np.max( fiducialXLow_all[ arm_ ][ 2 ] )
#         fiducialXHigh_all[ arm_ ][ 2 ] = np.min( fiducialXHigh_all[ arm_ ][ 2 ] )
#         fiducialYLow_all[ arm_ ][ 2 ] = np.max( fiducialYLow_all[ arm_ ][ 2 ] )
#         fiducialYHigh_all[ arm_ ][ 2 ] = np.min( fiducialYHigh_all[ arm_ ][ 2 ] )
#
#     print ( fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all )
#     
#     return ( fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all )


def fiducial_cuts_all( data_sample ):

    data_periods = None
    if data_sample == '2017':
        # data_periods = [ "2017B", "2017C1", "2017E", "2017F1" ]
        data_periods = [ "2017B", "2017C1", "2017C2", "2017D", "2017E", "2017F1", "2017F2", "2017F3" ]
    elif data_sample == '2018':
        data_periods = [ "2018A", "2018B1", "2018B2", "2018C", "2018D1", "2018D2" ]

    fiducialXLow_, fiducialXHigh_, fiducialYLow_, fiducialYHigh_ = fiducial_cuts()
    print ( fiducialXLow_, fiducialXHigh_, fiducialYLow_, fiducialYHigh_ )
    
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
            fiducialXLow_all[ arm_ ][ 2 ].append( fiducialXLow_[ period_ ][ arm_][ 2 ] )
            fiducialXHigh_all[ arm_ ][ 2 ].append( fiducialXHigh_[ period_ ][ arm_][ 2 ] )
            fiducialYLow_all[ arm_ ][ 2 ].append( fiducialYLow_[ period_ ][ arm_][ 2 ] )
            fiducialYHigh_all[ arm_ ][ 2 ].append( fiducialYHigh_[ period_ ][ arm_][ 2 ] )

    for arm_ in (0,1):
        fiducialXLow_all[ arm_ ][ 2 ] = np.max( fiducialXLow_all[ arm_ ][ 2 ] )
        fiducialXHigh_all[ arm_ ][ 2 ] = np.min( fiducialXHigh_all[ arm_ ][ 2 ] )
        fiducialYLow_all[ arm_ ][ 2 ] = np.max( fiducialYLow_all[ arm_ ][ 2 ] )
        fiducialYHigh_all[ arm_ ][ 2 ] = np.min( fiducialYHigh_all[ arm_ ][ 2 ] )

    print ( fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all )
    
    return ( fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all )


# Per data period, arm=(0,1)
# Periods: "2016_preTS2", "2016_postTS2", "2017_preTS2", "2017_postTS2", "2018"
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
        return -999.


def check_aperture( period, arm, xangle, xi, theta_x ):
    return ( theta_x < -aperture_parametrisation( period, arm, xangle, xi ) )


def get_data( fileNames, selection=None, version="V1" ):

    if not version in ("V1", "V2"):
        print ( "Unsupported version: {}".format( version ) )
        exit()

    df_columns_types_ = {
        "Run": pl.Int64,
        "LumiSection": pl.Int64,
        "EventNum": pl.Int64,
        "Slice": pl.Int32,
        "MultiRP": pl.Int32,
        "Arm": pl.Int32,
        "RPId1": pl.Int32,
        "RPId2": pl.Int32,
        "TrackPixShift_SingleRP": pl.Int32,
        "Track1PixShift_MultiRP": pl.Int32,
        "Track2PixShift_MultiRP": pl.Int32,
        "nVertices": pl.Int32,
        "ExtraPfCands": pl.Int32,
        }
   
    #df_list = None
    #df_protons_multiRP_list = None
    #df_protons_singleRP_list = None

    if version == "V1":
        df_list = []
    elif version == "V2":
        df_protons_multiRP_list = []
        df_protons_singleRP_list = []

    if version == "V1":
        df_list = []
    elif version == "V2":
        df_protons_multiRP_list = []
        df_protons_singleRP_list = []

    for file_ in fileNames:
        print ( file_ )
        with h5py.File( file_, 'r' ) as f:
            print ( list(f.keys()) )

            dset = None
            dset_protons_multiRP = None
            dset_protons_singleRP = None
            if version == "V1":
                dset = f['protons']
                print ( dset.shape )
                print ( dset[:,:] )
            elif version == "V2":
                dset_protons_multiRP = f['protons_multiRP']
                print ( dset_protons_multiRP.shape )
                print ( dset_protons_multiRP[:,:] )
                dset_protons_singleRP = f['protons_singleRP']
                print ( dset_protons_singleRP.shape )
                print ( dset_protons_singleRP[:,:] )

            dset_columns = f['columns']
            print ( dset_columns.shape )
            columns = list( dset_columns )
            print ( columns )
            columns_str = [ item.decode("utf-8") for item in columns ]
            print ( columns_str )

            dset_selections = f['selections']
            selections_ = [ item.decode("utf-8") for item in dset_selections ]
            print ( selections_ )

            dset_counts = f['event_counts']
            counts_array = np.array(dset_counts)

            if counts_array.ndim == 1:
                df_counts = pl.DataFrame({"counts": counts_array})
            
            elif counts_array.ndim == 2 and counts_array.shape[1] == 2:
                df_counts = pl.DataFrame(counts_array, schema=["selection", "counts"])
            
            else:
                raise ValueError(f"Formato inesperado de dset_counts: shape={counts_array.shape}") 

            if "Run_mc" in columns_str:
                df_columns_types_.update( { "Run_mc": pl.Int64 } )

            if "Run_rnd" in columns_str: 
                df_columns_types_.update( { "Run_rnd": pl.Int64, "LumiSection_rnd": pl.Int64, "EventNum_rnd": pl.Int64 } )

            chunk_size = 1000000
            
            if version == "V1":
                entries = dset.shape[0]
                start_ = list( range( 0, entries, chunk_size ) )
                stop_ = start_[1:]
                stop_.append( entries )
                print ( start_ )
                print ( stop_ )
                for idx in range( len( start_ ) ):
                    print ( start_[idx], stop_[idx] )
                    #print ( dset[ start_[idx] : stop_[idx] ] )
                    # df_ = pl.DataFrame( dset[ start_[idx] : stop_[idx] ], columns=columns_str )
                    # df_ = df_[ df_columns_ ].astype( df_columns_types_ )
                    data = dset[ start_[idx]:stop_[idx] ]
                    data_dict = { col: data[ :, i ] for i, col in enumerate( columns_str ) }
                    df_ = pl.DataFrame( data_dict ).with_columns( [ pl.col(k).cast(v) for k, v in df_columns_types_.items() ] ) 
                    
                    if selection:
                        #print ( len(df_) )
                        df_ = selection( df_ )
                        #print ( len(df_) )
    
                    df_list.append( df_ )
                    print ( df_list[-1].head() )
                    print ( len( df_list[-1] ) )
            elif version == "V2":
                entries_protons_multiRP = dset_protons_multiRP.shape[0]
                start_ = list( range( 0, entries_protons_multiRP, chunk_size ) )
                stop_ = start_[1:]
                stop_.append( entries_protons_multiRP )
                print ( start_ )
                print ( stop_ )
                for idx in range( len( start_ ) ):
                    print ( start_[idx], stop_[idx] )
                    # df_ = pl.DataFrame( dset_protons_multiRP[ start_[idx] : stop_[idx] ], columns=columns_str )
                    # df_ = df_[ df_columns_ ].astype( df_columns_types_ )
                    data = dset_protons_multiRP[ start_[idx]:stop_[idx] ]
                    data_dict = { col: data[ :, i ] for i, col in enumerate( columns_str ) }
                    df_ = pl.DataFrame( data_dict ).with_columns( [ pl.col(k).cast(v) for k, v in df_columns_types_.items() ] ) 
                    if selection:
                        #print ( len(df_) )
                        df_ = selection( df_ )
                        #print ( len(df_) )
    
                    df_protons_multiRP_list.append( df_ )
                    print ( df_protons_multiRP_list[-1].head() )
                    print ( len( df_protons_multiRP_list[-1] ) )

                entries_protons_singleRP = dset_protons_singleRP.shape[0]
                start_ = list( range( 0, entries_protons_singleRP, chunk_size ) )
                stop_ = start_[1:]
                stop_.append( entries_protons_singleRP )
                print ( start_ )
                print ( stop_ )
                for idx in range( len( start_ ) ):
                    print ( start_[idx], stop_[idx] )
                    # df_ = pl.DataFrame( dset_protons_singleRP[ start_[idx] : stop_[idx] ], columns=columns_str )
                    # df_ = df_[ df_columns_ ].astype( df_columns_types_ )
                    data = dset_protons_singleRP[ start_[idx]:stop_[idx] ]
                    data_dict = { col: data[ :, i ] for i, col in enumerate( columns_str ) }
                    df_ = pl.DataFrame( data_dict ).with_columns( [ pl.col(k).cast(v) for k, v in df_columns_types_.items() ] ) 
                    
                    if selection:
                        #print ( len(df_) )
                        df_ = selection( df_ )
                        #print ( len(df_) )
    
                    df_protons_singleRP_list.append( df_ )
                    print ( df_protons_singleRP_list[-1].head() )
                    print ( len( df_protons_singleRP_list[-1] ) )

    if "selection" in df_counts.columns:
        df_counts = ( df_counts.groupby( "selection" ).agg( pl.col( "counts" ).sum().alias( "counts" ) ).sort( "selection" ) ) 
    print( df_counts )

    if version == "V1":
        df = pl.concat( df_list )
        print ( df )
        return ( df_counts, df )
    elif version == "V2":
        df_protons_multiRP = pl.concat( df_protons_multiRP_list )
        print (df_protons_multiRP)
        df_protons_singleRP = pl.concat( df_protons_singleRP_list )
        print (df_protons_singleRP)    
        return ( df_counts, df_protons_multiRP, df_protons_singleRP )


def process_data( df, data_sample, lepton_type, proton_selection, min_mass=0., min_pt_1=-1, min_pt_2=-1, apply_fiducial=True, within_aperture=False, random_protons=False, mix_protons=False, runOnMC=False, select2protons=False ):

    if runOnMC:
        print ( "Turning within_aperture OFF for MC." )
        within_aperture = False
        
    msk = ( df["InvMass"] >= min_mass )
    if min_pt_1 > 0: msk = msk & ( df["Muon0Pt"] >= min_pt_1 )
    if min_pt_2 > 0: msk = msk & ( df["Muon1Pt"] >= min_pt_2 )

    fiducialXLow_all = None
    fiducialXHigh_all = None
    fiducialYLow_all = None
    fiducialYHigh_all = None
    if apply_fiducial:
        fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all = fiducial_cuts_all( data_sample )
    
    track_angle_cut_ = 0.02
    
    run_str_ = "Run"
    if random_protons or mix_protons:
        run_str_ = "Run_rnd"
    elif runOnMC and not mix_protons:
        run_str_ = "Run_mc"

    xangle_str_ = "CrossingAngle"
    if random_protons or mix_protons:
        xangle_str_ = "CrossingAngle_rnd"

    df_run_ranges_ = None
    df_run_ranges_mixing_ = None
    if data_sample == '2017':
        df_run_ranges_ = df_run_ranges_2017
        df_run_ranges_mixing_ = df_run_ranges_mixing_2017
    elif data_sample == '2018':
        df_run_ranges_ = df_run_ranges_2018
        df_run_ranges_mixing_ = df_run_ranges_mixing_2018
    print ( df_run_ranges_ )
    print ( df_run_ranges_mixing_ )

    if "period" not in df.columns:
        df = df.with_columns( [ pl.lit( np.nan ).alias( "period" ) ] )
        for idx_ in range( df_run_ranges_.shape[0] ):
            msk_period_ = ( ( df[run_str_] >= df_run_ranges_.get_column("min")[idx_] ) & 
                            ( df[run_str_] <= df_run_ranges_.get_column("max")[idx_] ) )
            sum_period_ = msk_period_.sum()
            if sum_period_ > 0:
                period_key_ = df_run_ranges_["period"][idx_]
                df = df.with_columns(
                    pl.when( msk_period_ ).then( period_key_ ).otherwise( pl.col( "period" ) ).alias( "period" )
                )
                print( f"{period_key_}: {sum_period_}" )
        print( df["period"] )


    msk1 = None
    msk2 = None
    if proton_selection == "SingleRP":
        # Single-RP in pixel stations
        msk1_arm = ( df["RPId1"] == 23 )
        msk2_arm = ( df["RPId1"] == 123 )
        df = df.with_columns([pl.lit(np.nan).alias("XiMuMu")])
        df = df.with_columns(
            pl.when(~msk1_arm).then(df["XiMuMuPlus"]).otherwise(df["XiMuMu"]).alias("XiMuMu")
        )
        df = df.with_columns(
            pl.when(~msk2_arm).then(df["XiMuMuMinus"]).otherwise(df["XiMuMu"]).alias("XiMuMu")
        )
        
        msk_pixshift = (df["TrackPixShift_SingleRP"] == 0)

        msk_fid = None
        if apply_fiducial:
            df = df.with_columns([
                pl.lit(np.nan).alias("xlow"),
                pl.lit(np.nan).alias("xhigh"),
                pl.lit(np.nan).alias("ylow"),
                pl.lit(np.nan).alias("yhigh")
            ])
            df = df.with_columns(
                pl.when(~msk1_arm).then(fiducialXLow_all[0][2]).otherwise(df["xlow"]).alias("xlow")
            )
            df = df.with_columns(
                pl.when(~msk1_arm).then(fiducialXHigh_all[0][2]).otherwise(df["xhigh"]).alias("xhigh")
            )
            df = df.with_columns(
                pl.when(~msk1_arm).then(fiducialYLow_all[0][2]).otherwise(df["ylow"]).alias("ylow")
            )
            df = df.with_columns(
                pl.when(~msk1_arm).then(fiducialYHigh_all[0][2]).otherwise(df["yhigh"]).alias("yhigh")
            )
            df = df.with_columns(
                pl.when(~msk2_arm).then(fiducialXLow_all[1][2]).otherwise(df["xlow"]).alias("xlow")
            )
            df = df.with_columns(
                pl.when(~msk2_arm).then(fiducialXHigh_all[1][2]).otherwise(df["xhigh"]).alias("xhigh")
            )
            df = df.with_columns(
                pl.when(~msk2_arm).then(fiducialYLow_all[1][2]).otherwise(df["ylow"]).alias("ylow")
            )
            df = df.with_columns(
                pl.when(~msk2_arm).then(fiducialYHigh_all[1][2]).otherwise(df["yhigh"]).alias("yhigh")
            )

            msk_fid = ( (df["TrackThX_SingleRP"].abs() <= track_angle_cut_) &
                        (df["TrackThY_SingleRP"].abs() <= track_angle_cut_) &
                        (df["TrackX1"] >= df["xlow"]) &
                        (df["TrackX1"] <= df["xhigh"]) &
                        (df["TrackY1"] >= df["ylow"]) &
                        (df["TrackY1"] <= df["yhigh"]) )

        msk1 = msk & msk_pixshift & (df["MultiRP"] == 0) & msk1_arm
        msk2 = msk & msk_pixshift & (df["MultiRP"] == 0) & msk2_arm
        if msk_fid is not None:
            msk1 = msk1 & msk_fid
            msk2 = msk2 & msk_fid

    elif proton_selection == "MultiRP":
        # Multi-RP
        msk_multiRP = ( df["MultiRP"] == 1 )
        msk1_arm = ( df["Arm"] == 0 )
        msk2_arm = ( df["Arm"] == 1 )
        df = df.with_columns([pl.lit(np.nan).alias("XiMuMu")])
        df = df.with_columns(
            pl.when(~msk1_arm).then(df["XiMuMuPlus"]).otherwise(df["XiMuMu"]).alias("XiMuMu")
        )
        df = df.with_columns(
            pl.when(~msk2_arm).then(df["XiMuMuMinus"]).otherwise(df["XiMuMu"]).alias("XiMuMu")
        )
        
        msk_pixshift = ( (df["Track1PixShift_MultiRP"] == 0) & (df["Track2PixShift_MultiRP"] == 0) )

        if within_aperture:
            df = df.with_columns(
                pl.lit(np.nan).alias("within_aperture")
            )
            df = df.with_columns(
                pl.when(msk_multiRP).then(df.apply(
                    lambda row: check_aperture(aperture_period_map[row["period"]], row["Arm"], row["CrossingAngle"], row["Xi"], row["ThX"]),
                    axis=1
                )).otherwise(df["within_aperture"])
            )

        msk_fid = None
        if apply_fiducial:
            df = df.with_columns([
                pl.lit(np.nan).alias("xlow"),
                pl.lit(np.nan).alias("xhigh"),
                pl.lit(np.nan).alias("ylow"),
                pl.lit(np.nan).alias("yhigh")
            ])
            df = df.with_columns(
                pl.when(~msk1_arm).then(fiducialXLow_all[0][2]).otherwise(df["xlow"]).alias("xlow")
            )
            df = df.with_columns(
                pl.when(~msk1_arm).then(fiducialXHigh_all[0][2]).otherwise(df["xhigh"]).alias("xhigh")
            )
            df = df.with_columns(
                pl.when(~msk1_arm).then(fiducialYLow_all[0][2]).otherwise(df["ylow"]).alias("ylow")
            )
            df = df.with_columns(
                pl.when(~msk1_arm).then(fiducialYHigh_all[0][2]).otherwise(df["yhigh"]).alias("yhigh")
            )
            df = df.with_columns(
                pl.when(~msk2_arm).then(fiducialXLow_all[1][2]).otherwise(df["xlow"]).alias("xlow")
            )
            df = df.with_columns(
                pl.when(~msk2_arm).then(fiducialXHigh_all[1][2]).otherwise(df["xhigh"]).alias("xhigh")
            )
            df = df.with_columns(
                pl.when(~msk2_arm).then(fiducialYLow_all[1][2]).otherwise(df["ylow"]).alias("ylow")
            )
            df = df.with_columns(
                pl.when(~msk2_arm).then(fiducialYHigh_all[1][2]).otherwise(df["yhigh"]).alias("yhigh")
            )

            msk_fid = ( (df["Track2ThX_MultiRP"].abs() <= track_angle_cut_) &
                        (df["Track2ThY_MultiRP"].abs() <= track_angle_cut_) &
                        (df["TrackX2"] >= df["xlow"]) &
                        (df["TrackX2"] <= df["xhigh"]) &
                        (df["TrackY2"] >= df["ylow"]) &
                        (df["TrackY2"] <= df["yhigh"]) )
        
        msk_aperture = None
        if within_aperture:
            msk_aperture = df["within_aperture"]

        msk1 = msk & msk_pixshift & msk_multiRP & msk1_arm
        msk2 = msk & msk_pixshift & msk_multiRP & msk2_arm
        if msk_fid is not None:
            msk1 = msk1 & msk_fid
            msk2 = msk2 & msk_fid
        if msk_aperture is not None:
            msk1 = msk1 & msk_aperture
            msk2 = msk2 & msk_aperture

            
    print ( "Number of rows: {}".format( len(df) ) )
    df = df.filter( msk1 | msk2 )
    print ( "Number of rows selected: {}".format( len(df) ) )
    
    columns_eff_ = []

    if runOnMC and not mix_protons and proton_selection == "MultiRP":
        if data_sample == '2017': 
            # efficiencies_2017
            from proton_efficiency import efficiencies_2017, strict_zero_efficiencies, proton_efficiency_uncertainty
            strips_multitrack_efficiency, strips_sensor_efficiency, multiRP_efficiency, file_eff_strips, file_eff_multiRP = efficiencies_2017()
            sz_efficiencies = strict_zero_efficiencies()

            data_periods = [ "2017B", "2017C1", "2017C2", "2017D", "2017E", "2017F1", "2017F2", "2017F3" ]

            lumi_periods_ = None
            if lepton_type == 'muon':
                lumi_periods_ = lumi_periods_2017[ 'muon' ]
            elif lepton_type == 'electron':
                lumi_periods_ = lumi_periods_2017[ 'electron' ]

            proton_eff_unc_per_arm_ = proton_efficiency_uncertainty[ "2017" ]

            df.loc[ :, 'eff_proton_all_weighted' ] = 0.
            df.loc[ :, 'eff_multitrack_weighted' ] = 0.
            df.loc[ :, 'eff_strictzero_weighted' ] = 0.
            for period_ in data_periods:
                f_eff_strips_multitrack_ = lambda row: strips_multitrack_efficiency[ period_ ][ "45" if row["Arm"] == 0 else "56" ].GetBinContent( 1 )
    
                f_eff_strips_sensor_     = lambda row: strips_sensor_efficiency[ period_ ][ "45" if row["Arm"] == 0 else "56" ].GetBinContent(
                                                strips_sensor_efficiency[ period_ ][ "45" if row["Arm"] == 0 else "56" ].FindBin( row["TrackX2"], row["TrackY2"] )
                                                )
    
                f_eff_multiRP_           = lambda row: multiRP_efficiency[ period_ ][ "45" if row["Arm"] == 0 else "56" ].GetBinContent(
                                                multiRP_efficiency[ period_ ][ "45" if row["Arm"] == 0 else "56" ].FindBin( row["TrackX1"], row["TrackY1"] )
                                                )
    
                f_eff_strips_strictzero_ = lambda row: sz_efficiencies[ period_ ][ "45" if row["Arm"] == 0 else "56" ][ int( ( row["CrossingAngle"] // 10 ) * 10 ) ]

                f_eff_proton_all_        = lambda row: f_eff_strips_sensor_(row) * f_eff_multiRP_(row)

                # f_eff_all_               = lambda row: f_eff_strips_sensor_(row) * f_eff_multiRP_(row) * f_eff_strips_multitrack_(row)

                df.loc[ :, 'eff_proton_all_' + period_ ] = df[ ["Arm", "TrackX1", "TrackY1", "TrackX2", "TrackY2"] ].apply( f_eff_proton_all_, axis=1 )
                df.loc[ :, 'eff_proton_all_weighted' ]   = df.loc[ :, 'eff_proton_all_weighted' ] + lumi_periods_[ period_ ] * df.loc[ :, 'eff_proton_all_' + period_ ]
                df.loc[ :, 'eff_multitrack_' + period_ ] = df[ [ "Arm" ] ].apply( f_eff_strips_multitrack_, axis=1 )
                df.loc[ :, 'eff_multitrack_weighted' ]   = df.loc[ :, 'eff_multitrack_weighted' ] + lumi_periods_[ period_ ] * df.loc[ :, 'eff_multitrack_' + period_ ]
                df.loc[ :, 'eff_strictzero_' + period_ ] = df[ [ "Arm", "CrossingAngle" ] ].apply( f_eff_strips_strictzero_, axis=1 )
                df.loc[ :, 'eff_strictzero_weighted' ]   = df.loc[ :, 'eff_strictzero_weighted' ] + lumi_periods_[ period_ ] * df.loc[ :, 'eff_strictzero_' + period_ ]
                columns_eff_.append( 'eff_proton_all_' + period_ )        
                columns_eff_.append( 'eff_multitrack_' + period_ )        
                columns_eff_.append( 'eff_strictzero_' + period_ )        
            columns_eff_.append( 'eff_proton_all_weighted' ) 
            columns_eff_.append( 'eff_multitrack_weighted' ) 
            columns_eff_.append( 'eff_strictzero_weighted' )

            lumi_ = np.sum( list( lumi_periods_.values() ) )
            df.loc[ :, 'eff_proton_all_weighted' ] = df.loc[ :, 'eff_proton_all_weighted' ] / lumi_
            df.loc[ :, 'eff_multitrack_weighted' ] = df.loc[ :, 'eff_multitrack_weighted' ] / lumi_
            df.loc[ :, 'eff_strictzero_weighted' ] = df.loc[ :, 'eff_strictzero_weighted' ] / lumi_

            f_eff_strips_multitrack_ = lambda row: strips_multitrack_efficiency[ row["period"] ][ "45" if row["Arm"] == 0 else "56" ].GetBinContent( 1 )
    
            f_eff_strips_sensor_     = lambda row: strips_sensor_efficiency[ row["period"] ][ "45" if row["Arm"] == 0 else "56" ].GetBinContent(
                                            strips_sensor_efficiency[ row["period"] ][ "45" if row["Arm"] == 0 else "56" ].FindBin( row["TrackX2"], row["TrackY2"] )
                                            )
    
            f_eff_multiRP_           = lambda row: multiRP_efficiency[ row["period"] ][ "45" if row["Arm"] == 0 else "56" ].GetBinContent(
                                            multiRP_efficiency[ row["period"] ][ "45" if row["Arm"] == 0 else "56" ].FindBin( row["TrackX1"], row["TrackY1"] )
                                            )
    
            f_eff_strips_strictzero_ = lambda row: sz_efficiencies[ row["period"] ][ "45" if row["Arm"] == 0 else "56" ][ int( ( row["CrossingAngle"] // 10 ) * 10 ) ]

            f_eff_proton_all_        = lambda row: f_eff_strips_sensor_(row) * f_eff_multiRP_(row)
            df.loc[ :, 'eff_proton_all' ] = df[ [ "period", "Arm", "TrackX1", "TrackY1", "TrackX2", "TrackY2" ] ].apply( f_eff_proton_all_, axis=1 )
            df.loc[ :, 'eff_multitrack' ] = df[ [ "period", "Arm" ] ].apply( f_eff_strips_multitrack_, axis=1 )
            df.loc[ :, 'eff_strictzero' ] = df[ [ "period", "Arm", "CrossingAngle" ] ].apply( f_eff_strips_strictzero_, axis=1 )
            columns_eff_.append( 'eff_proton_all' ) 
            columns_eff_.append( 'eff_multitrack' ) 
            columns_eff_.append( 'eff_strictzero' )

            f_eff_proton_unc_ = lambda row: proton_eff_unc_per_arm_[ "45" if row["Arm"] == 0 else "56" ]
            df.loc[ :, 'eff_proton_unc' ] = df[ [ "Arm" ] ].apply( f_eff_proton_unc_, axis=1 )
            columns_eff_.append( 'eff_proton_unc' ) 
        elif data_sample == '2018': 
            # efficiencies_2018
            from proton_efficiency import efficiencies_2018, proton_efficiency_uncertainty
            sensor_near_efficiency, multiRP_efficiency, file_eff_rad_near, file_eff_multiRP = efficiencies_2018()

            data_periods = [ "2018A", "2018B1", "2018B2", "2018C", "2018D1", "2018D2" ]

            lumi_periods_ = None
            if lepton_type == 'muon':
                lumi_periods_ = lumi_periods_2018[ 'muon' ]
            elif lepton_type == 'electron':
                lumi_periods_ = lumi_periods_2018[ 'electron' ]

            proton_eff_unc_per_arm_ = proton_efficiency_uncertainty[ "2018" ]

            df = df.with_columns( [
                pl.lit( 0. ).alias( "eff_proton_all_weighted" )
            ] )

            df_pd = df.to_pandas()
            for period_ in data_periods:
                f_eff_sensor_near_       = lambda row: sensor_near_efficiency[ period_ ][ "45" if row["Arm"] == 0 else "56" ].GetBinContent(
                                                sensor_near_efficiency[ period_ ][ "45" if row["Arm"] == 0 else "56" ].FindBin( row["TrackX1"], row["TrackY1"] )
                                                )
    
                f_eff_multiRP_           = lambda row: multiRP_efficiency[ period_ ][ "45" if row["Arm"] == 0 else "56" ].GetBinContent(
                                                multiRP_efficiency[ period_ ][ "45" if row["Arm"] == 0 else "56" ].FindBin( row["TrackX1"], row["TrackY1"] )
                                                )

                f_eff_proton_all_        = lambda row: f_eff_sensor_near_(row) * f_eff_multiRP_(row)

                df_pd.loc[ :, 'eff_proton_all_' + period_ ] = df_pd[ ["Arm", "TrackX1", "TrackY1"] ].apply( f_eff_proton_all_, axis=1 )
                df_pd.loc[ :, 'eff_proton_all_weighted' ]   = df_pd.loc[ :, 'eff_proton_all_weighted' ] + lumi_periods_[ period_ ] * df_pd.loc[ :, 'eff_proton_all_' + period_ ]
                columns_eff_.append( 'eff_proton_all_' + period_ )        
            columns_eff_.append( 'eff_proton_all_weighted' ) 

            lumi_ = np.sum( list( lumi_periods_.values() ) )
            df_pd.loc[ :, 'eff_proton_all_weighted' ] = df_pd.loc[ :, 'eff_proton_all_weighted' ] / lumi_

            f_eff_sensor_near_       = lambda row: sensor_near_efficiency[ row["period"] ][ "45" if row["Arm"] == 0 else "56" ].GetBinContent(
                                            sensor_near_efficiency[ row["period"] ][ "45" if row["Arm"] == 0 else "56" ].FindBin( row["TrackX1"], row["TrackY1"] )
                                            )
    
            f_eff_multiRP_           = lambda row: multiRP_efficiency[ row["period"] ][ "45" if row["Arm"] == 0 else "56" ].GetBinContent(
                                            multiRP_efficiency[ row["period"] ][ "45" if row["Arm"] == 0 else "56" ].FindBin( row["TrackX1"], row["TrackY1"] )
                                            )
    
            f_eff_proton_all_        = lambda row: f_eff_sensor_near_(row) * f_eff_multiRP_(row)
            df_pd.loc[ :, 'eff_proton_all' ] = df_pd[ [ "period", "Arm", "TrackX1", "TrackY1" ] ].apply( f_eff_proton_all_, axis=1 )
            columns_eff_.append( 'eff_proton_all' ) 

            f_eff_proton_unc_ = lambda row: proton_eff_unc_per_arm_[ "45" if row["Arm"] == 0 else "56" ]
            df_pd.loc[ :, 'eff_proton_unc' ] = df_pd[ [ "Arm" ] ].apply( f_eff_proton_unc_, axis=1 )
            columns_eff_.append( 'eff_proton_unc' )

    use_hash_index_ = True
    index_vars_ = None
    from pandas.util import hash_array
    #hash_ids = hash_array(df_pd["Muon0Pt"].values)
    #df_pd["hash_id"] = hash_ids
    #index_vars_ = ['Run', 'LumiSection', 'EventNum', 'hash_id', 'Slice'] if use_hash_index_ else ['Run', 'LumiSection', 'EventNum', 'Slice']

    df = pl.from_pandas(df_pd)

    if runOnMC and mix_protons and proton_selection == "MultiRP":
        df = df.with_columns([
        pl.lit(1.0).alias("eff_proton_all"),
        pl.lit(1.0).alias("eff_proton_all_weighted"),
        pl.lit(0.0).alias("eff_proton_unc")
    ])
    if data_sample == '2017':
        df = df.with_columns([
            pl.lit(1.0).alias("eff_multitrack"),
            pl.lit(1.0).alias("eff_strictzero"),
            pl.lit(1.0).alias("eff_multitrack_weighted"),
            pl.lit(1.0).alias("eff_strictzero_weighted")
        ])

    if not runOnMC and proton_selection == "MultiRP":
        df = df.with_columns([
            pl.lit(1.0).alias("eff_proton_all"),
            pl.lit(1.0).alias("eff_proton_all_weighted"),
            pl.lit(0.0).alias("eff_proton_unc")
        ])
        if data_sample == '2017':
            df = df.with_columns([
                pl.lit(1.0).alias("eff_multitrack"),
                pl.lit(1.0).alias("eff_strictzero"),
                pl.lit(1.0).alias("eff_multitrack_weighted"),
                pl.lit(1.0).alias("eff_strictzero_weighted")
            ])


    # Substitua esta seção:
    # if not use_hash_index_:
    #     index_vars_ = ['Run', 'LumiSection', 'EventNum', 'Slice']
    # else:
    #     arr_hash_id_ = np.array([hash(x) for x in df.select("Muon0Pt").to_series()])
    #     df = df.with_columns(pl.Series(name="hash_id", values=arr_hash_id_))
    #     print(df.select("hash_id"))
    #     index_vars_ = ['Run', 'LumiSection', 'EventNum', 'hash_id', 'Slice']
    
    # Com esta:
    use_hash_index_ = True
    index_vars_ = ['Run', 'LumiSection', 'EventNum', 'Slice']
    if use_hash_index_:
        df = df.with_columns(
            pl.col("Muon0Pt").hash().alias("hash_id") # Usar hash nativo do Polars
        )
        print ( df.select( "hash_id" ) )
        index_vars_ = ['Run', 'LumiSection', 'EventNum', 'hash_id', 'Slice']
    else:
        index_vars_ = ['Run', 'LumiSection', 'EventNum', 'Slice']
    
    print ( index_vars_ )
    # df_index = df.unique(subset=index_vars_) # Mova a criação do df_index para depois da seleção/filtragem

    columns_drop_ = [
        'MultiRP', 'Arm', 'RPId1', 'RPId2',
        'TrackX1', 'TrackY1', 'TrackX2', 'TrackY2',
        'TrackThX_SingleRP', 'TrackThY_SingleRP', 'Track1ThX_MultiRP', 'Track1ThY_MultiRP', 'Track2ThX_MultiRP', 'Track2ThY_MultiRP',
        'TrackPixShift_SingleRP', 'Track1PixShift_MultiRP', 'Track2PixShift_MultiRP',
        'Xi', 'T', 'ThX', 'ThY', 'Time'
    ]

    if runOnMC:
        columns_drop_.extend( columns_eff_ )
    print ( columns_drop_ )

    calculate_vars_pp_ = True
    df_events_ = None
    df_2protons_ = None
    df_ximax_ = None
    if proton_selection == "MultiRP":
        if calculate_vars_pp_:
            if select2protons:
                df_events_, df_2protons_ = process_events( data_sample, df_index, select2protons=select2protons, runOnMC=runOnMC, mix_protons=mix_protons, columns_drop=columns_drop_, use_hash_index=use_hash_index_ )
            else:
                df_ximax_ = process_events( data_sample, df_index, select2protons=select2protons, runOnMC=runOnMC, mix_protons=mix_protons, columns_drop=columns_drop_, use_hash_index=use_hash_index_ )
        else:
            labels_xi_ = [ "_nom", "_p100", "_m100" ]
            if runOnMC:
                columns_drop_.extend( [ "Xi" + label_ for label_ in labels_xi_ ] )

            df_events_ = df_index.drop( columns=columns_drop_ )
            df_events_ = df_events_.unique(subset=index_vars_)
            print ( "Number of events: {}".format( df_events_.shape[0] ) )

    print ( df_index )

    if proton_selection == "SingleRP":
        return ( df_index )
    elif proton_selection == "MultiRP":
        if select2protons:
            return ( df_index, df_events_, df_2protons_ )
        else:
            return ( df_index, df_ximax_ )


def process_events(data_sample, df_protons_multiRP_index, select2protons=False, runOnMC=False, mix_protons=False, columns_drop=None, use_hash_index=False):
    index_vars_ = ['Run', 'LumiSection', 'EventNum', 'Slice'] if not use_hash_index else ['Run', 'LumiSection', 'EventNum', 'hash_id', 'Slice']

    groupby_vars_ = index_vars_ + ['Arm']
    df_protons_multiRP_index_xi_max = (
        df_protons_multiRP_index
        .sort(by=groupby_vars_ + ['Xi'], descending=[False] * len(groupby_vars_) + [True])
        .groupby(groupby_vars_)
        .head(1)
    )

    if not select2protons:
        return df_protons_multiRP_index_xi_max

    valid_events = (
        df_protons_multiRP_index_xi_max
        .groupby(index_vars_)
        .agg(pl.col("Arm").sort().alias("sorted_arms"))
        .filter(
            (pl.col("sorted_arms").list.lengths() == 2) &
            (pl.col("sorted_arms").list.contains(0)) &
            (pl.col("sorted_arms").list.contains(1))
        )
        .select(index_vars_)
    )
    df_protons_multiRP_index_2protons = df_protons_multiRP_index_xi_max.join(
        valid_events, on=index_vars_, how="inner"
    )

    df_protons_multiRP_index_2protons = df_protons_multiRP_index_2protons.sort(by=index_vars_)

    df_protons_multiRP_2protons_grouped = df_protons_multiRP_index_2protons.groupby(index_vars_).agg(
        pl.struct(["Arm", "Xi"]).sort_by("Arm").alias("proton_pairs")
    )

    df_protons_multiRP_events = (
        df_protons_multiRP_index_2protons
        .drop(columns=columns_drop)
        .unique(subset=index_vars_)
        .join(
            df_protons_multiRP_2protons_grouped.select(index_vars_ + [
                (13000.0 * (pl.col("proton_pairs").list.get(0).struct.field("Xi") * pl.col("proton_pairs").list.get(1).struct.field("Xi")).sqrt()).alias("MX"),
                (0.5 * (pl.col("proton_pairs").list.get(0).struct.field("Xi").log() - pl.col("proton_pairs").list.get(1).struct.field("Xi").log())).alias("YX")
            ]),
            on=index_vars_,
            how="left"
        )
        .with_columns([
            pl.col("MX").alias("MX_nom"),
            pl.col("YX").alias("YX_nom")
        ])
    )

    # df_protons_multiRP_events = ... join ... with_columns([pl.col("MX").alias("MX_nom"), pl.col("YX").alias("YX_nom")])

    if runOnMC:
        # Definir colunas de eficiência a serem agregadas
        eff_cols_prod = ["eff_proton_all_weighted", "eff_proton_all"]
        eff_cols_unc = ["eff_proton_unc"] # Coluna para soma quadrática
        if data_sample == '2017':
            eff_cols_prod.extend(["eff_multitrack_weighted", "eff_strictzero_weighted", "eff_multitrack", "eff_strictzero"])
    
        # Criar expressões de agregação
        aggs = []
        for col in eff_cols_prod:
            # Agregação por produto para eficiências multiplicativas
            aggs.append(pl.col(col).product().alias(col))
        for col in eff_cols_unc:
            # Agregação por soma quadrática para incertezas
            # Nota: Certifique-se que 'eff_proton_unc' exista antes desta etapa
            aggs.append(
                (pl.col(col).pow(2)).sum().sqrt().alias(col)
            )
    
        # Agrupar e agregar
        df_aggregated_eff = (
            df_protons_multiRP_index_2protons
            .groupby(index_vars_)
            .agg(aggs) # Aplicar todas as agregações
        )
    
        # Juntar os resultados agregados ao DataFrame de eventos
        df_protons_multiRP_events = df_protons_multiRP_events.join(
            df_aggregated_eff, on=index_vars_, how="left"
        )
    
        # Calcular variações up/down após a agregação
        if "eff_proton_unc" in df_protons_multiRP_events.columns:
              df_protons_multiRP_events = df_protons_multiRP_events.with_columns([
                  (1.0 + pl.col("eff_proton_unc")).alias("eff_proton_var_up"),
                  (1.0 - pl.col("eff_proton_unc").fill_null(0.0)).clip(lower_bound=0.0).alias("eff_proton_var_dw") # Evitar eficiência negativa
              ])
        else: # Adiciona colunas dummy se a incerteza não foi calculada/agregada
              df_protons_multiRP_events = df_protons_multiRP_events.with_columns([
                  pl.lit(1.0).alias("eff_proton_var_up"),
                  pl.lit(1.0).alias("eff_proton_var_dw"),
                  pl.lit(0.0).alias("eff_proton_unc") # Adicionar coluna de incerteza zerada
              ])
    
    
    # Adicionar colunas de eficiência/variação com 1.0 ou 0.0 para casos não-MC, se necessário
    # para garantir que o schema final seja consistente
    else: # Caso não seja runOnMC
         eff_cols_to_add = ["eff_proton_all_weighted", "eff_proton_all", "eff_proton_unc", "eff_proton_var_up", "eff_proton_var_dw"]
         if data_sample == '2017':
            eff_cols_to_add.extend(["eff_multitrack_weighted", "eff_strictzero_weighted", "eff_multitrack", "eff_strictzero"])
    
         default_vals = {
              "eff_proton_unc": 0.0,
              "eff_proton_var_up": 1.0,
              "eff_proton_var_dw": 1.0
         }
    
         df_protons_multiRP_events = df_protons_multiRP_events.with_columns([
             pl.lit(default_vals.get(col, 1.0)).alias(col)
             for col in eff_cols_to_add if col not in df_protons_multiRP_events.columns # Adiciona apenas se não existir
         ])
    
    
    if select2protons:
        return (df_protons_multiRP_events, df_protons_multiRP_index_2protons)
    else:
        # Retornar df_protons_multiRP_index_xi_max como antes
        # Certifique-se que df_protons_multiRP_index_xi_max tenha as colunas necessárias
        # Se você precisar das eficiências *antes* da agregação aqui, ajuste a lógica
        return df_protons_multiRP_index_xi_max



#def process_events(data_sample, df_protons_multiRP_index, select2protons=False, runOnMC=False, mix_protons=False, columns_drop=None, use_hash_index=False):
#    index_vars_ = ['Run', 'LumiSection', 'EventNum', 'Slice'] if not use_hash_index else ['Run', 'LumiSection', 'EventNum', 'hash_id', 'Slice']
#
#    groupby_vars_ = index_vars_ + ['Arm']
#    df_protons_multiRP_index_xi_max = (
#        df_protons_multiRP_index
#        .sort(by=groupby_vars_ + ['Xi'], descending=[False]*len(groupby_vars_) + [True])
#        .unique(subset=groupby_vars_)
#    )
#
#    if not select2protons:
#        return df_protons_multiRP_index_xi_max
#
#    counts_df = df_protons_multiRP_index_xi_max.groupby(index_vars_).agg(
#        pl.col("Arm").alias("Arm_list")
#    )
#
#    valid_mask = counts_df.filter(
#        (pl.col("Arm_list").list.count_match(0) == 1) &
#        (pl.col("Arm_list").list.count_match(1) == 1)
#    )
#
#    df_protons_multiRP_index_2protons = df_protons_multiRP_index_xi_max.join(
#        valid_mask.select(index_vars_),
#        on=index_vars_,
#        how="inner"
#    )
#
#    grouped_lists = df_protons_multiRP_index_2protons.groupby(index_vars_).agg([
#        pl.col("Xi").alias("Xi_list"),
#        pl.col("Arm").alias("Arm_list")
#    ])
#
#    grouped_mx = grouped_lists.with_columns(
#        pl.when(pl.col("Xi_list").list.lengths() == 2)
#        .then(13000.0 * (pl.col("Xi_list").list.get(0) * pl.col("Xi_list").list.get(1)).sqrt())
#        .otherwise(None)
#        .alias("MX_nom")
#    )
#
#
#    grouped_yx = grouped_mx.with_columns(
#    pl.when(
#        (pl.col("Arm_list").list.contains(0)) &
#        (pl.col("Arm_list").list.contains(1)) &
#        (pl.col("Arm_list").list.lengths() == 2) &
#        (pl.col("Xi_list").list.lengths() == 2)
#    ).then(
#        pl.struct([
#            pl.col("Arm_list"),
#            pl.col("Xi_list")
#        ]).apply(
#            lambda x: 0.5 * (x[1][x[0].list.index(0)].log() - x[1][x[0].list.index(1)].log())
#            if 0 in x[0] and 1 in x[0] and len(x[0]) == 2 and len(x[1]) == 2
#            else None,
#            return_dtype=pl.Float64
#        )
#    ).otherwise(None)
#    .alias("YX_nom")
#    )
#
#    df_protons_multiRP_events = (
#        df_protons_multiRP_index_2protons
#        .drop(columns_drop)
#        .unique(subset=index_vars_)
#        .join(
#            grouped_yx.select(index_vars_ + ["MX_nom", "YX_nom"]),
#            on=index_vars_,
#            how="left"
#        )
#    )
#
#    if runOnMC:
#        var_list_ = ["Arm", "Xi", "eff_proton_all_weighted", "eff_proton_all", "eff_proton_unc"]
#        if data_sample == '2017':
#            var_list_.extend(["eff_multitrack_weighted", "eff_strictzero_weighted", "eff_multitrack", "eff_strictzero"])
#
#        eff_cols = [col for col in var_list_ if col.startswith("eff_")]
#
#        for col in eff_cols:
#            grouped_eff = (
#                df_protons_multiRP_index_2protons
#                .groupby(index_vars_)
#                .agg(pl.col(col).alias(f"{col}_list"))
#                .with_columns(
#                    pl.col(f"{col}_list")
#                    .list.eval(pl.element().product())
#                    .list.first()
#                    .alias(col)
#                )
#            )
#
#            df_protons_multiRP_events = df_protons_multiRP_events.join(
#                grouped_eff.select([*index_vars_, col]),
#                on=index_vars_,
#                how="left"
#            )
#
#    if select2protons:
#        return (df_protons_multiRP_events, df_protons_multiRP_index_2protons)
#    else:
#        return df_protons_multiRP_index_xi_max
