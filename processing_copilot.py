import h5py
import polars as pl
import numpy as np

# Definição dos períodos e luminosidades para os anos de 2017 e 2018
run_ranges_periods_2017 = {
    "2017B": [297020, 299329],
    "2017C1": [299337, 300785],
    "2017C2": [300806, 302029],
    "2017D": [302030, 303434],
    "2017E": [303435, 304826],
    "2017F1": [304911, 305114],
    "2017F2": [305178, 305902],
    "2017F3": [305965, 306462]
}

df_run_ranges_2017 = pl.DataFrame([
    {"period": key, "min": val[0], "max": val[1]}
    for key, val in run_ranges_periods_2017.items()
])

run_ranges_periods_2018 = {
    "2018A": [315252, 316995],
    "2018B1": [316998, 317696],
    "2018B2": [318622, 319312],
    "2018C": [319313, 320393],
    "2018D1": [320394, 322633],
    "2018D2": [323363, 325273]
}

df_run_ranges_2018 = pl.DataFrame([
    {"period": key, "min": val[0], "max": val[1]}
    for key, val in run_ranges_periods_2018.items()
])

# Definições de luminosidade
lumi_periods_2017 = {
    'muon': {
        "2017B": 4.799881474,
        "2017C1": 5.785813941,
        "2017C2": 3.786684323,
        "2017D": 4.247682053,
        "2017E": 9.312832062,
        "2017F1": 1.738905587,
        "2017F2": 8.125575961,
        "2017F3": 3.674404546
    },
    'electron': {
        "2017B": 4.799881474 * 0.957127,
        "2017C1": 5.785813941 * 0.954282,
        "2017C2": 3.786684323 * 0.954282,
        "2017D": 4.247682053 * 0.9539,
        "2017E": 9.312832062 * 0.956406,
        "2017F1": 1.738905587 * 0.953733,
        "2017F2": 8.125575961 * 0.953733,
        "2017F3": 3.674404546 * 0.953733
    }
}

lumi_periods_2018 = {
    'muon': {
        "2018A": 14.027047499 * 0.999913,
        "2018B1": 6.629673574 * 0.998672,
        "2018B2": 0.430948924 * 0.998672,
        "2018C": 6.891747024 * 0.999991,
        "2018D1": 20.962647459 * 0.998915,
        "2018D2": 10.868724698 * 0.998915
    },
    'electron': {
        "2018A": 14.027047499 * 0.933083,
        "2018B1": 6.629673574 * 0.999977,
        "2018B2": 0.430948924 * 0.999977,
        "2018C": 6.891747024 * 0.999978,
        "2018D1": 20.962647459 * 0.999389,
        "2018D2": 10.868724698 * 0.999389
    }
}

# Mapas de períodos e reconstrução
aperture_period_map = {
    "2016_preTS2": "2016_preTS2",
    "2016_postTS2": "2016_postTS2",
    "2017B": "2017_preTS2",
    "2017C1": "2017_preTS2",
    "2017C2": "2017_preTS2",
    "2017D": "2017_preTS2",
    "2017E": "2017_postTS2",
    "2017F1": "2017_postTS2",
    "2017F2": "2017_postTS2",
    "2017F3": "2017_postTS2",
    "2018A": "2018",
    "2018B1": "2018",
    "2018B2": "2018",
    "2018C": "2018",
    "2018D1": "2018",
    "2018D2": "2018"
}

reco_period_map = {
    "2016_preTS2": "2016_preTS2",
    "2016_postTS2": "2016_postTS2",
    "2017B": "2017_preTS2",
    "2017C1": "2017_preTS2",
    "2017C2": "2017_preTS2",
    "2017D": "2017_preTS2",
    "2017E": "2017_postTS2",
    "2017F1": "2017_postTS2",
    "2017F2": "2017_postTS2",
    "2017F3": "2017_postTS2",
    "2018A": "2018_preTS1",
    "2018B1": "2018_TS1_TS2",
    "2018B2": "2018_TS1_TS2",
    "2018C": "2018_TS1_TS2",
    "2018D1": "2018_postTS2",
    "2018D2": "2018_postTS2"
}

# Cortes Fiduciais
def fiducial_cuts():
    # Criação de dicionários para armazenar os cortes fiduciais
    fiducialXLow_ = {}
    fiducialXHigh_ = {}
    fiducialYLow_ = {}
    fiducialYHigh_ = {}

    # Definição dos períodos de dados
    data_periods = ["2017B", "2017C1", "2017E", "2017F1", "2018A", "2018B1", "2018B2", "2018C", "2018D1", "2018D2"]
    for period_ in data_periods:
        fiducialXLow_[period_] = {0: {}, 1: {}}
        fiducialXHigh_[period_] = {0: {}, 1: {}}
        fiducialYLow_[period_] = {0: {}, 1: {}}
        fiducialYHigh_[period_] = {0: {}, 1: {}}
    
    # Adiciona valores específicos para cada período aqui...
    return fiducialXLow_, fiducialXHigh_, fiducialYLow_, fiducialYHigh_

# Função para carregar dados de arquivos HDF5
def get_data(fileNames, selection=None, version="V1"):
    # Verificações iniciais
    if version not in ("V1", "V2"):
        raise ValueError(f"Unsupported version: {version}")
    
    # Configuração inicial de listas para armazenar os DataFrames
    df_list = [] if version == "V1" else []
    chunk_size = 1000000

    for file_ in fileNames:
        with h5py.File(file_, 'r') as f:
            # Carrega o conjunto de dados principal e as colunas
            dset = f['protons'] if version == "V1" else f['protons_multiRP']
            columns = [item.decode("utf-8") for item in f['columns']]
            
            # Itera em chunks para evitar sobrecarga de memória
            for idx in range(0, dset.shape[0], chunk_size):
                data_chunk = dset[idx: idx + chunk_size]
                df_chunk = pl.DataFrame({col: data_chunk[:, i] for i, col in enumerate(columns)})

                # Casting os tipos das colunas para garantir consistência
                df_chunk = df_chunk.with_columns([
                    pl.col(col).cast(pl.Int64) if col in ['Run', 'LumiSection', 'EventNum'] else pl.col(col)
                    for col in columns
                ])
                
                # Aplica a seleção, se fornecida
                if selection:
                    df_chunk = selection(df_chunk)
                
                # Adiciona o chunk processado à lista
                df_list.append(df_chunk)
    
    # Concatena todos os chunks processados em um único DataFrame
    df = pl.concat(df_list)
    return df


# Função para aplicar cortes fiduciais a todas as amostras de dados
def fiducial_cuts_all(data_sample):
    # Define os períodos de acordo com o ano de dados
    data_periods = {
        '2017': ["2017B", "2017C1", "2017C2", "2017D", "2017E", "2017F1", "2017F2", "2017F3"],
        '2018': ["2018A", "2018B1", "2018B2", "2018C", "2018D1", "2018D2"]
    }.get(data_sample, [])

    # Obtém os cortes fiduciais para os períodos especificados
    fiducialXLow_, fiducialXHigh_, fiducialYLow_, fiducialYHigh_ = fiducial_cuts()

    # Inicializa as estruturas para armazenar os cortes combinados
    fiducialXLow_all = {}
    fiducialXHigh_all = {}
    fiducialYLow_all = {}
    fiducialYHigh_all = {}

    # Combina os cortes para todos os períodos
    for arm_ in (0, 1):
        fiducialXLow_all[arm_] = {2: []}
        fiducialXHigh_all[arm_] = {2: []}
        fiducialYLow_all[arm_] = {2: []}
        fiducialYHigh_all[arm_] = {2: []}

    for period_ in data_periods:
        for arm_ in (0, 1):
            fiducialXLow_all[arm_][2].append(fiducialXLow_[period_][arm_][2])
            fiducialXHigh_all[arm_][2].append(fiducialXHigh_[period_][arm_][2])
            fiducialYLow_all[arm_][2].append(fiducialYLow_[period_][arm_][2])
            fiducialYHigh_all[arm_][2].append(fiducialYHigh_[period_][arm_][2])

    # Calcula os valores combinados para cada corte
    for arm_ in (0, 1):
        fiducialXLow_all[arm_][2] = np.max(fiducialXLow_all[arm_][2])
        fiducialXHigh_all[arm_][2] = np.min(fiducialXHigh_all[arm_][2])
        fiducialYLow_all[arm_][2] = np.max(fiducialYLow_all[arm_][2])
        fiducialYHigh_all[arm_][2] = np.min(fiducialYHigh_all[arm_][2])

    return fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all


# Função para processar os dados com base nas seleções e cortes fornecidos
def process_data(df, data_sample, lepton_type, proton_selection, min_mass=0., min_pt_1=-1, min_pt_2=-1,
                 apply_fiducial=True, within_aperture=False, random_protons=False, mix_protons=False, runOnMC=False):

    if runOnMC:
        print("Turning within_aperture OFF for MC.")
        within_aperture = False

    # Aplicação de máscaras iniciais com base em massa e pT mínimos
    msk = (df["InvMass"] >= min_mass)
    if min_pt_1 > 0:
        msk &= (df["Muon0Pt"] >= min_pt_1)
    if min_pt_2 > 0:
        msk &= (df["Muon1Pt"] >= min_pt_2)

    # Inicialização de cortes fiduciais, se necessário
    if apply_fiducial:
        fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all = fiducial_cuts_all(data_sample)

    # Configuração de strings de execução e ângulo de cruzamento
    run_str_ = "Run_rnd" if (random_protons or mix_protons) else ("Run_mc" if runOnMC else "Run")
    xangle_str_ = "CrossingAngle_rnd" if (random_protons or mix_protons) else "CrossingAngle"

    # Seleção do DataFrame de intervalos de execução
    df_run_ranges_ = df_run_ranges_2017 if data_sample == '2017' else df_run_ranges_2018

    # Adiciona coluna de período aos dados, se necessário
    if "period" not in df.columns:
        df = df.with_columns(pl.lit(None).alias("period"))
        for idx_ in range(df_run_ranges_.shape[0]):
            msk_period_ = (df[run_str_] >= df_run_ranges_["min"][idx_]) & (
                    df[run_str_] <= df_run_ranges_["max"][idx_])
            period_key_ = df_run_ranges_["period"][idx_]
            df = df.with_columns(
                pl.when(msk_period_).then(period_key_).otherwise(pl.col("period")).alias("period")
            )

    # Aplicação de seleções específicas com base no tipo de prótons
    if proton_selection == "SingleRP":
        # Processamento para prótons SingleRP
        df = process_single_rp(df, fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all, msk,
                               apply_fiducial)

    elif proton_selection == "MultiRP":
        # Processamento para prótons MultiRP
        df = process_multi_rp(df, fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all, msk,
                              apply_fiducial, within_aperture)

    return df


# Função auxiliar para processar prótons SingleRP
def process_single_rp(df, fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all, msk,
                      apply_fiducial):
    # Máscaras para braços específicos e cortes fiduciais
    msk1_arm = (df["RPId1"] == 23)
    msk2_arm = (df["RPId1"] == 123)

    # Inicializa colunas de XiMuMu e cortes
    df = df.with_columns([
        pl.lit(None).alias("XiMuMu"),
        pl.when(~msk1_arm).then(df["XiMuMuPlus"]).otherwise(pl.col("XiMuMu")).alias("XiMuMu"),
        pl.when(~msk2_arm).then(df["XiMuMuMinus"]).otherwise(pl.col("XiMuMu")).alias("XiMuMu")
    ])

    if apply_fiducial:
        df = apply_fiducial_cuts(df, fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all,
                                 msk1_arm, msk2_arm)

    return df


# Função auxiliar para aplicar cortes fiduciais
def apply_fiducial_cuts(df, fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all, msk1_arm,
                        msk2_arm):
    # Adiciona colunas para cortes fiduciais
    df = df.with_columns([
        pl.lit(None).alias("xlow"),
        pl.lit(None).alias("xhigh"),
        pl.lit(None).alias("ylow"),
        pl.lit(None).alias("yhigh")
    ])
    df = df.with_columns([
        pl.when(~msk1_arm).then(fiducialXLow_all[0][2]).otherwise(pl.col("xlow")).alias("xlow"),
        pl.when(~msk1_arm).then(fiducialXHigh_all[0][2]).otherwise(pl.col("xhigh")).alias("xhigh"),
        pl.when(~msk1_arm).then(fiducialYLow_all[0][2]).otherwise(pl.col("ylow")).alias("ylow"),
        pl.when(~msk1_arm).then(fiducialYHigh_all[0][2]).otherwise(pl.col("yhigh")).alias("yhigh"),
        pl.when(~msk2_arm).then(fiducialXLow_all[1][2]).otherwise(pl.col("xlow")).alias("xlow"),
        pl.when(~msk2_arm).then(fiducialXHigh_all[1][2]).otherwise(pl.col("xhigh")).alias("xhigh"),
        pl.when(~msk2_arm).then(fiducialYLow_all[1][2]).otherwise(pl.col("ylow")).alias("ylow"),
        pl.when(~msk2_arm).then(fiducialYHigh_all[1][2]).otherwise(pl.col("yhigh")).alias("yhigh")
    ])
    return df

# Função auxiliar para processar prótons MultiRP
def process_multi_rp(df, fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all, msk, apply_fiducial, within_aperture):
    # Máscaras para braços específicos
    msk_multiRP = (df["MultiRP"] == 1)
    msk1_arm = (df["Arm"] == 0)
    msk2_arm = (df["Arm"] == 1)

    # Inicializa colunas de XiMuMu
    df = df.with_columns([
        pl.lit(None).alias("XiMuMu"),
        pl.when(~msk1_arm).then(df["XiMuMuPlus"]).otherwise(pl.col("XiMuMu")).alias("XiMuMu"),
        pl.when(~msk2_arm).then(df["XiMuMuMinus"]).otherwise(pl.col("XiMuMu")).alias("XiMuMu")
    ])

    # Máscara para verificar shifts de pixel
    msk_pixshift = ((df["Track1PixShift_MultiRP"] == 0) & (df["Track2PixShift_MultiRP"] == 0))

    # Verificação dentro do "aperture" se necessário
    if within_aperture:
        df = df.with_columns(
            pl.lit(None).alias("within_aperture")
        )
        df = df.with_columns(
            pl.when(msk_multiRP).then(
                df.apply(
                    lambda row: check_aperture(aperture_period_map.get(row["period"], ""), row["Arm"], row["CrossingAngle"], row["Xi"], row["ThX"]),
                    axis=1
                )
            ).otherwise(pl.col("within_aperture")).alias("within_aperture")
        )

    # Aplicação de cortes fiduciais, se habilitado
    if apply_fiducial:
        df = apply_fiducial_cuts(df, fiducialXLow_all, fiducialXHigh_all, fiducialYLow_all, fiducialYHigh_all, msk1_arm, msk2_arm)

    # Máscaras finais para seleção de prótons válidos
    msk1 = msk & msk_pixshift & msk_multiRP & msk1_arm
    msk2 = msk & msk_pixshift & msk_multiRP & msk2_arm

    if within_aperture:
        msk1 &= df["within_aperture"]
        msk2 &= df["within_aperture"]

    # Filtra o DataFrame com base nas máscaras finais
    df = df.filter(msk1 | msk2)
    return df


# Função para calcular eventos processados
def process_events(data_sample, df_protons_multiRP_index, select2protons=False, runOnMC=False, mix_protons=False, columns_drop=None, use_hash_index=False):
    # Definição de variáveis de índice e agrupamento
    index_vars_ = ['Run', 'LumiSection', 'EventNum', 'Slice'] if not use_hash_index else ['Run', 'LumiSection', 'EventNum', 'hash_id', 'Slice']
    groupby_vars_ = index_vars_ + ['Arm']

    # Seleção do máximo de Xi por grupo
    df_protons_multiRP_index_xi_max = (
        df_protons_multiRP_index
        .sort(by=groupby_vars_ + ['Xi'], descending=[False] * len(groupby_vars_) + [True])
        .groupby(groupby_vars_)
        .head(1)
    )

    # Seleção de eventos com exatamente dois prótons válidos
    if select2protons:
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
        df_protons_multiRP_index_2protons = df_protons_multiRP_index_xi_max.join(valid_events, on=index_vars_, how="inner")

        # Agrupar e calcular variáveis derivadas
        df_protons_multiRP_events = (
            df_protons_multiRP_index_2protons
            .drop(columns=columns_drop)
            .unique(subset=index_vars_)
            .join(
                df_protons_multiRP_index_2protons.groupby(index_vars_).agg(
                    pl.struct(["Arm", "Xi"]).sort_by("Arm").alias("proton_pairs")
                ).select(index_vars_ + [
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

        # Adiciona colunas de eficiência, se necessário
        if runOnMC:
            eff_cols_prod = ["eff_proton_all_weighted", "eff_proton_all"]
            eff_cols_unc = ["eff_proton_unc"]

            if data_sample == '2017':
                eff_cols_prod.extend(["eff_multitrack_weighted", "eff_strictzero_weighted", "eff_multitrack", "eff_strictzero"])

            # Agregações de eficiência
            aggs = []
            for col in eff_cols_prod:
                aggs.append(pl.col(col).product().alias(col))
            for col in eff_cols_unc:
                aggs.append(
                    (pl.col(col).pow(2)).sum().sqrt().alias(col)
                )

            df_aggregated_eff = (
                df_protons_multiRP_index_2protons
                .groupby(index_vars_)
                .agg(aggs)
            )

            df_protons_multiRP_events = df_protons_multiRP_events.join(
                df_aggregated_eff, on=index_vars_, how="left"
            )

            df_protons_multiRP_events = df_protons_multiRP_events.with_columns([
                (1.0 + pl.col("eff_proton_unc")).alias("eff_proton_var_up"),
                (1.0 - pl.col("eff_proton_unc").fill_null(0.0)).clip(lower_bound=0.0).alias("eff_proton_var_dw")
            ])

        return df_protons_multiRP_events

    return df_protons_multiRP_index_xi_max

# Função auxiliar para verificar condições de abertura de prótons
def check_aperture(period, arm, xangle, xi, theta_x):
    # Parametrização de abertura baseada no período
    aperture_value = aperture_parametrisation(period, arm, xangle, xi)
    return theta_x < -aperture_value


# Função para calcular a parametrização de abertura
def aperture_parametrisation(period, arm, xangle, xi):
    # Retorna valores baseados no período e condições
    if period == "2016_preTS2":
        if arm == 0:
            return 3.76296E-05 + ((xi < 0.117122) * 0.00712775 + (xi >= 0.117122) * 0.0148651) * (xi - 0.117122)
        elif arm == 1:
            return 1.85954E-05 + ((xi < 0.14324) * 0.00475349 + (xi >= 0.14324) * 0.00629514) * (xi - 0.14324)
    elif period == "2016_postTS2":
        if arm == 0:
            return 6.10374E-05 + ((xi < 0.113491) * 0.00795942 + (xi >= 0.113491) * 0.01935) * (xi - 0.113491)
        elif arm == 1:
            return (xi - 0.110) / 130.0
    elif period == "2017_preTS2":
        if arm == 0:
            return -(8.71198E-07 * xangle - 0.000134726) + ((xi < (0.000264704 * xangle + 0.081951))
                                                            * -(4.32065E-05 * xangle - 0.0130746)
                                                            + (xi >= (0.000264704 * xangle + 0.081951))
                                                            * -(0.000183472 * xangle - 0.0395241))
        elif arm == 1:
            return 3.43116E-05 + ((xi < (0.000626936 * xangle + 0.061324))
                                  * 0.00654394
                                  + (xi >= (0.000626936 * xangle + 0.061324))
                                  * -(0.000145164 * xangle - 0.0272919)) * (xi - (0.000626936 * xangle + 0.061324))
    elif period == "2017_postTS2":
        if arm == 0:
            return -(8.92079E-07 * xangle - 0.000150214) + ((xi < (0.000278622 * xangle + 0.0964383))
                                                            * -(3.9541e-05 * xangle - 0.0115104)
                                                            + (xi >= (0.000278622 * xangle + 0.0964383))
                                                            * -(0.000108249 * xangle - 0.0249304))
        elif arm == 1:
            return 4.56961E-05 + ((xi < (0.00075625 * xangle + 0.0643361))
                                  * -(3.01107e-05 * xangle - 0.00985126)
                                  + (xi >= (0.00075625 * xangle + 0.0643361))
                                  * -(8.95437e-05 * xangle - 0.0169474)) * (xi - (0.00075625 * xangle + 0.0643361))
    elif period == "2018":
        if arm == 0:
            return -(8.44219E-07 * xangle - 0.000100957) + ((xi < (0.000247185 * xangle + 0.101599))
                                                            * -(1.40289E-05 * xangle - 0.00727237)
                                                            + (xi >= (0.000247185 * xangle + 0.101599))
                                                            * -(0.000107811 * xangle - 0.0261864))
        elif arm == 1:
            return -(-4.74758E-07 * xangle + 3.0881E-05) + ((xi < (0.000727859 * xangle + 0.0722653))
                                                            * -(2.43968E-05 * xangle - 0.0085461)
                                                            + (xi >= (0.000727859 * xangle + 0.0722653))
                                                            * -(7.19216E-05 * xangle - 0.0148212))
    return -999.0


# Adiciona colunas de eficiência padrão para casos não-MC
def add_default_efficiency_columns(df, data_sample, runOnMC):
    # Lista de colunas de eficiência
    eff_cols_to_add = ["eff_proton_all_weighted", "eff_proton_all", "eff_proton_unc", "eff_proton_var_up", "eff_proton_var_dw"]
    
    if data_sample == '2017':
        eff_cols_to_add.extend(["eff_multitrack_weighted", "eff_strictzero_weighted", "eff_multitrack", "eff_strictzero"])

    # Valores padrão
    default_vals = {
        "eff_proton_unc": 0.0,
        "eff_proton_var_up": 1.0,
        "eff_proton_var_dw": 1.0
    }

    # Adiciona as colunas ao DataFrame
    df = df.with_columns([
        pl.lit(default_vals.get(col, 1.0)).alias(col)
        for col in eff_cols_to_add if col not in df.columns
    ])
    return df


# Função para gerenciar eficiências em eventos processados
def handle_efficiencies(df_protons_multiRP_events, df_protons_multiRP_index_2protons, data_sample, runOnMC):
    if runOnMC:
        # Definir colunas de eficiência para agregação
        eff_cols_prod = ["eff_proton_all_weighted", "eff_proton_all"]
        eff_cols_unc = ["eff_proton_unc"]  # Soma quadrática

        if data_sample == '2017':
            eff_cols_prod.extend(["eff_multitrack_weighted", "eff_strictzero_weighted", "eff_multitrack", "eff_strictzero"])

        # Agregar eficiências
        aggs = []
        for col in eff_cols_prod:
            aggs.append(pl.col(col).product().alias(col))
        for col in eff_cols_unc:
            aggs.append(
                (pl.col(col).pow(2)).sum().sqrt().alias(col)
            )

        # Agrega as eficiências
        df_aggregated_eff = (
            df_protons_multiRP_index_2protons
            .groupby(['Run', 'LumiSection', 'EventNum', 'Slice'])
            .agg(aggs)
        )

        # Junta as eficiências ao DataFrame de eventos
        df_protons_multiRP_events = df_protons_multiRP_events.join(
            df_aggregated_eff, on=['Run', 'LumiSection', 'EventNum', 'Slice'], how="left"
        )

        # Adiciona colunas de variação de eficiência
        df_protons_multiRP_events = df_protons_multiRP_events.with_columns([
            (1.0 + pl.col("eff_proton_unc")).alias("eff_proton_var_up"),
            (1.0 - pl.col("eff_proton_unc").fill_null(0.0)).clip(lower_bound=0.0).alias("eff_proton_var_dw")
        ])
    else:
        # Adiciona colunas padrão para não-MC
        df_protons_multiRP_events = add_default_efficiency_columns(df_protons_multiRP_events, data_sample, runOnMC)

    return df_protons_multiRP_events


# Função para processar eventos para MultiRP (com suporte a 2 prótons)
def process_events_with_2protons(df_protons_multiRP_index, data_sample, runOnMC):
    # Seleciona eventos válidos com dois prótons
    valid_events = (
        df_protons_multiRP_index
        .groupby(['Run', 'LumiSection', 'EventNum', 'Slice'])
        .agg(pl.col("Arm").sort().alias("sorted_arms"))
        .filter(
            (pl.col("sorted_arms").list.lengths() == 2) &
            (pl.col("sorted_arms").list.contains(0)) &
            (pl.col("sorted_arms").list.contains(1))
        )
    )

    # Junta os eventos válidos
    df_protons_multiRP_index_2protons = df_protons_multiRP_index.join(
        valid_events.select(['Run', 'LumiSection', 'EventNum', 'Slice']),
        on=['Run', 'LumiSection', 'EventNum', 'Slice'],
        how="inner"
    )

    # Calcula variáveis derivadas (MX e YX)
    df_protons_multiRP_events = (
        df_protons_multiRP_index_2protons
        .groupby(['Run', 'LumiSection', 'EventNum', 'Slice'])
        .agg([
            (13000.0 * (pl.col("Xi").list.get(0) * pl.col("Xi").list.get(1)).sqrt()).alias("MX"),
            (0.5 * (pl.col("Xi").list.get(0).log() - pl.col("Xi").list.get(1).log())).alias("YX")
        ])
        .with_columns([
            pl.col("MX").alias("MX_nom"),
            pl.col("YX").alias("YX_nom")
        ])
    )

    return df_protons_multiRP_events

