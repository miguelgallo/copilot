from processing_copilot import *  # Importa o cabeçalho atualizado com Polars

lepton_type = "muon"
data_sample = '2018'
base_path = "output"
output_dir = "output_polars"

labels_signals = ["GGToMuMu_Pt-25_Elastic", "GGToMuMu_Pt-25_Inel-El", "GGToMuMu_Pt-25_Inel-Inel"]

fileNames_signals = {
    'GGToMuMu_Pt-25_Elastic': ['output-test-GGToMuMu_Pt-25_Elastic-PreSel-Pt1_30-Pt2_20.h5'],
    'GGToMuMu_Pt-25_Inel-El': ['output-test-GGToMuMu_Pt-25_Inel-El-PreSel-Pt1_30-Pt2_20.h5'],
    'GGToMuMu_Pt-25_Inel-Inel': ['output-test-GGToMuMu_Pt-25_Inel-Inel-PreSel-Pt1_30-Pt2_20.h5'],
}

# Atualiza os caminhos dos arquivos de entrada, se necessário
for key_ in fileNames_signals:
    if base_path is not None and base_path != "":
        fileNames_signals[key_] = [f"{base_path}/{item_}" for item_ in fileNames_signals[key_]]
print(labels_signals)
print(fileNames_signals)

# Itera sobre cada sinal e processa os dados
for label_ in labels_signals:
    import time
    print(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime()))
    time_s_ = time.time()

    # Define o caminho de saída para o arquivo processado
    file_name_label_ = f"data-store-test-{label_}.h5"
    file_path_ = f"{output_dir}/{file_name_label_}" if output_dir else file_name_label_
    print(file_path_)

    # Utiliza Polars para processar e salvar os dados
    with h5py.File(file_path_, 'w') as store_:
        # Ajusta o número de retornos da função get_data
        result = get_data(fileNames_signals[label_], version='V2')

        # Verifica o número de valores retornados
        if len(result) == 3:
            df_counts_, df_protons_multiRP_, df_protons_singleRP_ = result
        elif len(result) == 2:
            df_counts_, df_protons_multiRP_ = result
            df_protons_singleRP_ = None  # Define como None se não estiver presente
        else:
            raise ValueError("Unexpected number of values returned by get_data")

        # Processa os dados para MultiRP com seleção de dois prótons
        df_protons_multiRP_events_ = process_events_with_2protons(
            df_protons_multiRP_,
            data_sample=data_sample,
            runOnMC=True
        )

        print(df_protons_multiRP_events_)

        # Salva os resultados nos grupos do arquivo HDF5
        store_.create_dataset("counts", data=df_counts_.to_numpy())
        store_.create_dataset("protons_multiRP", data=df_protons_multiRP_.to_numpy())
        store_.create_dataset("events_multiRP", data=df_protons_multiRP_events_.to_numpy())

    time_e_ = time.time()
    print(f"Total time elapsed: {time_e_ - time_s_:.0f} seconds")

    # Valida os dados processados
    with h5py.File(file_path_, 'r') as store_:
        print(list(store_))
