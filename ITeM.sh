java -cp ./target/uber-STM-1.3-SNAPSHOT.jar gov.pnnl.stm.algorithms.STM_NodeArrivalRateMultiType -Dspark.master=local[*] \
-input_file="D:/localcode/STM/data/testGDifficult2.csv" -out_json_file_os_path="D:\localdata\mao_global_edges_ITeM_log2.json" -separator="," \
-avg_outdeg_file="D:\localdata\mao_global_edges_ITeM_log2_deg.csv" \
-sampling=false \
-sampling_population=16 \
-sample_selection_prob=1.0 \
-valid_etypes=1 \
-num_iterations=1 \
-delta_limit=false \
-t_delta=600 \
-max_cores=4 \
-k_top=4 \
-base_out_dir=".\output\autorunTest\\"

echo "ITeM Processing Done. Generating embeddings\n"

python STMGetEmbedding.py ./output/autorunTest/ ./output/autorunTest/  
