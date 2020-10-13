java -jar ./target/uber-STM-1.3-SNAPSHOT.jar -Dspark.master=local[*] -input_file="D:\localdata\kdd_tech-as-topology.csv"
-out_json_file_os_path="D:\localdata\mao_global_edges_ITeM_log2.json"
-separator=","
-avg_outdeg_file="D:\localdata\mao_global_edges_ITeM_log2_deg.csv"
-sampling=false
-sampling_population=16
-sample_selection_prob=1.0
-valid_etypes=1
-num_iterations=1
-delta_limit=false
-t_delta=600
-max_cores=4
-k_top=4
-base_out_dir=".\output\itemfreq\"

mkdir ./output/itemfreq_output

python STMGetEmbedding.py ./output/itemfreq/ ./output/itemfreq_output/  
