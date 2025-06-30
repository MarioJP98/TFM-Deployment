import os
import glob
import numpy as np
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

def extract_features(path):
    try:
        import hdf5_getters as GETTERS
        h5 = GETTERS.open_h5_file_read(path)
        features = {
            'song_id': GETTERS.get_song_id(h5).decode('utf-8'),
            'title': GETTERS.get_title(h5).decode('utf-8'),
            'artist': GETTERS.get_artist_name(h5).decode('utf-8'),
            'tempo': np.float32(GETTERS.get_tempo(h5)),
            'key': np.int8(GETTERS.get_key(h5)),
            'mode': np.int8(GETTERS.get_mode(h5)),
            'time_signature': np.int8(GETTERS.get_time_signature(h5)),
            'loudness': np.float32(GETTERS.get_loudness(h5)),
            'danceability': np.float32(GETTERS.get_danceability(h5))
        }
        h5.close()
        return features
    except Exception as e:
        return None

def main():
    dataset_path = r"C:\Dataset\msd_targz\F\F"
    output_dir = r"C:\Dataset\parquet\batches_F"
    batch_size = 5000
    num_workers = min(12, multiprocessing.cpu_count())

    os.makedirs(output_dir, exist_ok=True)
    file_paths = glob.glob(f"{dataset_path}\\**\\*.h5", recursive=True)

    for i in range(0, len(file_paths), batch_size):
        batch_paths = file_paths[i:i + batch_size]
        print(f"\n Procesando batch {i // batch_size + 1} ({len(batch_paths)} archivos)")

        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            results = list(tqdm(executor.map(extract_features, batch_paths),
                                total=len(batch_paths),
                                desc=f"Batch {i // batch_size + 1}"))

        batch_data = [r for r in results if r]

        if batch_data:
            df_batch = pd.DataFrame(batch_data)
            batch_file = os.path.join(output_dir, f"batch_{i // batch_size + 1}.parquet")
            df_batch.to_parquet(batch_file, index=False, compression="snappy")
            print(f" Guardado: {batch_file} ({len(df_batch)} canciones)")

if __name__ == "__main__":
    main()
