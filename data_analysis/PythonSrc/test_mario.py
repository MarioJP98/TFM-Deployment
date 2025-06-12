import hdf5_getters as GETTERS
import h5py
import glob

# Localiza un archivo de canción
filename = glob.glob('C:\Users\Mario\Documents\1.Estudios\1.UNIR\TFM\Datasets\millionsongsubset\MillionSongSubset\A\A\A\TRAAAAW128F429D538.h5')[0]

# Abre el archivo
h5 = h5py.File(filename, 'r')

# Obtén algunos datos
artist_name = GETTERS.get_artist_name(h5)
song_title = GETTERS.get_title(h5)
year = GETTERS.get_year(h5)
duration = GETTERS.get_duration(h5)

print(
    f"Artist: {artist_name}, Song: {song_title}, Year: {year}, Duration: {duration:.2f}s")

h5.close()
