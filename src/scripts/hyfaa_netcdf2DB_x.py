import xarray as xr
import numpy as np
import pandas as pd

filepath = "/home/jean/dev/IRD/hyfaa-mgb-platform/hyfaa-backend/sample_data/post_processing_portal.nc"
ds = xr.open_dataset(filepath, mask_and_scale=True)
keep_variables=[
    'water_elevation_catchment_analysis_mean',
    'water_elevation_catchment_analysis_median',
    'water_elevation_catchment_analysis_std',
    'streamflow_catchment_analysis_mean',
    'streamflow_catchment_analysis_median',
    'streamflow_catchment_analysis_std'
] # we could also use drop_variables argument in open_dataset
df = ds.sel(n_time=1423).fillna(0).to_dataframe().filter(items=keep_variables)
# df = df.replace(to_replace=9.685657, value=None) # does not work apparently
print(df)
# me produit le tableau de valeur à insérer (filtrer ce qui ne m'intéresse pas, using xarray.Dataset.drop_vars)
# output:
#            time  ...  streamflow_catchment_analysis_std
# n_cells           ...
# 0        25963.0  ...                       1.422304e-02
# 1        25963.0  ...                       1.423629e-01
# 2        25963.0  ...                       1.428340e-05
# 3        25963.0  ...                       5.804570e-12
# 4        25963.0  ...                       4.718808e-05
#           ...  ...                                ...
# 11590    25963.0  ...                       4.205429e+01
# 11591    25963.0  ...                       4.348390e+01
# 11592    25963.0  ...                       4.466470e+01
# 11593    25963.0  ...                       4.703321e+01
# 11594    25963.0  ...                       4.840752e+01
# [11595 rows x 17 columns]

# But does not support masked_arrays, donc useless for me right now