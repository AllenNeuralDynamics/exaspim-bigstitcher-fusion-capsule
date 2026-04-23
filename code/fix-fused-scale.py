import sys
import json

import zarr
from xarray_multiscale import downscale, windowed_mean
import xarray as xr
import dask.array as da
import numpy as np
import boto3
from botocore.exceptions import ClientError


def read_json_from_s3(path: str, *, s3_client: boto3.client = None) -> dict:
    if s3_client is None:
        s3_client = boto3.client("s3")
    if path.startswith("s3://"):
        path = path.replace("s3://", "")
    bucket, key = path.split('/', 1)[-2:]
    print(bucket, key)

    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        raw_data = response["Body"].read()
        return json.loads(raw_data)
    except ClientError as err:
        if err.response["Error"]["Code"] == "NoSuchKey":
            raise FileNotFoundError(f"{bucket}/{key} does not exist") from err
        raise


def main():
    path = sys.argv[1].rstrip('/')

    # Open the Zarr dataset
    z = zarr.open(path, 'r+')
    tile_group = z

    # Copy attrs to a regular dict so we can edit freely
    meta = dict(tile_group.attrs)

    # ---- REMOVE UNWANTED ZATTRS KEYS ----
    # Remove BigStitcher/Bigstitcher key (handle either capitalization)
    meta.pop("Bigstitcher-Spark", None)

    # Remove "discrete" from each axis object
    # (example shows it under multiscales[0]["axes"])
    try:
        for ms in meta.get("multiscales", []):
            for axis in ms.get("axes", []):
                axis.pop("discrete", None)
    except Exception:
        # If the structure is unexpected, just skip axis cleanup
        pass

    datasets = meta['multiscales'][0]['datasets']
    file_stem = "fused"
    print(file_stem)

    translation = [0, 0, 0]
    scale = [1.0, 0.748, 0.748]

    # Fix rounding errors that occur during Zarr conversion
    for i in range(len(scale)):
        scale[i] = round(scale[i], 4)

    arr = da.from_array(tile_group[0]).squeeze()

    z_coords = translation[0] + scale[0] * np.arange(arr.shape[0])
    y_coords = translation[1] + scale[1] * np.arange(arr.shape[1])
    x_coords = translation[2] + scale[2] * np.arange(arr.shape[2])

    darray = xr.DataArray(
        arr,
        dims=['z', 'y', 'x'],
        coords={'z': z_coords, 'y': y_coords, 'x': x_coords},
        name='image'
    )

    n_scales = len(tile_group.keys())

    adjusted_translations = [translation]
    for i in range(n_scales - 1):
        darray = downscale(darray, windowed_mean, scale_factors=(2,) * darray.ndim)
        first_coord_downscaled = [float(darray.z[0]), float(darray.y[0]), float(darray.x[0])]
        adjusted_translations.append(first_coord_downscaled)

    for i, (ds, adj) in enumerate(zip(datasets, adjusted_translations)):
        translation = ds['coordinateTransformations'][1]['translation']
        translation[-3:] = adj
        ds['coordinateTransformations'][0]['scale'] = [1, 1] + [s * 2**i for s in scale]

    print(meta)

    # IMPORTANT: clear then update, otherwise removed keys can persist in .zattrs
    tile_group.attrs.clear()
    tile_group.attrs.update(meta)


if __name__ == "__main__":
    main()
