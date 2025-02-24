import logging
import multiprocessing
import os
import shutil
import time
import warnings
from datetime import datetime
from functools import partial
from math import ceil
from multiprocessing import shared_memory
from pathlib import Path

from dask.distributed import Client, LocalCluster

import geopandas as gpd
import numpy as np
import pandas as pd
import psutil
import xarray as xr
from data_processing.file_paths import file_paths
from data_processing.zarr_utils import get_forcing_data
from exactextract import exact_extract
from exactextract.raster import NumPyRasterSource
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from typing import Tuple


logger = logging.getLogger(__name__)
# Suppress the specific warning from numpy to keep the cli output clean
warnings.filterwarnings(
    "ignore", message="'DataFrame.swapaxes' is deprecated", category=FutureWarning
)
warnings.filterwarnings(
    "ignore", message="'GeoDataFrame.swapaxes' is deprecated", category=FutureWarning
)


def weighted_sum_of_cells(flat_raster: np.ndarray, 
                          cell_ids: np.ndarray, 
                          factors: np.ndarray) -> np.ndarray:
    '''  
    Take an average of each forcing variable in a catchment. Create an output
    array initialized with zeros, and then sum up the forcing variable and 
    divide by the sum of the cell weights to get an averaged forcing variable 
    for the entire catchment.

    Parameters
    ----------
    flat_raster : np.ndarray
        An array of dimensions (time, x*y) containing forcing variable values
        in each cell. Each element in the array corresponds to a cell ID.
    cell_ids : np.ndarray
        A list of the raster cell IDs that intersect the study catchment.
    factors : np.ndarray
        A list of the weights (coverages) of each cell in cell_ids.

    Returns
    -------
    np.ndarray
        An one-dimensional array, where each element corresponds to a timestep.
        Each element contains the averaged forcing value for the whole catchment
        over one timestep.
    '''
    result = np.zeros(flat_raster.shape[0])
    result = np.sum(flat_raster[:, cell_ids] * factors, axis=1)
    sum_of_weights = np.sum(factors)
    result /= sum_of_weights
    return result


def get_cell_weights(raster: xr.Dataset, 
                     gdf: gpd.GeoDataFrame, 
                     wkt: str) -> pd.DataFrame:
    '''
    Get the cell weights (coverage) for each cell in a divide. Coverage is 
    defined as the fraction (a float in [0,1]) of a raster cell that overlaps 
    with the polygon in the passed gdf.

    Parameters
    ----------
    raster : xr.Dataset
        One timestep of a gridded forcings dataset.
    gdf : gpd.GeoDataFrame
        A GeoDataFrame with a polygon feature.
    wkt : str
        Well-known text (WKT) representation of gdf's coordinate reference
        system (CRS)

    Returns
    -------
    pd.DataFrame
        DataFrame indexed by divide_id that contains information about coverage
        for each raster cell in gridded forcing file.
    '''
    xmin = raster.x[0]
    xmax = raster.x[-1]
    ymin = raster.y[0]
    ymax = raster.y[-1]
    rastersource = NumPyRasterSource(
        raster["RAINRATE"], srs_wkt=wkt, xmin=xmin, xmax=xmax, ymin=ymin, ymax=ymax
    )
    output = exact_extract(
        rastersource,
        gdf,
        ["cell_id", "coverage"],
        include_cols=["divide_id"],
        output="pandas",
    )
    return output.set_index("divide_id")


def add_APCP_SURFACE_to_dataset(dataset: xr.Dataset) -> xr.Dataset:
    '''Convert precipitation value to correct units.'''
    # precip_rate is mm/s
    # cfe says input atmosphere_water__liquid_equivalent_precipitation_rate is mm/h
    # nom says prcpnonc input is mm/s
    # technically should be kg/m^2/s at 1kg = 1l it equates to mm/s
    # nom says qinsur output is m/s, hopefully qinsur is converted to mm/h by ngen
    dataset["APCP_surface"] = dataset["precip_rate"] * 3600
    dataset["APCP_surface"].attrs["units"] = "mm h^-1" # ^-1 notation copied from source data
    dataset["APCP_surface"].attrs["source_note"] = "This is just the precip_rate variable converted to mm/h by multiplying by 3600"
    return dataset


def get_index_chunks(data: xr.DataArray) -> list[tuple[int, int]]:
    '''  
    Take a DataArray and calculate the start and end index for each chunk based
    on the available memory.

    Parameters
    ----------
    data : xr.DataArray
        Large DataArray that can't be loaded into memory all at once.

    Returns
    -------
    list[Tuple[int, int]]
        Each element in the list represents a chunk of data. The tuple within
        the chunk indicates the start index and end index of the chunk.
    '''
    array_memory_usage = data.nbytes
    free_memory = psutil.virtual_memory().available * 0.8 # 80% of available memory
    # limit the chunk to 20gb, makes things more stable
    free_memory = min(free_memory, 20 * 1024 * 1024 * 1024)
    num_chunks = ceil(array_memory_usage / free_memory)
    max_index = data.shape[0]
    stride = max_index // num_chunks
    chunk_start = range(0, max_index, stride)
    index_chunks = [(start, start + stride) for start in chunk_start]
    return index_chunks


def create_shared_memory(lazy_array: xr.Dataset) -> Tuple[
    shared_memory.SharedMemory,
    np.dtype,
    np.dtype
]:
    '''
    Create a shared memory object so that multiple processes can access loaded 
    data.
    
    Parameters
    ----------
    lazy_array : xr.Dataset
        A chunk of gridded forcing variable data.

    Returns
    -------
    shared_memory.SharedMemory
        A specific block of memory allocated by the OS of the size of 
        lazy_array.
    np.dtype.shape
        A shape object with dimensions (# timesteps, # of raster cells) in
        reference to lazy_array.
    np.dtype
        Data type of objects in lazy_array.
    '''
    logger.debug(f"Creating shared memory size {lazy_array.nbytes/ 10**6} Mb.")
    shm = shared_memory.SharedMemory(create=True, size=lazy_array.nbytes)
    shared_array = np.ndarray(lazy_array.shape, dtype=np.float32, buffer=shm.buf)
    # if your data is not float32, xarray will do an automatic conversion here
    # which consumes a lot more memory, forcings downloaded with this tool will work
    for start, end in get_index_chunks(lazy_array):
            # copy data from lazy to shared memory one chunk at a time
            shared_array[start:end] = lazy_array[start:end]

    time, x, y = shared_array.shape
    shared_array = shared_array.reshape(time, -1)

    return shm, shared_array.shape, shared_array.dtype


def process_chunk_shared(variable: str,
                         times: np.ndarray,
                         shm_name: str,
                         shape: np.dtype.shape,
                         dtype: np.dtype, 
                         chunk: gpd.GeoDataFrame) -> xr.DataArray:
    '''  
    Process the gridded forcings chunk loaded into a SharedMemory block. 

    Parameters
    ----------
    variable : str
        Name of forcing variable to be processed.
    times : np.ndarray
        Timesteps in gridded forcings chunk.
    shm_name : str
        Unique name that identifies the SharedMemory block.
    shape : np.dtype.shape
        A shape object with dimensions (# timesteps, # of raster cells) in
        reference to the gridded forcings chunk.
    dtype : np.dtype
        Data type of objects in the gridded forcings chunk.
    chunk : gpd.GeoDataFrame
        A chunk of gridded forcings data.

    Returns
    -------
    xr.DataArray
        Averaged forcings data for each timestep for each catchment.
    '''
    existing_shm = shared_memory.SharedMemory(name=shm_name)
    raster = np.ndarray(shape, dtype=dtype, buffer=existing_shm.buf)
    results = []

    for catchment in chunk.index.unique():
        cell_ids = chunk.loc[catchment]["cell_id"]
        weights = chunk.loc[catchment]["coverage"]
        mean_at_timesteps = weighted_sum_of_cells(raster, cell_ids, weights)
        temp_da = xr.DataArray(
            mean_at_timesteps,
            dims=["time"],
            coords={"time": times},
            name=f"{variable}_{catchment}",
        )
        temp_da = temp_da.assign_coords(catchment=catchment)
        results.append(temp_da)
    existing_shm.close()
    return xr.concat(results, dim="catchment")


def get_cell_weights_parallel(gdf: gpd.GeoDataFrame,
                              input_forcings: xr.Dataset,
                              num_partitions: int) -> pd.DataFrame:
    '''
    Execute get_cell_weights with multiprocessing, with chunking for the passed
    GeoDataFrame to conserve memory usage.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        A GeoDataFrame with a polygon feature.
    input_forcings : xr.Dataset
        A gridded forcings file.
    num_partitions : int
        Number of chunks to split gdf into.

    Returns
    -------
    pd.DataFrame
        DataFrame indexed by divide_id that contains information about coverage
        for each raster cell and each timestep in gridded forcing file.
    '''
    gdf_chunks = np.array_split(gdf, num_partitions)
    wkt = gdf.crs.to_wkt()
    one_timestep = input_forcings.isel(time=0).compute()
    with multiprocessing.Pool() as pool:
        args = [(one_timestep, gdf_chunk, wkt) for gdf_chunk in gdf_chunks]
        catchments = pool.starmap(get_cell_weights, args)
    return pd.concat(catchments)

def get_units(dataset: xr.Dataset) -> dict:
    '''
    Return dictionary of units for each variable in dataset.
    
    Parameters
    ----------
    dataset : xr.Dataset
        Dataset with variables and units.
    
    Returns
    -------
    dict 
        {variable name: unit}
    '''
    units = {}
    for var in dataset.data_vars:
        if dataset[var].attrs["units"]:
            units[var] = dataset[var].attrs["units"]
    return units

def compute_zonal_stats(
    gdf: gpd.GeoDataFrame, merged_data: xr.Dataset, forcings_dir: Path
) -> None:
    '''  
    Compute zonal statistics in parallel for all timesteps over all desired 
    catchments. Create chunks of catchments and within those, chunks of 
    timesteps for memory management.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Contains identity and geometry information on desired catchments.
    merged_data : xr.Dataset
        Gridded forcing data that intersects with desired catchments.
    forcings_dir : Path
        Path to directory where outputs are to be stored.
    '''
    logger.info("Computing zonal stats in parallel for all timesteps")
    timer_start = time.time()
    num_partitions = multiprocessing.cpu_count() - 1
    if num_partitions > len(gdf):
        num_partitions = len(gdf)

    catchments = get_cell_weights_parallel(gdf, merged_data, num_partitions)

    units = get_units(merged_data)

    variables = {
                "LWDOWN": "DLWRF_surface",
                "PSFC": "PRES_surface",
                "Q2D": "SPFH_2maboveground",
                "RAINRATE": "precip_rate",
                "SWDOWN": "DSWRF_surface",
                "T2D": "TMP_2maboveground",
                "U2D": "UGRD_10maboveground",
                "V2D": "VGRD_10maboveground",
            }

    cat_chunks = np.array_split(catchments, num_partitions)

    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TextColumn("{task.completed}/{task.total}"),
        "•",
        TextColumn(" Elapsed Time:"),
        TimeElapsedColumn(),
        TextColumn(" Remaining Time:"),
        TimeRemainingColumn(),
    )

    timer = time.perf_counter()
    variable_task = progress.add_task(
        "[cyan]Processing variables...", total=len(variables), elapsed=0
    )
    progress.start()
    for variable in variables.keys():
        progress.update(variable_task, advance=1)
        progress.update(variable_task, description=f"Processing {variable}")

        if variable not in merged_data.data_vars:
            logger.warning(f"Variable {variable} not in forcings, skipping")
            continue

        # to make sure this fits in memory, we need to chunk the data
        time_chunks = get_index_chunks(merged_data[variable])
        chunk_task = progress.add_task("[purple] processing chunks", total=len(time_chunks))
        for i, times in enumerate(time_chunks):
            progress.update(chunk_task, advance=1)
            start, end = times
            # select the chunk of time we want to process
            data_chunk = merged_data[variable].isel(time=slice(start,end))
            # put it in shared memory
            shm, shape, dtype = create_shared_memory(data_chunk)
            times = data_chunk.time.values
            # create a partial function to pass to the multiprocessing pool
            partial_process_chunk = partial(process_chunk_shared,variable,times,shm.name,shape,dtype)

            logger.debug(f"Processing variable: {variable}")
            # process the chunks of catchments in parallel
            with multiprocessing.Pool(num_partitions) as pool:
                variable_data = pool.map(partial_process_chunk, cat_chunks)
            del partial_process_chunk
            # clean up the shared memory
            shm.close()
            shm.unlink()
            logger.debug(f"Processed variable: {variable}")
            concatenated_da = xr.concat(variable_data, dim="catchment")
            # delete the data to free up memory
            del variable_data
            logger.debug(f"Concatenated variable: {variable}")
            # write this to disk now to save memory
            # xarray will monitor memory usage, but it doesn't account for the shared memory used to store the raster
            # This reduces memory usage by about 60%
            concatenated_da.to_dataset(name=variable).to_netcdf(forcings_dir/ "temp" / f"{variable}_{i}.nc")
        # Merge the chunks back together
        datasets = [xr.open_dataset(forcings_dir / "temp" / f"{variable}_{i}.nc") for i in range(len(time_chunks))]
        result = xr.concat(datasets, dim="time")
        result.to_netcdf(forcings_dir / "temp" / f"{variable}.nc")
        # close the datasets
        result.close()
        _ = [dataset.close() for dataset in datasets]

        for file in forcings_dir.glob("temp/*_*.nc"):
            file.unlink()
        progress.remove_task(chunk_task)
    progress.update(
        variable_task,
        description=f"Forcings processed in {time.perf_counter() - timer:2f} seconds",
    )
    progress.stop()
    logger.info(
        f"Forcing generation complete! Zonal stats computed in {time.time() - timer_start:2f} seconds"
    )
    write_outputs(forcings_dir, variables, units)


def write_outputs(forcings_dir: Path,
                  variables: dict,
                  units: dict) -> None:
    '''  
    Write outputs to disk in the form of a NetCDF file, using dask clusters to
    facilitate parallel computing.

    Parameters
    ----------
    forcings_dir : Path
        Path to directory where outputs are to be stored.
    variables : dict
        Preset dictionary where the keys are forcing variable names and the 
        values are units.
    units : dict
        Dictionary where the keys are forcing variable names and the values are 
        units. Differs from variables, as this dictionary depends on the gridded
        forcing dataset.
    '''

    # start a dask cluster if there isn't one already running
    try:
        client = Client.current()
    except ValueError:
        cluster = LocalCluster()
        client = Client(cluster)
    temp_forcings_dir = forcings_dir / "temp"
    # Combine all variables into a single dataset using dask
    results = [xr.open_dataset(file, chunks="auto") for file in temp_forcings_dir.glob("*.nc")]
    final_ds = xr.merge(results)
    for var in final_ds.data_vars:
        if var in units:
            final_ds[var].attrs["units"] = units[var]
        else:
            logger.warning(f"Variable {var} has no units")

    rename_dict = {}
    for key, value in variables.items():
        if key in final_ds:
            rename_dict[key] = value

    final_ds = final_ds.rename_vars(rename_dict)
    final_ds = add_APCP_SURFACE_to_dataset(final_ds)

    # this step halves the storage size of the forcings
    for var in final_ds.data_vars:
        final_ds[var] = final_ds[var].astype(np.float32)

    logger.info("Saving to disk")
    # The format for the netcdf is to support a legacy format
    # which is why it's a little "unorthodox"
    # There are no coordinates, just dimensions, catchment ids are stored in a 1d data var
    # and time is stored in a 2d data var with the same time array for every catchment
    # time is stored as unix timestamps, units have to be set

    # add the catchment ids as a 1d data var
    final_ds["ids"] = final_ds["catchment"].astype(str)
    # time needs to be a 2d array of the same time array as unix timestamps for every catchment
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        time_array = (
            final_ds.time.astype("datetime64[s]").astype(np.int64).values // 10**9
        )  ## convert from ns to s
    time_array = time_array.astype(np.int32) ## convert to int32 to save space
    final_ds = final_ds.drop_vars(["catchment", "time"]) ## drop the original time and catchment vars
    final_ds = final_ds.rename_dims({"catchment": "catchment-id"}) # rename the catchment dimension
    # add the time as a 2d data var, yes this is wasting disk space.
    final_ds["Time"] = (("catchment-id", "time"), [time_array for _ in range(len(final_ds["ids"]))])
    # set the time unit
    final_ds["Time"].attrs["units"] = "s"
    final_ds["Time"].attrs["epoch_start"] = "01/01/1970 00:00:00" # not needed but suppresses the ngen warning

    final_ds.to_netcdf(forcings_dir / "forcings.nc", engine="netcdf4")
    # close the datasets
    _ = [result.close() for result in results]
    final_ds.close()

    # clean up the temp files
    for file in temp_forcings_dir.glob("*.*"):
        file.unlink()
    temp_forcings_dir.rmdir()


def setup_directories(cat_id: str) -> file_paths:
    forcing_paths = file_paths(cat_id)
    # delete everything in the forcing folder except the cached nc file
    for file in forcing_paths.forcings_dir.glob("*.*"):
        if file != forcing_paths.cached_nc_file:
            file.unlink()

    os.makedirs(forcing_paths.forcings_dir / "temp", exist_ok=True)

    return forcing_paths


def create_forcings(
    start_time: str, end_time: str, output_folder_name: str, forcing_vars: list[str] = None
) -> None:
    forcing_paths = setup_directories(output_folder_name)
    projection = xr.open_dataset(forcing_paths.template_nc, engine="h5netcdf").crs.esri_pe_string
    logger.debug("Got projection from grid file")

    gdf = gpd.read_file(forcing_paths.geopackage_path, layer="divides").to_crs(projection)
    logger.debug(f"gdf  bounds: {gdf.total_bounds}")

    if type(start_time) == datetime:
        start_time = start_time.strftime("%Y-%m-%d %H:%M")
    if type(end_time) == datetime:
        end_time = end_time.strftime("%Y-%m-%d %H:%M")

    merged_data = get_forcing_data(forcing_paths.cached_nc_file, start_time, end_time, gdf, forcing_vars)
    compute_zonal_stats(gdf, merged_data, forcing_paths.forcings_dir)


if __name__ == "__main__":
    # Example usage
    start_time = "2010-01-01 00:00"
    end_time = "2010-01-02 00:00"
    output_folder_name = "cat-1643991"
    # looks in output/cat-1643991/config for the geopackage cat-1643991_subset.gpkg
    # puts forcings in output/cat-1643991/forcings
    logger.basicConfig(level=logging.DEBUG)
    create_forcings(start_time, end_time, output_folder_name)
