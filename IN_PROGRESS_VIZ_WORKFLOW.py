
import logging
import logging.config

import pdgstaging  # For staging
import pdgraster
import viz_3dtiles #import Cesium3DTile, Cesium3DTileset

import os
import ray
# from ray import workflow
import time
import sys
import traceback
import socket # for seeing where things run distributed
from collections import Counter
import pathlib

# Change me 😁  
# ALWAYS include the tailing slash "/"
BASE_DIR_OF_INPUT = '/home/kastanday/maple_data/'   # The output data of MAPLE. Which is the input data for STAGING.
OUTPUT            = '/home/kastanday/output/'       # Dir for results. High I/O is good.
OUTPUT_OF_STAGING = OUTPUT + 'staged/'              # Output dirs for each sub-step
GEOTIFF_PATH      = OUTPUT + 'geotiff/'
WEBTILE_PATH      = OUTPUT + 'web_tiles/'
THREE_D_PATH      = OUTPUT + '3d_tiles/'

# Convenience for little test runs. Change me 😁  
ONLY_SMALL_TEST_RUN = True                          # For testing, this ensures only a small handful of files are processed.
TEST_RUN_SIZE       = 3                              # Number of files to pre processed during testing (only effects testing)

# ⛔️ don't change these ⛔️
IWP_CONFIG = {'dir_input': BASE_DIR_OF_INPUT, 'ext_input': '.shp', 'dir_geotiff': GEOTIFF_PATH, 'dir_web_tiles': WEBTILE_PATH, 'dir_staged': OUTPUT_OF_STAGING, 'filename_staging_summary': OUTPUT_OF_STAGING + 'staging_summary.csv', 'filename_rasterization_events': GEOTIFF_PATH + 'raster_events.csv', 'filename_rasters_summary': GEOTIFF_PATH + 'raster_summary.csv', 'simplify_tolerance': 1e-05, 'tms_id': 'WorldCRS84Quad', 'tile_path_structure': ['style', 'tms', 'z', 'x', 'y'], 'z_range': [0, 13], 'tile_size': [256, 256], 'statistics': [{'name': 'iwp_count', 'weight_by': 'count', 'property': 'centroids_per_pixel', 'aggregation_method': 'sum', 'resampling_method': 'sum', 'val_range': [0, None], 'palette': ['rgb(102 51 153 / 0.1)', '#d93fce', 'lch(85% 100 85)']}, {'name': 'iwp_coverage', 'weight_by': 'area', 'property': 'area_per_pixel_area', 'aggregation_method': 'sum', 'resampling_method': 'average', 'val_range': [0, 1], 'palette': ['rgb(102 51 153 / 0.1)', 'lch(85% 100 85)']}], 'deduplicate_at': ['raster'], 'deduplicate_method': 'neighbor', 'deduplicate_keep_rules': [['Date', 'larger'], ['Time', 'larger']], 'deduplicate_overlap_tolerance': 0, 'deduplicate_overlap_both': False, 'deduplicate_centroid_tolerance': None}

FAILURES = []
FAILURE_PATHS = []
IP_ADDRESSES_OF_WORK = []

# TODO: return path names instead of 0. 
# TODO: use logging instead of prints. 
# TODO: Global option for small test run.

def main():
    ray.shutdown()
    assert ray.is_initialized() == False
    ray.init(address="141.142.204.7:6379", dashboard_port=8265)   # most reliable way to start Ray
    # ray.init(address='auto')                                    # multinode, but less reliable than above.
    # ray.init()                                                  # single-node only!
    assert ray.is_initialized() == True
    
    # todo add Ray Workflows
    # workflow.init(storage="/home/kastanday/~/ray_workflow_storage")

    # instantiate classes for their helper functions
    rasterizer = pdgraster.RasterTiler(IWP_CONFIG)
    stager = pdgstaging.TileStager(IWP_CONFIG)
    # tile_manager = stager.tiles
    # config_manager = stager.config
    start = time.time()

    
    # (optionally) CHANGE ME, if you only want certain steps 😁
    try:
        ########## MAIN STEPS ##########

        step0_result = step0_staging(stager)           # Staging 
        print(step0_result)
        step1_3d_tiles()                             # Create 3D tiles from .shp
        # step2_result = step2(stager)                 # rasterize highest Z level only 
        # print(step2_result)                          
        # step3(stager, batch_size_geotiffs=100)       # rasterize all LOWER Z levels
        # step4(tile_manager, rasterizer, batch_size_web_tiles=100) # convert to web tiles.

        """
        # NOT WORKING YET (6/1/22)
        ########## RAY WORKFLOWS VERSION (same as above) ##########
        step1_result = step0_staging.step(tile_manager)
        setp2_result = step2.step(stager) # rasterize highest Z level
        step3(tile_manager, config_manager, batch_size_geotiffs=100) # rasterize all other Z levels
        step4(tile_manager, rasterizer, batch_size_web_tiles=100) # convert to web tiles.

        RUN WORKFLOW
        workflow_id = make_workflow_id('first_test_step_2') # add arbitrary human-readable name
        setp2_result.run(workflow_id)
        print("Workflow status is: " + workflow.get_status(workflow_id=workflow_id))
        """

    except Exception as e:
        print(f"Caught error in main: {str(e)}", "\nTraceback", traceback.print_exc())
    finally:
        # cancel work. shutdown ray.
        print(f"Runtime: {(time.time() - start)/60:.2f} minutes")
        ray.shutdown()
        assert ray.is_initialized() == False

############### MAIN STEPS ###############

# @workflow.step(name="Step0_Stage_All")
def step0_staging(stager):
    start = time.time()

    ids = []
    stager.tile_manager.add_base_dir('base_input', BASE_DIR_OF_INPUT, '.shp')
    staging_input_files_list = stager.tile_manager.get_filenames_from_dir(base_dir = 'base_input')

    if ONLY_SMALL_TEST_RUN:
        staging_input_files_list = staging_input_files_list[:TEST_RUN_SIZE]
    
    print("number of items", len(staging_input_files_list))

    # catch kill signal to shutdown on command (ctrl + c)
    try: 
        # START REMOTE FUNCTIONS
        for filepath in staging_input_files_list: # [40:45] -- FOR SHORTER RUNS
            print(f"Starting remote function for {filepath}")
            IWP_CONFIG['dir_input'] = filepath
            ids.append(stage_remote.remote(IWP_CONFIG))

        # print at start of staging
        print(f"\n\n👉 Staging {len(staging_input_files_list)} files in parallel 👈\n\n")
        print(f'''This cluster consists of
            {len(ray.nodes())} nodes in total
            {ray.cluster_resources()['CPU']} CPU cores in total
            {ray.cluster_resources()['memory']/1e9:.2f} GB CPU memory in total
            {ray.cluster_resources()['GPU']} GRAPHICCSSZZ cards in total
        ''')

        # BLOCKING - WAIT FOR REMOTE FUNCTIONS TO FINISH
        for i in range(0, len(ids)): 
            # ray.wait(ids) catches ONE at a time. 
            ready, not_ready = ray.wait(ids)

            # check for failures
            if any(err in ray.get(ready)[0] for err in ["FAILED", "Failed", "❌"]):
                FAILURES.append(ray.get(ready)[0])
                print(f"❌ Failed {ray.get(ready)}")
                IP_ADDRESSES_OF_WORK.append(ray.get(ready)[0][1])
            else:
                # success case
                print(f"✅ Finished {ray.get(ready)[0][0]}")
                IP_ADDRESSES_OF_WORK.append(ray.get(ready)[0][1])


            # print FINAL stats about Staging 
            print(f"📌 Completed {i+1} of {len(staging_input_files_list)}")
            print(f"⏰ Running total of elapsed time: {(time.time() - start)/60:.2f} minutes\n")
            ids = not_ready
            if not ids:
                break
    except Exception as e:
        print(f"Cauth error in Head-node loop: {str(e)}")
    finally:
        print(FAILURES)
        print(FAILURE_PATHS)
        print(f"Number of failures = {len(FAILURES)}")
        print("☝️ ☝️ ☝️ I'm not properly catching all the failures")
        print(f"Runtime: {(time.time() - start)/60:.2f} minutes\n")
        print(f"\n\n👉 Staging {len(staging_input_files_list)} files in parallel 👈\n\n")

        print("Which nodes were used?")
        for ip_address, num_tasks in Counter(IP_ADDRESSES_OF_WORK).items():
            print('    {} tasks on {}'.format(num_tasks, ip_address))
        
        return "😁 step 1 success"

# @workflow.step(name="Step1_3D_Tiles")
def step1_3d_tiles(stager):
    IP_ADDRESSES_OF_WORK_3D_TILES = []
    FAILURES_3D_TILES = []

    try:
        # collect staged files
        stager.tiles.add_base_dir('staging', OUTPUT_OF_STAGING, '.gpkg')
        staged_files_list = stager.tiles.get_filenames_from_dir(base_dir = 'staging')

        if ONLY_SMALL_TEST_RUN:
            staged_files_list = staged_files_list[:TEST_RUN_SIZE]
        print("total staged files len:", len(staged_files_list))

        # START JOBS
        start = time.time()
        ids = []
        for filepath in staged_files_list:
            filename, save_to_dir = build_filepath(filepath)
            # print(filepath)
            # print(save_to_dir, filename, "\n") # todo remove print
            ids.append(three_d_tile.remote(filepath, filename, save_to_dir))

        print("RUNNING {} JOBS".format(len(ids)))

        # get 3D tiles, send to Tileset
        for i in range(0,len(staged_files_list)): 
            ready, not_ready = ray.wait(ids)
            
            # Check for failures
            if any(err in ray.get(ready)[0] for err in ["FAILED", "Failed", "❌"]):
                # failure case
                FAILURES_3D_TILES.append( [ray.get(ready)[0][0], ray.get(ready)[0][1]] )
                print(f"❌ Failed {ray.get(ready)}")
            else:
                # success case
                print(f"✅ Finished {ray.get(ready)[0][0]}")
                print(f"📌 Completed {i+1} of {len(staged_files_list)}, {(i+1)/len(staged_files_list)*100}%, ⏰ Elapsed time: {(time.time() - start)/60:.2f} min\n")
                IP_ADDRESSES_OF_WORK_3D_TILES.append(ray.get(ready)[0][1])

            ids = not_ready
            if not ids:
                break
            
    except Exception as e:
        print("❌❌  Failed in main Ray loop (of Viz-3D). Error:", e)
    finally:
        print(f"⏰ Running total of elapsed time: {(time.time() - start)/60:.2f} minutes\n")
        print("Which nodes were used?")
        for ip_address, num_tasks in Counter(IP_ADDRESSES_OF_WORK_3D_TILES).items():
            print('    {} tasks on {}'.format(num_tasks, ip_address))
        print(f"There were {len(FAILURES_3D_TILES)} failures.")
        # todo -- group failures by node IP address...
        for failure_text, ip_address in FAILURES_3D_TILES:
            print(f"    {failure_text} on {ip_address}")

# @workflow.step(name="Step2_Rasterize_only_higest_z_level")
def step2(stager, batch_size=10):
    """
    This is a BLOCKING step (but it can run in parallel).
    Rasterize all staged tiles (only highest z-level).
    """
    # Get paths to all the newly staged tiles
    stager.tile_manager.add_base_dir('output_of_staging', OUTPUT_OF_STAGING, '.gpkg')
    staged_paths = stager.tiles.get_filenames_from_dir( base_dir = 'output_of_staging' )

    if ONLY_SMALL_TEST_RUN:
        staged_paths = staged_paths[:TEST_RUN_SIZE]

    staged_batches = make_batch(staged_paths, batch_size)

    print(OUTPUT_OF_STAGING)

    print("2️⃣  Step 2 Rasterize only highest Z")
    print(f"🌄 Rasterize {len(staged_paths)} gpkgs")
    print(f"🏎  Parallel jobs: {len(staged_batches)} \n")

    start = time.time()

    # catch kill signal to shutdown on command (ctrl + c)
    try: 
        # Start remote functions
        app_futures = []
        for batch in staged_batches:
            app_future = rasterize.remote(batch, IWP_CONFIG)
            app_futures.append(app_future)

        for i in range(0, len(app_futures)): 
            ready, not_ready = ray.wait(app_futures)
            # print(f"✅ Finished {ray.get(ready)}")
            print(f"📌 Completed {i+1} of {len(staged_batches)}")
            print(f"⏰ Running total of elapsed time: {(time.time() - start)/60:.2f} minutes\n")
            app_futures = not_ready
            if not app_futures:
                break
    except Exception as e:
        print(f"Cauth error in Ray loop: {str(e)}")
    finally:
        # shutdown ray 
        end = time.time()
        print(f"Runtime: {(end - start):.2f} seconds")
        ray.shutdown()
        assert ray.is_initialized() == False

    print(f'⏰ Total time to rasterize {len(staged_paths)} tiles: {(time.time() - start)/60:.2f} minutes\n')
    return "Done rasterize all only highest z level"

# @workflow.step(name="Step3_Rasterize_lower_z_levels")
def step3(stager, batch_size_geotiffs=200):
    '''
    STEP 3: Create parent geotiffs for all z-levels (except highest)
    THIS IS HARD TO PARALLELIZE multiple zoom levels at once..... sad, BUT
    👉 WE DO PARALLELIZE BATCHES WITHIN one zoom level.
    '''
    # find all Z levels
    min_z = stager.config_manager.get_min_z()
    max_z = stager.config_manager.get_max_z()
    parent_zs = range(max_z - 1, min_z - 1, -1)
    print("3️⃣ Step 3: Create parent geotiffs for all z-levels (except highest)")
    

    start = time.time()

    # Can't start lower z-level until higher z-level is complete.
    for z in parent_zs:
        print(f"👉 Starting Z level {z} of {len(parent_zs)}")
        # Loop thru Z levels 
        # Make lower z-levels based on the path names of the files just created
        stager.tile_manager.add_base_dir('geotiff_path', GEOTIFF_PATH, '.tif')
        child_paths = stager.tiles.get_filenames_from_dir( base_dir = 'geotiff_path', z=z + 1)

        if ONLY_SMALL_TEST_RUN:
            child_paths = child_paths[:TEST_RUN_SIZE]

        parent_tiles = set()
        for child_path in child_paths:
            parent_tile = stager.tiles.get_parent_tile(child_path)
            parent_tiles.add(parent_tile)
        parent_tiles = list(parent_tiles)

        # Break all parent tiles at level z into batches
        parent_tile_batches = make_batch(parent_tiles, batch_size_geotiffs)
        print(f"📦 Staging {len(child_paths)} tifs")
        print(f"📦 Staging {len(parent_tile_batches)} parents")
        print(f"📦 Staging {parent_tile_batches} parents")

        # PARALLELIZE batches WITHIN one zoom level (best we can do for now).
        try:
            app_futures = []
            for parent_tile_batch in parent_tile_batches:
                app_future = create_composite_geotiffs.remote(
                    parent_tile_batch, IWP_CONFIG, logging_dict=None)
                app_futures.append(app_future)

            print(f"💥💥💥💥 NUMBER OF FUTURES {len(app_futures)} futures (equal to num batches)")
            # Don't start the next z-level (or move to step 4) until the
            # current z-level is complete
            for i in range(0, len(app_futures)): 
                ready, not_ready = ray.wait(app_futures)
                print(f"✅ Finished {ray.get(ready)}")
                print(f"📌 Completed {i+1} of {len(parent_tile_batches)}")
                print(f"⏰ Running total of elapsed time: {(time.time() - start)/60:.2f} minutes\n")
                app_futures = not_ready
                if not app_futures:
                    break
        except Exception as e:
            print(f"Cauth error in Ray loop in step 3: {str(e)}")

    print(f"⏰ Total time to create parent geotiffs: {(time.time() - start)/60:.2f} minutes\n")
    return "Done step 3."

# @workflow.step(name="Step4_create_webtiles")
def step4(tile_manager, rasterizer, batch_size_web_tiles=100):
    '''
    STEP 4: Create web tiles from geotiffs
    Infinite parallelism.
    '''
    start = time.time()
    # Update color ranges
    rasterizer.update_ranges()

    tile_manager.add_base_dir('geotiff_path', GEOTIFF_PATH, '.tif')
    geotiff_paths = tile_manager.get_filenames_from_dir(base_dir = 'geotiff_path')

    if ONLY_SMALL_TEST_RUN:
        geotiff_paths = geotiff_paths[:TEST_RUN_SIZE]
    
    geotiff_batches = make_batch(geotiff_paths, batch_size_web_tiles)
    print(f"📦 Creating web tiles from geotiffs. Num parallel batches = {len(geotiff_batches)}")
    
    # catch kill signal to shutdown on command (ctrl + c)
    try: 
        # Start remote functions
        app_futures = []
        for batch in geotiff_batches:
            app_future = create_web_tiles.remote(batch, IWP_CONFIG)
            app_futures.append(app_future)

        for i in range(0, len(app_futures)): 
            ready, not_ready = ray.wait(app_futures)
            print(f"✅ Finished {ray.get(ready)}")
            print(f"📌 Completed {i+1} of {len(geotiff_batches)}")
            print(f"⏰ Running total of elapsed time: {(time.time() - start)/60:.2f} minutes\n")
            app_futures = not_ready
            if not app_futures:
                break
    except Exception as e:
        print(f"Cauth error in Ray loop: {str(e)}")
    finally:
        # cancel work. shutdown ray 
        end = time.time()
        print(f"Runtime: {(end - start):.2f} seconds")
        ray.shutdown()
        assert ray.is_initialized() == False

    # Don't record end time until all web tiles have been created
    [a.result() for a in app_futures]

    print(f'⏰ Total time to create web tiles:  {len(geotiff_paths)} tiles: {(time.time() - start)/60:.2f} minutes\n')

############### HELPER FUNCTIONS BELOW HERE ###############

@ray.remote
def rasterize(staged_paths, config, logging_dict=None):
    """
    Rasterize a batch of vector files (step 2)
    """
    config['dir_input'] = OUTPUT_OF_STAGING
    import pdgraster
    if logging_dict:
        import logging.config
        logging.config.dictConfig(logging_dict)

    try:
        rasterizer = pdgraster.RasterTiler(config)
        rasterizer.rasterize_vectors(staged_paths, make_parents=False)
    except Exception as e:
        print("⚠️ Failed to rasterize path: ", staged_paths, "\nWith error: ", e, "\nTraceback", traceback.print_exc())
        
    return staged_paths

@ray.remote
def create_composite_geotiffs(tiles, config, logging_dict=None):
    """
    Make composite geotiffs (step 3)
    """
    import pdgraster
    if logging_dict:
        import logging.config
        logging.config.dictConfig(logging_dict)
    try:
        rasterizer = pdgraster.RasterTiler(config)
        rasterizer.parent_geotiffs_from_children(tiles, recursive=False)
    except Exception as e:
        print("⚠️ Failed to create rasterizer. With error ", e)
    return 0

@ray.remote
def create_web_tiles(geotiff_paths, config, logging_dict=None):
    """
    Create a batch of webtiles from geotiffs (step 4)
    """
    import pdgraster
    if logging_dict:
        import logging.config
        logging.config.dictConfig(logging_dict)
    try:
        rasterizer = pdgraster.RasterTiler(config)
        rasterizer.webtiles_from_geotiffs(geotiff_paths, update_ranges=False)
    except Exception as e:
        print("⚠️ Failed to create webtiles. With error ", e)
    return 0

def make_batch(items, batch_size):
    """
    Simple helper.
    Create batches of a given size from a list of items.
    """
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

@ray.remote
def stage_remote(config_path):
    """
    Step 1. 
    Parallelism at the per-shape-file level.
    """
    try: 
        stager = pdgstaging.TileStager(config_path)
        # stager.stage_all()
        stager.stage(config_path['dir_input'])
    except Exception as e:
        # traceback.print_exc()
        return [f"‼️ ‼️ ❌ ❌ ❌ ❌ -- THIS TASK FAILED ❌ ❌ ❌ ❌ ‼️ ‼️ with path: {config_path['dir_input']} and \nError: {e}\nTraceback {traceback.print_exc()}", 
            socket.gethostbyname(socket.gethostname())]
    return [f"Done stage_remote w/ config_path: {config_path['dir_input']}", 
        socket.gethostbyname(socket.gethostname())]

# 🎯 Best practice to ensure unique Workflow names.
def make_workflow_id(name: str) -> str:
  import pytz
  from datetime import datetime

  # Timezones: US/{Pacific, Mountain, Central, Eastern}
  # All timezones `pytz.all_timezones`. Always use caution with timezones.
  curr_time = datetime.now(pytz.timezone('US/Central'))
  return f"{name}-{str(curr_time.strftime('%h_%d,%Y@%H:%M'))}"

def build_filepath(input_file):
    # Demo input: /home/kastanday/output/staged/WorldCRS84Quad/13/1047/1047.gpkg

    # Replace 'staged' -> '3d_tiles' in path
    # In:       /home/kastanday/output/staged/WorldCRS84Quad/13/1047/1047.gpkg
    # new_path: /home/kastanday/output/3d_tiles/WorldCRS84Quad/13/1047/1047.gpkg
    path = pathlib.Path(input_file)
    index = path.parts.index('staged')
    pre_path = os.path.join(*path.parts[:index])
    post_path = os.path.join(*path.parts[index+1:])
    new_path = os.path.join(pre_path, '3d_tiles', post_path)
    new_path = pathlib.Path(new_path)

    # filename: 1047
    filename = new_path.stem

    # save_to_dir: /home/kastanday/output/staged/WorldCRS84Quad/13/1047
    save_to_dir = new_path.parent

    return str(filename), str(save_to_dir)

@ray.remote
def three_d_tile(input_file, filename, save_to):
    try:
        # the 3DTile saves the .b3dm file. 
        tile = viz_3dtiles.Cesium3DTile()
        tile.set_save_to_path(save_to)
        tile.set_b3dm_name(filename)        
        # build 3d tile.
        tile.from_file(filepath=input_file)

        # Tileset saves the json summary.
        tileset = viz_3dtiles.Cesium3DTileset(tile)
        tileset.set_save_to_path(save_to)
        tileset.set_json_filename(filename)
        tileset.write_file()
        
    except Exception as e:
        return [f"‼️ ‼️ ❌ ❌ ❌ ❌ -- THIS TASK FAILED ❌ ❌ ❌ ❌ ‼️ ‼️ with path: {save_to, filename} and \nError: {e}\nTraceback: {traceback.print_exc()}", 
    socket.gethostbyname(socket.gethostname())]
    
    # success case
    return [f"Path {save_to}", socket.gethostbyname(socket.gethostname())]


if __name__ == '__main__':
    main()

