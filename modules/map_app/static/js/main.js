var cat_id_dict = {};
var selected_cat_layer = null;
var upstream_maps = {};
var flowline_layers = {};

var registered_layers = {}

async function update_selected() {
    console.log('updating selected');
    if (!(Object.keys(cat_id_dict).length === 0)) {
        return fetch('/get_geojson_from_catids', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(cat_id_dict),
        })
            .then(response => response.json())
            .then(data => {
                // if the cat_id is already in the dict, remove the key
                // remove the old layer
                if (selected_cat_layer) {
                    map.removeLayer(selected_cat_layer);
                }
                console.log(data);
                // add the new layer
                selected_cat_layer = L.geoJSON(data).addTo(map);
                selected_cat_layer.eachLayer(function (layer) {
                    layer._path.classList.add('selected-cat-layer');
                });
            })
            .catch(error => {
                console.error('Error:', error);
            });
    } else {
        if (selected_cat_layer) {
            map.removeLayer(selected_cat_layer);
        }
        return Promise.resolve();
    }
}

async function populate_upstream() {
    console.log('populating upstream selected');
    // drop any key that is not in the cat_id_dict
    for (const [key, value] of Object.entries(upstream_maps)) {
        if (!(key in cat_id_dict)) {
            map.removeLayer(value);
            delete upstream_maps[key];
        }
    }
    // add any key that is in the cat_id_dict but not in the upstream_maps
    for (const [key, value] of Object.entries(cat_id_dict)) {
        if (!(key in upstream_maps)) {
            upstream_maps[key] = null;
        }
    }
    if (Object.keys(upstream_maps).length === 0) {
        return Promise.resolve();
    }

    const fetchPromises = Object.entries(upstream_maps).map(([key, value]) => {
        if (value === null) {
            return fetch('/get_upstream_geojson_from_catids', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(key),
            })
                .then(response => response.json())
                .then(data => {
                    // if the cat_id is already in the dict, remove the key
                    // remove the old layer
                    if (upstream_maps[key]) {
                        map.removeLayer(upstream_maps[key]);
                    }
                    console.log(data);
                    // add the new layer if the downstream cat's still selected
                    if (key in cat_id_dict) {
                        layer_group = L.geoJSON(data).addTo(map);
                        upstream_maps[key] = layer_group;
                        layer_group.eachLayer(function (layer) {
                            if (layer._path) {
                                layer._path.classList.add('upstream-cat-layer');
                            }
                        });
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                });
        }
    });
    return fetchPromises;

}

async function populate_flowlines() {
    console.log('populating flowlines');
    // drop any key that is not in the cat_id_dict
    for (const [key, value] of Object.entries(flowline_layers)) {
        if (!(key in cat_id_dict)) {
            for (i of flowline_layers[key]) {
                map.removeLayer(i);
                delete flowline_layers[key];
            }
        }
    }
    // add any key that is in the cat_id_dict but not in the flowline_layers
    for (const [key, value] of Object.entries(cat_id_dict)) {
        if (!(key in flowline_layers)) {
            flowline_layers[key] = null;
        }
    }
    if (Object.keys(flowline_layers).length === 0) {
        return Promise.resolve();
    }

    const fetchPromises = Object.entries(flowline_layers).map(([key, value]) => {
        if (value === null) {
            return fetch('/get_flowlines_from_catids', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(key),
            })
                .then(response => response.json())
                .then(data => {
                    // if the cat_id is already in the dict, remove the key
                    // remove the old layer
                    if (flowline_layers[key]) {
                        for (i of flowline_layers[key]) {
                            map.removeLayer(i);
                        }
                    }
                    // loud!
                    // console.log(data);
                    to_cat = JSON.parse(data['to_cat']);
                    to_nexus = JSON.parse(data['to_nexus']);
                    nexus = JSON.parse(data['nexus']);
                    // add the new layer if the downstream cat's still selected
                    if (key in cat_id_dict) {
                        to_cat_layer = L.geoJSON(to_cat).addTo(map);
                        to_nexus_layer = L.geoJSON(to_nexus).addTo(map);
                        nexus_layer = L.geoJSON(nexus).addTo(map);
                        // hack to add css classes to the flowline layers
                        // using eachLayer as it waits for layer to be done updating
                        // directly accessing the _layers keys may not always work
                        to_cat_layer.eachLayer(function (layer) {
                            if (layer._path) {
                                layer._path.classList.add('flowline-to-cat-layer');
                            }
                        });
                        to_nexus_layer.eachLayer(function (layer) {
                            if (layer._path) {
                                layer._path.classList.add('flowline-to-nexus-layer');
                            }
                        });
                    }
                    flowline_layers[key] = [to_cat_layer, to_nexus_layer, nexus_layer];
                })

                .catch(error => {
                    console.error('Error:', error);
                });
        }
    });
    return fetchPromises;

}

async function synchronizeUpdates() {
    console.log('Starting updates');

    // wait for all promises
    const upstreamPromises = await populate_upstream();
    const flowlinePromises = await populate_flowlines();
    const selectedPromise = await update_selected();
    await Promise.all([selectedPromise, ...upstreamPromises, ...flowlinePromises]).then(() => {
        // This block executes after all promises from populate_upstream and populate_flowlines have resolved
        console.log('All updates are complete');
        // BringToFront operations or any other operations to perform after updates
        if (selected_cat_layer) {
            selected_cat_layer.bringToFront();
        }
        for (const [key, value] of Object.entries(flowline_layers)) {
            if (key in cat_id_dict) {
                value[0].bringToFront();
                value[1].bringToFront();
            }
        }
    }).catch(error => {
        console.error('An error occurred:', error);
    });
}

function onMapClick(event) {
    // Extract the clicked coordinates
    var lat = event.latlng.lat;
    var lng = event.latlng.lng;

    // Send an AJAX request to the Flask backend
    fetch('/handle_map_interaction', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            coordinates: { lat: lat, lng: lng }
        }),
    })
        .then(response => response.json())
        .then(data => {

            // if the cat_id is already in the dict, remove the key
            if (data['cat_id'] in cat_id_dict) {
                delete cat_id_dict[data['cat_id']];
            }
            else {
                // temporary fix to only allow one basin to be selected
                cat_id_dict = {};
                // uncomment above line to allow multiple basins to be selected
                cat_id_dict[data['cat_id']] = [lat, lng];
            }
            console.log('clicked on cat_id: ' + data['cat_id'] + ' coords :' + lat + ', ' + lng);


            synchronizeUpdates();
            //$('#selected-basins').text(Object.keys(cat_id_dict).join(', '));
            // revert this line too
            $('#selected-basins').text(Object.keys(cat_id_dict));

        })
        .catch(error => {
            console.error('Error:', error);
        });

}

function select_by_lat_lon() {
    lat = $('#lat_input').val();
    lon = $('#lon_input').val();
    fetch('/handle_map_interaction', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            coordinates: { lat: lat, lng: lon }
        }),
    })
        .then(response => response.json())
        .then(data => {
            cat_id = data['cat_id'];
            cat_id_dict[cat_id] = [lat, lon];
            synchronizeUpdates();
            $('#selected-basins').text(Object.keys(cat_id_dict));
        })
        .catch(error => {
            console.error('Error:', error);
        });
}

$('#select-lat-lon-button').click(select_by_lat_lon);

function select_by_id() {
    cat_id_dict = {};
    cat_ids = $('#cat_id_input').val();

    cat_ids = cat_ids.split(',');
    for (cat_id of cat_ids) {
        cat_id_dict[cat_id] = [0, 0];
    }
    synchronizeUpdates();
    $('#selected-basins').text(Object.keys(cat_id_dict));
}

$('#select-button').click(select_by_id);

function clear_selection() {
    cat_id_dict = {};
    synchronizeUpdates();
    $('#selected-basins').text(Object.keys(cat_id_dict));
}

$('#clear-button').click(clear_selection);

function get_catid_from_gage_id(gage_id) {
    return fetch('/get_catid_from_gage_id', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            gage_id: gage_id
        }),
    })
        .then(response => response.json())
        .then(data => {
            return data['cat_ids'];
        })
        .catch(error => {
            console.error('Error:', error);
        });
}

function select_by_gage_id() {
    gage_ids = $('#gage_id_input').val();
    gage_ids = gage_ids.split(',');
    for (gage_id of gage_ids) {
        cat_ids = get_catid_from_gage_id(gage_id);
        cat_ids.then(function (result) {
            for (result of result) {
                cat_id_dict[result] = [0, 0];
            }
            $('#selected-basins').text(Object.keys(cat_id_dict));
        });
    }
    synchronizeUpdates();
}

$('#select-gage-button').click(select_by_gage_id);

// Initialize the map
var map = L.map('map', { crs: L.CRS.EPSG3857 }).setView([40, -96], 5);

//Create in-map Legend / Control Panel
var legend = L.control({ position: 'bottomright' });
// load in html template for the legend
legend.onAdd = function (map) {
    legend_div = L.DomUtil.create('div', 'custom_legend');
    return legend_div
};
legend.addTo(map);

southWest = L.latLng(22.5470, -125);
northEast = L.latLng(53, -65);
bounds = L.latLngBounds(southWest, northEast);

// Add OpenStreetMap tiles to the map
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18,
    attribution: '© OpenStreetMap contributors',
    crs: L.CRS.EPSG3857

}).addTo(map);

L.tileLayer('static/tiles/tms/{z}/{x}/{y}.png', {
    minZoom: 8,
    maxZoom: 18,
    maxNativeZoom: 11,
    attribution: '© Johnson, J. M. (2022). National Hydrologic Geospatial Fabric (hydrofabric) for the Next Generation (NextGen) Hydrologic Modeling Framework',
    crs: L.CRS.EPSG3857,
    reuseTiles: true,
    bounds: bounds
}).addTo(map);

L.tileLayer('static/tiles/vpu/{z}/{x}/{y}.png', {
    minZoom: 0,
    maxZoom: 11,
    maxNativeZoom: 9,
    crs: L.CRS.EPSG3857,
    reuseTiles: true,
    bounds: bounds
}).addTo(map);


map.on('click', onMapClick);

