//////////////////////////////////////////////////////////////////////////////////
///
///  Archivo Javascript que inserta el mapa y la gráfica de burbujas.
///  El script crea el mapa y la gráfica usando los datos que pasa Flask como objetos
///  al html de la página principal. Con esos arreglos se obtienen los valores que
///  llenan las gráficas.
///
/// La creación de los objetos es mediante funciones para poder hacer que cambien los
/// valores cuando se cambie la selección del objeto <select id="selDataset"> 
///
//////////////////////////////////////////////////////////////////////////////////


// Crea el objeto del mapa en el <div id="world-map-toggle"> que corresponde en index.html
var myMap = L.map("world-map-toggle", {
    center: [15.5994, -28.6731],
    zoom: 1
});

// Añade el layer principal
L.tileLayer("https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}", {
    attribution: "Map data &copy; <a href=\"https://www.openstreetmap.org/\">OpenStreetMap</a> contributors, <a href=\"https://creativecommons.org/licenses/by-sa/2.0/\">CC-BY-SA</a>, Imagery © <a href=\"https://www.mapbox.com/\">Mapbox</a>",
    maxZoom: 1,
    minZoom: 1,
    id: "mapbox.streets-basic",
    accessToken: API_KEY
}).addTo(myMap);

// Función que cambia el formato de número para convertirlo en moneda
function formatNumber(num) {
    return num.toString().replace(/(\d)(?=(\d{3})+(?!\d))/g, '$1,')
}

// Añade los círculos al mapa con la información del arreglo que viene de Flask
let countries_list = g_data.map(d => {
    return L.circle([d.lat, d.lon], {
        color: "yellow",
        fillColor: "yellow",
        fillOpacity: 0.75,
        radius: 200000
    });
});
let paises_todos = L.layerGroup(countries_list);
myMap.addLayer(paises_todos);

// Función para manejar el evento de cambio de opción en el objeto <select id="selDataset"> 
// Esta marca en un color diferente al país seleccionado en el mapa y carga los datos de ese país
// en la gráfica de burbujas.
function optionChanged(country) {
    let countries_list = g_data.map(d => {
        if (d.name == country) {
            fillColorSelect = "blue"
            radius = 400000
            posicion = d.contador
        } else {
            fillColorSelect = "yellow"
            radius = 200000
        };

        return L.circle([d.lat, d.lon], {
            color: "yellow",
            fillColor: fillColorSelect,
            fillOpacity: 0.75,
            radius: radius
        });
    });
    myMap.removeLayer(paises_todos);
    paises_todos = L.layerGroup(countries_list);
    myMap.addLayer(paises_todos);
    if (kill == 1) {
        d3.select("svg").remove();
    } else {
        kill = 1;
    }
    insertBubble(posicion);
    buildMetadata(posicion);
};

// Función de inicio, crea los objetos necesarios y los llena con el valor del primer país
// del arreglo que recibe de flask en la página de index.html
function init() {
    var selector = d3.select("#selDataset");
    g_data.forEach(d => {
        selector
            .append("option")
            .text(d.name)
            .property("value", d.name);
    });
    optionChanged("Andorra");
};

// Variable para evitar que la función de cambio de opción "mate" a los objetos antes de crearlos
// Esto porque al cambiar los datos de la gráfica de burbujas se requiere quitar el objeto primero
// y luego crear uno nuevo para la opción seleccionada
kill = 0;

function buildMetadata(posicion) {
    let dataPanel = d3.select("#sample-features");
    dataPanel.html("");
    b_data[posicion].forEach(sample_metadata => {
        const object = sample_metadata;
        for (let [key, value] of Object.entries(object)) {
            if (key != "country") {
                dataPanel
                    .append("div")
                    .text(`${value}`)
            };
        };
    });
};

// Llamada a la función de inicio
init();