/// Función que inserta la gráfica de burbujas usando los datos del arreglo que genera Flask
/// recibe una posición con la que hace referencia a los datos correspondientes en el arreglo

function insertBubble(posicion) {
    width = 632;
    height = 632;

    pack = data => d3.pack()
        .size([width - 2, height - 2])
        .padding(3)
        (d3.hierarchy({ children: data })
            .sum(d => d.coefficient));

    const root = pack(b_data[posicion]);

    format = d3.format(",d");

    color = d3.scaleOrdinal(b_data[posicion].map(d => b_data[posicion].name), d3.schemeCategory10);

    const svg = d3.select("#bubble-country-chart")
        .append("svg")
        .attr("viewBox", [0, 0, width, height])
        .attr("font-size", 15)
        .attr("font-family", "sans-serif")
        .attr("text-anchor", "middle");

    const leaf = svg.selectAll("g")
        .data(root.leaves())
        .join("g")
        .attr("transform", d => `translate(${d.x + 1},${d.y + 1})`);

    leaf.append("circle")
        .attr("id", d => (d.leafUid = d.name))
        .attr("r", d => d.r)
        .attr("fill-opacity", 0.7)
        .attr("fill", d => color(d.name));

    leaf.append("text")
        .attr("clip-path", d => d.clipUid)
        .selectAll("tspan")
        .data(d => d.data.name.split(/(?=[A-Z][^A-Z])/g))
        .join("tspan")
        .attr("x", 0)
        .attr("y", (d, i, nodes) => `${i - nodes.length / 2 + 0.8}em`)
        .text(d => d);

    leaf.append("title")
        .text(d => `${d.data.name}\n${d.data.coefficient}`);
}