/// Función para manejar la tabla de búsqueda dinamica
/// Lee los datos de un Json y los añade dinámicamente a la tabla del html
/// La función searchCountry "esconde" los renglones que no cumplen con el texto buscado
/// y deja visibles los que si coinciden.

function productsAdd() {

    d3.json("/static/js/csvjson.json", (err, tableData) => {
        tableData.forEach(function(ufoRow) {
            $("#ufo-table tbody").append(
                "<tr>" +
                `<td>${ufoRow.Country}</td>` +
                `<td>${parseFloat(ufoRow.Alcohol).toFixed(4)}</td>` +
                `<td>${parseFloat(ufoRow.bmi).toFixed(4)}</td>` +
                `<td>${parseFloat(ufoRow.CO2_emissions).toFixed(4)}</td>` +
                `<td>${parseFloat(ufoRow.Fertilizer_Application).toFixed(4)}</td>` +
                `<td>${parseFloat(ufoRow.Population).toFixed(4)}</td>` +
                `<td>${parseFloat(ufoRow.Sugar).toFixed(4)}</td>` +
                `<td>${parseFloat(ufoRow.Tobacco).toFixed(4)}</td>` +
                `<td>${parseFloat(ufoRow.Urban_Majority).toFixed(4)}</td>` +
                `<td>${parseFloat(ufoRow.Work_Hours_Per_Week).toFixed(4)}</td>` +
                "</tr>"
            );
        })
    })
}


function searchCountry() {
    // Declare variables 
    var input, filter, table, tr, td, i, txtValue;
    input = document.getElementById("datetime");
    filter = input.value.toUpperCase();
    table = document.getElementById("ufo-table");
    tr = table.getElementsByTagName("tr");

    // Loop through all table rows, and hide those who don't match the search query
    for (i = 0; i < tr.length; i++) {
        td = tr[i].getElementsByTagName("td")[0];
        if (td) {
            txtValue = td.textContent || td.innerText;
            if (txtValue.toUpperCase().indexOf(filter) > -1) {
                tr[i].style.display = "";
            } else {
                tr[i].style.display = "none";
            }
        }
    }
}


$(document).ready(function() {
    productsAdd();
});