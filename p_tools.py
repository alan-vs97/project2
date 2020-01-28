#####################################################################################
#
# Import general
#
#####################################################################################
from sqlalchemy import create_engine
import glob
import pandas as pd
from scipy.stats import pearsonr


#####################################################################################
#                                                                                   #
# Function fetch_data()                                                             #
# This function extracts the correlation coefficients saved in the data base.       #
#                                                                                   #
# Parameters:                                                                       #
#            None.                                                                  #
#                                                                                   #
# Returns: result_query - A dataframe with the result of the query.                 #
#                                                                                   #
#####################################################################################
def fetch_data():

    engine = create_engine('postgresql://postgres:sql@localhost:5432/project2')
    conn = engine.connect()

    query = '''
        select countrydata.*, lat, lon 
        from countrydata inner join coord_countries
        on "Country" = coord_countries.country
    '''
    result_qry = pd.read_sql(query, conn)
    
    return result_qry

#####################################################################################
#                                                                                   #
# Function analysis_func()                                                          #
# Esta función recibe un archivo base y un folder de archivos a utilizar para       #
# determinar si están correlacionados y obtener su coeficiente de correlación       #
#                                                                                   #
# Parameters:                                                                       #
# data_path - folder que contiene los archivos a comparar                           #
# origin - Archivo principal sobre el que se hará la comparación.                   #
# yy - año base con el que se buscará la correlación                                #
# cause - nombre del set de datos con el que se compara el archivo principal        #
#                                                                                   #
# Returns: df - Un dataframe con los datos del coeficiente.                         #
#                                                                                   #
#####################################################################################
def analysis_func(data_path, origin, yy, cause):

    le_years = range(yy-4, yy+1)
    le_cols = ["Countries"]
    cols_dict = {"country": "Countries"}

    for y in le_years:
        le_cols.append(str(y))
        cols_dict[str(y)] = "le_" + str(y)

    life_expectancy = pd.read_csv(
        origin, encoding="ISO-8859-1")[le_cols]

    #Change column names for clarity
    life_expectancy = life_expectancy.rename(
        columns=cols_dict)

    try:
        data = pd.read_csv(data_path)
    except:
        try:
            data = pd.read_csv(data_path, encoding="ISO-8859-1")
        except:
            print("Error: It was not possible to read the CSV file.")
            return


    #Inner join to ensure all the countries are the same in both dataframes
    joint_data = life_expectancy.merge(data, on="Countries", how="inner")

    # Create empty lists to keep track of values
    data_dict = []

    # Get all the years we will be using for our analysis
    years = joint_data.columns.tolist()


    # Get the coefficients and the p-values
    for row in joint_data.iterrows():
        coeffs = []
        p_values = []
        for i in range(1, len(years) - 10):
            row[1].iloc[1:] = pd.to_numeric(row[1].iloc[1:])
            coeff, p = pearsonr(row[1].iloc[1:6].to_list(),
                                row[1].iloc[6+i:11+i].to_list())
            coeffs.append(coeff)
            p_values.append(p)
        data_dict.append({
            "country": row[1].iloc[0],
            cause: max([abs(c) for c in coeffs])
        })

    df = pd.DataFrame(data_dict)

    return df

#.to_sql('countrydata', engine)
