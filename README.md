# intercity-symbol-permeation
A python tool for measuring intercity symbol permeation

ExtractPlaceSymbol.py :Extract the symbol representing the city from the POI name

Geocode_city.py :Call the Amap API to get the geographic location of the cities

SymbolDict.py :Create a symbol dictionary of cities

SymbolFlow.py :Construct symbol flows to represent intercity symbol permeation

allsymbols.py :Construct symbol flows to represent intercity symbol permeation,take local city to local city into account

CitiesAttributes.py :Calculate the symbol diversity and symbol dispersion for each city

AMap_adcode.csv,city_alias.csv,minority.csv,provincialcounties.csv,shortname_adcode.csv : Data used to create a symbol dictionary of cities

POI data source :https://doi.org/10.18170/DVN/WSXCNM
