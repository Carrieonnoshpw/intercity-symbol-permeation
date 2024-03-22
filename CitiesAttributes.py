from math import radians, cos, sin, asin, sqrt, atan2, degrees, log2
import pandas as pd
import os
def haversine_distance(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # Convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # Haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def calculate_bearing(lon1, lat1, lon2, lat2):
    """
    Calculate the bearing between two points.
    The formulae used is the following:
    θ = atan2(sin(Δlong).cos(lat2),
              cos(lat1).sin(lat2) - sin(lat1).cos(lat2).cos(Δlong))
    """
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1

    x = sin(dlon) * cos(lat2)
    y = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dlon)
    initial_bearing = atan2(x, y)

    # Now we have the initial bearing but atan2 return values from -π to +π (−180° to +180°)
    # so we need to normalize the result by converting it to a compass bearing
    initial_bearing = degrees(initial_bearing)
    compass_bearing = (initial_bearing + 360) % 360

    return compass_bearing

def rose_entropy(bearings, weights, interval=20):
    """
    Calculate the weighted rose entropy of a set of directions (bearings) with associated weights (distances).
    Parameters:
        bearings: List of bearings in degrees (0-360).
        weights: List of weights (e.g., distances) for each bearing.
        interval: Size of the interval in degrees for dividing the circle (default is 20 degrees).
    Returns:
        The weighted rose entropy value.
    """
    # Initialize a dictionary to count the weighted sum in each interval
    interval_counts = {i: 0 for i in range(0, 360, interval)}
    
    # Sum weights in their respective interval
    for bearing, weight in zip(bearings, weights):
        index = int(bearing // interval) * interval
        interval_counts[index] += weight
    
    # Calculate total weight for normalization
    total_weight = sum(interval_counts.values())
    
    # Calculate the weighted probability for each interval
    probabilities = [w / total_weight for w in interval_counts.values() if total_weight > 0]
    
    # Calculate the weighted rose entropy
    entropy = -sum(p * log2(p) for p in probabilities if p > 0)
    
    return entropy
def rose_gravity(bearings, weights):
    """
    Calculate the center of gravity for a set of directions (bearings) with associated weights (distances).
    Parameters:
        bearings: List of bearings in degrees (0-360).
        weights: List of weights (e.g., distances) for each bearing.
    Returns:
        The distance of the center of gravity from the origin.
    """
    x = y = total_weight = 0
    for bearing, weight in zip(bearings, weights):
        # Convert bearing to radians
        angle = radians(bearing)
        # Convert polar to Cartesian coordinates
        x += cos(angle) * weight
        y += sin(angle) * weight
        total_weight += weight

    # # Calculate the weighted center of gravity in Cartesian coordinates
    # if total_weight > 0:
    #     x /= total_weight
    #     y /= total_weight
    # else:
    #     return 0

    # Calculate the distance of the center of gravity from the origin
    return sqrt(x**2 + y**2)

def calculate_od_metrics(df):
    """
    Calculate the rose entropy and rose gravity for outgoing and incoming flows of each city in the dataframe.
    """
    cities_out = {}  # Dictionary to store outgoing flows' bearings and weights for each city
    cities_in = {}  # Dictionary to store incoming flows' bearings and weights for each city
    
    # Iterate through DataFrame rows
    for index, row in df.iterrows():
        o_city = row['Ocity_name']
        d_city = row['Dcity_name']
        weight = row['count'] * haversine_distance(row['O_X'], row['O_Y'], row['D_X'], row['D_Y'])  # Assuming 'count' represents the weight
        bearing_out = calculate_bearing(row['O_X'], row['O_Y'], row['D_X'], row['D_Y'])
        bearing_in = calculate_bearing(row['D_X'], row['D_Y'], row['O_X'], row['O_Y'])
        
        # Update outgoing data
        cities_out.setdefault(o_city, {'bearings': [], 'weights': []})
        cities_out[o_city]['bearings'].append(bearing_out)
        cities_out[o_city]['weights'].append(weight)
        
        # Update incoming data
        cities_in.setdefault(d_city, {'bearings': [], 'weights': []})
        cities_in[d_city]['bearings'].append(bearing_in)
        cities_in[d_city]['weights'].append(weight)
    
    # Initialize dictionaries to store calculated metrics
    metrics_out = {}
    metrics_in = {}
    
    # Calculate metrics for each city
    for city in set(cities_out.keys()) | set(cities_in.keys()):
        if city in cities_out:
            bearings, weights = cities_out[city]['bearings'], cities_out[city]['weights']
            entropy = rose_entropy(bearings, weights)
            gravity = rose_gravity(bearings, weights)
            metrics_out[city] = {'entropy': entropy, 'gravity': gravity}
        
        if city in cities_in:
            bearings, weights = cities_in[city]['bearings'], cities_in[city]['weights']
            entropy = rose_entropy(bearings, weights)
            gravity = rose_gravity(bearings, weights)
            metrics_in[city] = {'entropy': entropy, 'gravity': gravity}
    
    # Convert to DataFrame
    df_metrics = convert_to_dataframe_with_metrics(metrics_out, metrics_in)
    
    return df_metrics

def convert_to_dataframe_with_metrics(metrics_out, metrics_in):
    """
    Convert the metrics_out and metrics_in dictionaries to a pandas DataFrame.
    """
    data = []
    cities = set(metrics_out.keys()) | set(metrics_in.keys())
    for city in cities:
        out_metrics = metrics_out.get(city, {'entropy': None, 'gravity': None})
        in_metrics = metrics_in.get(city, {'entropy': None, 'gravity': None})
        data.append((city, out_metrics['entropy'], in_metrics['entropy'], out_metrics['gravity'], in_metrics['gravity']))
    
    df = pd.DataFrame(data, columns=['cityname', 'entropy_out', 'entropy_in', 'gravity_out', 'gravity_in'])
    
    return df

# Assuming 'df' is your DataFrame containing the OD flow data
# Note: This function assumes 'df' is a pandas DataFrame containing the OD flow data.
data_folder = 'data/output'
filename = 'Symbolicflows.csv'
df = pd.read_csv(os.path.join(data_folder, filename))
# Call the function to calculate metrics and convert to DataFrame
df_metrics = calculate_od_metrics(df)

# Save the DataFrame to a CSV file
df_metrics.to_csv(os.path.join(data_folder, 'city_od_metrics.csv'), index=False)


